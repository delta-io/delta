/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.amt

import java.io.File

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{Checkpoints, CommitStats, CurrentTransactionInfo, DeltaOperations, LastCheckpointInfo}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, ContentRoot, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

class AMTCheckpointWriteSuite extends AMTCheckpointTestBase {

  import testImplicits._

  /**
   * The nested `(name, type, field id)` shape of a persisted struct, recursing into sub-structs so
   * per-field Iceberg ids are covered. Comparing an on-disk struct's shape to the schema
   * `AMTPartitionValues`/`AMTContentStats` declare verifies the writer stamped exactly the declared
   * names, types and ids.
   */
  private def fieldIdShape(schema: StructType): Seq[(String, String, Long)] =
    schema.fields.toSeq.flatMap { field =>
      val id = field.metadata.getLong(ParquetUtils.FIELD_ID_METADATA_KEY)
      field.dataType match {
        case nested: StructType => (field.name, "struct", id) +: fieldIdShape(nested)
        case dataType => Seq((field.name, dataType.sql, id))
      }
    }

  /**
   * Asserts the live AddFiles survive a deferred checkpoint's construction unchanged: the snapshot
   * captured before the checkpoint is written must match the post-checkpoint snapshot. No-op for
   * inline scenarios, whose data commit also writes the checkpoint, so `preCheckpointSnapshot` is
   * still the pre-trigger state and has fewer files than the post-checkpoint snapshot.
   */
  private def assertLiveAddFilesRoundTrip(context: AMTCheckpointScenarioContext): Unit = {
    if (!context.scenario.isInline) {
      // The AddFiles constructed from pre-checkpoint snapshot might not match exactly the AddFiles
      // constructed from the post-checkpoint snapshot. This is because the post-checkpoint AddFiles
      // are reconstructed from the AMT thus going through DataEntry.toAddFile, and fields like
      // backReference and amtPassthrough are set differently between the pre- and post-checkpoint
      // AddFiles. Canonicalization is also needed because the stats are JSON strings, so it needs
      // to be compared as a parsed tree (order-insensitive).
      def canonical(add: AddFile) =
        add.copy(
          modificationTime = 0L,
          stats = null,
          backReference = None,
          amtPassthrough = None) -> Option(add.stats).map(JsonUtils.mapper.readTree)
      val original = context.preCheckpointSnapshot.allFiles.collect().map(canonical).toSet
      val reconstructed = context.postCheckpointSnapshot.allFiles.collect().map(canonical).toSet
      assert(reconstructed == original,
        s"live AddFiles changed across AMT checkpoint construction\n" +
          s"  before=$original\n  after=$reconstructed")
    }
  }

  test("interval boundary emits a follow-up OPTIMIZE CHECKPOINT commit carrying the Checkpoint") {
    withTable("amt_inline_emit") {
      val name = "amt_inline_emit"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1: not an interval boundary.
      sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary -> schedule maintenance.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      val snapshot = deltaLog.update()
      // The AMT is written by a follow-up OPTIMIZE CHECKPOINT commit at v3, not inline at v2.
      assert(snapshot.version == 3, "A follow-up OPTIMIZE CHECKPOINT commit lands at v3.")

      // Note the leaf may be promoted to root -- in that case no file with root naming pattern
      // will exist.
      assert(rootFiles(path).size + leafFiles(path).size > 0,
        "At least one root/leaf should be written")

      // The v2 commit carries only the user AddFile, no Checkpoint.
      val v2Actions = actionsAt(deltaLog, 2)
      assert(v2Actions.exists(_.isInstanceOf[AddFile]), "v2 carries the user AddFile.")
      assert(v2Actions.collect { case c: Checkpoint => c }.isEmpty,
        s"v2 must not carry a Checkpoint action; got: $v2Actions")

      // The v3 follow-up commit carries a single Checkpoint action and no user AddFile.
      val v3Actions = actionsAt(deltaLog, 3)
      val checkpoints = v3Actions.collect { case c: Checkpoint => c }
      assert(checkpoints.size == 1, s"Expected one Checkpoint action at v3, got: $v3Actions")
      assert(!v3Actions.exists(_.isInstanceOf[AddFile]), "v3 carries no user AddFile.")
      // The Checkpoint describes state as of v2 (the version whose maintenance it fulfills).
      assert(checkpoints.head.version == 2,
        s"Checkpoint must describe state as of v2; got ${checkpoints.head.version}")
      assert(checkpoints.head.contentRoot.version == 2,
        s"contentRoot.version must be v2; got ${checkpoints.head.contentRoot.version}")

      // The Checkpoint's contentRoot points at an on-disk manifest file. A single promoted leaf
      // keeps its leaf-* name, so accept either a root or leaf manifest.
      val rootName = new File(checkpoints.head.contentRoot.path).getName
      assert(isRootFileName(rootName) || isLeafFileName(rootName),
        "contentRoot must follow the given naming pattern")
      assert((rootFiles(path) ++ leafFiles(path)).exists(_.getName == rootName),
        s"contentRoot must reference an on-disk manifest; got $rootName")
    }
  }

  testAcrossAMTCheckpointScenarios(
      "small full checkpoint promotes its single distributed manifest to the root",
      "amt_root_only",
      deferredScenarios = Seq(AMTCheckpointScenario.DeferredFull))(
      setup = name => {
        sql(s"INSERT INTO $name VALUES (1)")
        sql(s"INSERT INTO $name VALUES (2)")
      }) { context =>
    val rootDataEntries = withManifestDataEntries(
      Seq(context.checkpoint.contentRoot.getAbsolutePath(context.provider.tableRoot).toString)
    )(_.count())

    assert(context.provider.leaves.isEmpty, "The root must contain no leaf pointers.")
    assert(rootDataEntries == 2L, "Both live files must be reachable as DATA entries in the root.")

    // The promoted root alone must reconstruct exactly the table's live files.
    assertReconstructsLiveFileSet(context)
  }

  testAcrossAMTCheckpointScenarios(
      "partition values use the typed Iceberg struct and round-trip",
      "amt_typed_partition",
      sqlConfs = leafPackingConfs,
      tableSchema =
        ("id INT" +: partitionableTestColumns.map(_.columnDef("p_"))).mkString(", "),
      partitionColumns = partitionableTestColumns.map("p_" + _.name))(
      setup = name => appendRowsAsSeparateFiles(
        name,
        numFiles = leafPackedFiles - 1,
        columnExprs = "CAST(id AS INT)" +: partitionableTestColumns.map(_.valueExpr)),
      inlineCheckpointTriggerActionsOrSQL = Some { name =>
        val cols = ("CAST(id AS INT)" +: partitionableTestColumns.map(_.valueExpr)).mkString(", ")
        Right(
          s"""INSERT INTO $name
             |SELECT $cols
             |FROM range(${leafPackedFiles - 1}, $leafPackedFiles)""".stripMargin)
      }) { context =>
    allowReadWithinDeltaLog {
      val leafDf = spark.read.parquet(
        context.provider.liveLeafManifestAbsolutePaths.map(_.toString): _*)
      val partitionSchema = context.postCheckpointSnapshot.metadata.partitionSchema
      val partition = leafDf.schema("partition")
      assert(partition.nullable)
      val expectedSchema = AMTPartitionValues.persistedSchema(partitionSchema)
      assert(
        fieldIdShape(partition.dataType.asInstanceOf[StructType]) == fieldIdShape(expectedSchema),
        s"persisted partition schema did not match the expected typed shape\n" +
          s"  actual=${partition.dataType}\n  expected=$expectedSchema")
      assert(leafDf.select("partition.p_int").collect().map(_.getInt(0)).toSet ==
        (0 until leafPackedFiles).toSet)
    }

    // Every physical-name -> string partition entry must come back exactly as the log recorded it;
    // a cast that disagrees with Delta's own serialization would corrupt these silently.
    assertLiveAddFilesRoundTrip(context)
  }

  testAcrossAMTCheckpointScenarios(
      "an unpartitioned table writes no partition column at all",
      "amt_unpartitioned",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    // An unpartitioned table has no partition struct to persist, so `partition` is dropped from the
    // schema rather than written as a null -- Iceberg field 102 is optional. Checking the parquet
    // itself, since the reconstructed AddFile carries a `partition` either way (`forRead` adds one
    // back as a null map).
    val manifests =
      (context.provider.topLevelFiles.map(_.getPath.toString) ++
        context.provider.liveLeafManifestAbsolutePaths.map(_.toString))
    assert(manifests.nonEmpty, "Expected at least a root manifest.")
    manifests.foreach { manifest =>
      val columns = allowReadWithinDeltaLog {
        spark.read.parquet(manifest).columns.toSeq
      }
      assert(!columns.contains("partition"),
        s"an unpartitioned manifest must have no `partition` column; $manifest has $columns")
    }
  }

  testAcrossAMTCheckpointScenarios(
      "typed content stats round-trip through reconstruction",
      "amt_typed_content_stats",
      tableSchema = allTypeColumnDefinitions.mkString(", "))(
      setup = name =>
        appendRowsAsSeparateFiles(name, numFiles = 2, columnExprs = allTypeColumnExprs),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name SELECT ${allTypeColumnExprs.mkString(", ")} FROM range(2, 3)"))) {
    context =>
    val snapshot = context.postCheckpointSnapshot

    val manifest = context.provider.topLevelFiles.map(_.getPath.toString).head
    val contentStats = allowReadWithinDeltaLog {
      spark.read.parquet(manifest).schema("content_stats")
    }
    assert(contentStats.nullable)
    val expectedSchema = AMTContentStats.persistedSchema(snapshot.metadata, snapshot.protocol)
    assert(
      fieldIdShape(contentStats.dataType.asInstanceOf[StructType]) == fieldIdShape(expectedSchema),
      s"persisted content_stats schema did not match the expected typed shape\n" +
        s"  actual=${contentStats.dataType}\n  expected=$expectedSchema")

    // The typed content stats must round-trip: the AddFiles reconstructed from the checkpoint must
    // carry the same stats Delta originally wrote.
    assertLiveAddFilesRoundTrip(context)
  }

  testAcrossAMTCheckpointScenarios(
      "manifest pointers are stored relative to the table root",
      "amt_relative_pointers",
      // The leaf packing keeps several leaf pointers in the root to check, rather than the single
      // manifest a small table would promote.
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val name = context.tableName
    val provider = context.provider

    // The Checkpoint's contentRoot pointer is stored relative to the table root, i.e.
    // `metadata/root-<uuid>.parquet`, not an absolute URI.
    val rootPointer = provider.checkpointAction.contentRoot.path
    assert(rootPointer == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(rootPointer).getName}",
      s"contentRoot.path must be table-root-relative; got $rootPointer")
    assert(!new Path(rootPointer).isAbsolute,
      s"contentRoot.path must not be absolute; got $rootPointer")
    assert(isRootFileName(new File(rootPointer).getName))

    // Every leaf pointer stored in the root manifest is likewise table-root-relative.
    val leafLocations = allowReadWithinDeltaLog {
      spark.read.parquet(new File(new File(tablePath(name),
        FileNames.AMT_METADATA_DIR_NAME), new File(rootPointer).getName).toString)
        .where(col("content_type") === AMTSingleAction.ContentType.Type.DataManifest)
        .select("location").as[String].collect().toSeq
    }
    assert(leafLocations.size == expectedLeafCount(leafPackedFiles),
      s"$leafPackedFiles files at $entriesPerLeaf per leaf must yield "  +
        s"${expectedLeafCount(leafPackedFiles)} leaf pointers; got $leafLocations")
    leafLocations.foreach { loc =>
      assert(loc == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(loc).getName}",
        s"leaf pointer must be table-root-relative; got $loc")
      assert(isLeafFileName(new File(loc).getName))
    }

    // The pointers are stored relative on disk; the provider re-absolutizes them via
    // `liveLeafManifestAbsolutePaths`, and reconstruction driven off the manifest tree (root +
    // leaves, both stored relative) surfaces exactly the committed data files.
    val leafLocs = provider.leaves.map(_.location)
    assert(leafLocs.forall(loc =>
      loc == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(loc).getName}"),
      s"leaf pointer locations must be table-root-relative; got $leafLocs")
    assert(provider.liveLeafManifestAbsolutePaths.forall(_.isAbsolute),
      "resolved leaf manifest paths must be absolute; got " +
        s"${provider.liveLeafManifestAbsolutePaths}")
    assertReconstructsLiveFileSet(context)
    // `setup` writes one file per id, and the trigger adds the last one.
    checkAnswer(spark.table(name), (0 until leafPackedFiles).map(Row(_)))
  }

  test("manifest tree round-trips when the table root contains spaces") {
    withTempDir { baseDir =>
      // A table location with a space exercises raw (non-URL-encoded) manifest path handling: the
      // stored relative pointers (`metadata/leaf-<uuid>.parquet`) must resolve back by literal join
      // onto the spaced root, not URI parsing (which would turn the space into %20 or fail).
      val tableRoot = new File(baseDir, "amt table with spaces")
      withTable("amt_spaced_root") {
        val name = "amt_spaced_root"
        createAMTTable(name, checkpointInterval = 2, location = Some(tableRoot.toString))
        withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
          sql(s"INSERT INTO $name VALUES (1)") // v1: one data file.
          sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary -> triggers AMT emission.
        }

        val deltaLog = deltaLogForName(name)
        val snapshot = deltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))

        // The table root really does contain a space, and it is not percent-encoded on disk.
        assert(deltaLog.dataPath.toString.contains("amt table with spaces"),
          s"table root must contain a space; got ${deltaLog.dataPath}")
        assert(!deltaLog.dataPath.toString.contains("%20"),
          s"table root must not be URL-encoded; got ${deltaLog.dataPath}")

        // The root pointer is stored table-root-relative (raw).
        val rootPointer = provider.checkpointAction.contentRoot.path
        assert(
          rootPointer == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(rootPointer).getName}",
          s"contentRoot.path must be table-root-relative; got $rootPointer")
        // The provider re-absolutizes leaf pointers to paths that live under the spaced root and
        // are not percent-encoded.
        provider.liveLeafManifestAbsolutePaths.foreach { leafPath =>
          assert(leafPath.isAbsolute && leafPath.toString.contains("amt table with spaces"),
            s"resolved leaf path must live under the spaced table root; got $leafPath")
          assert(!leafPath.toString.contains("%20"),
            s"resolved leaf path must stay raw, not URL-encoded; got $leafPath")
        }

        // The tree reconstructs end-to-end through the spaced paths.
        val reconstructed = provider.loadActionsForStateReconstruction(spark, deltaLog)
          .getOrElse(fail("AMT provider must contribute reconstructed actions."))
        assert(reconstructed.where("add is not null").count() == 2,
          "Reconstruction from the spaced-path manifest tree must surface both committed files.")
        checkAnswer(spark.table(name), Seq(Row(1), Row(2)))
      }
    }
  }

  test("manifest tree round-trips when the manifest file names contain spaces") {
    withTable("amt_spaced_manifest") {
      val name = "amt_spaced_manifest"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2: triggers a real AMT emission we then override.

      val deltaLog = deltaLogForName(name)
      val base = amtProvider(deltaLog.update())
        .getOrElse(fail("expected AMTCheckpointProvider")).checkpointAction
      val dataPath = deltaLog.dataPath
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      val metadataDir = FileNames.amtMetadataDirPath(dataPath)
      val enc = org.apache.spark.sql.delta.implicits.amtSingleActionEncoder

      // The production writer names manifests `leaf-<uuid>`/`root-<uuid>`, so a normal write never
      // yields a spaced manifest file name. Synthesize a tree whose leaf and root file names
      // contain spaces, storing raw table-root-relative pointers, to prove the pointer round-trips.
      def writeManifest(fileName: String, rows: Seq[AMTSingleAction]): (String, Long) = {
        val file = new Path(metadataDir, fileName)
        val metadata = base.metaData
        val protocol = base.protocol
        val withPartition = AMTPartitionValues.forWrite(
          spark.createDataset(rows)(enc).toDF(), metadata.partitionSchema)
        val df = AMTContentStats.forWrite(withPartition, metadata, protocol)
        Checkpoints.writeAtomicCheckpointParquetFile(
          spark,
          df,
          file,
          hadoopConf,
          useRename = false,
          outputSchema = Some(AMTSingleAction.persistedSchema(metadata, protocol)),
          writeAsIcebergManifest = true)
        val relative = AMTUtils.relativizeManifestPathToTableRoot(
          file.getFileSystem(hadoopConf), dataPath, file)
        assert(relative == s"${FileNames.AMT_METADATA_DIR_NAME}/$fileName" &&
          !relative.contains("%20"),
          s"stored pointer must be raw and table-root-relative; got $relative")
        (relative, file.getFileSystem(hadoopConf).getFileStatus(file).getLen)
      }

      val dataAdd = AddFile(path = "part-0.parquet", partitionValues = Map.empty, size = 128L,
        modificationTime = 0L, dataChange = false, stats = s"""{"numRecords":3}""")
      val (leafLoc, leafSize) = writeManifest(
        "leaf with space.parquet",
        Seq(AMTSingleAction.fromAddFile(dataAdd, addedTracking, dataPath)))
      val leafPointer = DataManifestEntry(
        location = leafLoc,
        file_format = AMTSingleAction.FileFormatParquet,
        tracking = addedTracking,
        record_count = 1L,
        file_size_in_bytes = leafSize,
        manifest_info = emptyManifestInfo.copy(added_files_count = 1))
      val (rootLoc, rootSize) = writeManifest("root with space.parquet", Seq(leafPointer.wrap))
      val checkpoint = base.copy(
        contentRoot = ContentRoot(path = rootLoc, sizeInBytes = rootSize, version = base.version))

      // The synthesized pointers really do carry spaces and are not URL-encoded.
      assert(rootLoc.contains("root with space.parquet") && !rootLoc.contains("%20"))
      assert(leafLoc.contains("leaf with space.parquet") && !leafLoc.contains("%20"))

      val provider = AMTCheckpointProvider.fromCheckpoint(
        deltaLog, checkpoint, manifestCommitVersion = checkpoint.version)
      // The provider resolves the spaced pointers to absolute, raw paths under the table root.
      assert(provider.liveLeafManifestAbsolutePaths.forall(p =>
        p.isAbsolute && p.toString.contains("leaf with space.parquet") &&
          !p.toString.contains("%20")),
        s"resolved leaf paths must stay raw; got ${provider.liveLeafManifestAbsolutePaths}")

      // Reconstruction reads the DATA entry back through the spaced leaf path.
      val reconstructed = provider.loadActionsForStateReconstruction(spark, deltaLog)
        .getOrElse(fail("AMT provider must contribute reconstructed actions."))
      val addPaths = reconstructed.where("add is not null").select("add.path").as[String].collect()
      assert(addPaths.toSeq == Seq("part-0.parquet"),
        s"reconstruction from the spaced-manifest tree must surface the DATA entry; got $addPaths")
    }
  }

  test("no emission on a vanilla (non-AMT) table") {
    withTable("amt_vanilla") {
      val name = "amt_vanilla"
      sql(
        s"""CREATE TABLE $name (id INT) USING DELTA
           |TBLPROPERTIES ('delta.checkpointInterval' = '2')""".stripMargin)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)") // interval boundary, but no AMT feature.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      assert(rootFiles(path).isEmpty && leafFiles(path).isEmpty,
        "No AMT artifacts on a vanilla table.")
      assert(checkpointAt(deltaLog, 2).isEmpty, "No Checkpoint action on a vanilla table.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("no emission on a non-interval commit") {
    withTable("amt_non_interval") {
      val name = "amt_non_interval"
      createAMTTable(name, checkpointInterval = 10) // interval far from the versions we write.
      sql(s"INSERT INTO $name VALUES (1)") // v1: 1 % 10 != 0.

      val deltaLog = deltaLogForName(name)
      assert(checkpointAt(deltaLog, 1).isEmpty, "v1 is not an interval boundary; no emission.")
      assert(rootFiles(tablePath(name)).isEmpty,
        "No manifest tree written off an interval boundary.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  testAcrossAMTCheckpointScenarios(
      "leaf cardinality respects AMT_ENTRIES_PER_LEAF",
      "amt_entries_per_leaf",
      sqlConfs = Seq(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "7"))(
      setup = name => appendRowsAsSeparateFiles(name, 20),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (20)"))) { context =>
    assert(context.postCheckpointSnapshot.allFiles.count() == 21,
      s"Expected 21 live files, got ${context.postCheckpointSnapshot.allFiles.count()}.")
    assert(context.provider.leaves.size == 3,
      s"21 files at entriesPerLeaf=7 must pack into 3 leaves; got ${context.provider.leaves.size}.")
  }

  testAcrossAMTCheckpointScenarios(
      "full rewrite records each distributed leaf's entry count",
      "amt_leaf_counts",
      deferredScenarios = Seq(AMTCheckpointScenario.DeferredFull),
      sqlConfs = leafPackingConfs)(
      setup = name => {
        // Half the files carry 2 rows and half carry 4, so a leaf's existing_rows_count reflects
        // the summed physical rows of its files rather than just its entry count.
        val filesPerRowGroup = leafPackedFiles / 2
        appendRowsAsSeparateFiles(name, numFiles = filesPerRowGroup, rowsPerFile = 2)
        appendRowsAsSeparateFiles(name, numFiles = filesPerRowGroup, rowsPerFile = 4,
          startId = filesPerRowGroup * 2)
      }) { context =>
    val leaves = context.provider.leaves
    assertLeafCount(leaves)
    val totalLiveFiles = context.postCheckpointSnapshot.allFiles.count()
    val filesPerRowGroup = leafPackedFiles / 2
    val expectedTotalRows = filesPerRowGroup * 2 + filesPerRowGroup * 4
    leaves.foreach { leaf =>
      val mi = leaf.manifest_info
      // The distributed writer flushes files that already lived in the table, so they are EXISTING
      // (not ADDED).
      assert(leaf.record_count > 0L,
        s"A full-rewrite leaf must report a non-zero record_count; got $leaf.")
      assert(mi.existing_files_count.toLong == leaf.record_count,
        s"existing_files_count must equal record_count; got $mi vs ${leaf.record_count}.")
      // A leaf holds a data-dependent mix of 2- and 4-row files (every file has at least 2 rows),
      // so per-leaf existing_rows_count is at least twice its entry count; the exact total is
      // asserted in aggregate below.
      assert(mi.existing_rows_count >= 2 * leaf.record_count,
        s"existing_rows_count must be at least 2x record_count; got $mi vs ${leaf.record_count}.")
      assert(mi.added_rows_count == 0L && mi.deleted_rows_count == 0L &&
        mi.replaced_rows_count == 0L && mi.modified_rows_count == 0L,
        s"A fresh full-rewrite leaf counts only existing rows; got $mi.")
      assert(mi.added_files_count == 0 && mi.deleted_files_count == 0 &&
        mi.replaced_files_count == 0 && mi.modified_files_count == 0,
        s"A fresh full-rewrite leaf counts only existing files; got $mi.")
    }
    // Conservation: the per-leaf counts account for every live file and row (a multi-leaf tree
    // keeps no root-resident data entries).
    assert(leaves.map(_.record_count).sum == totalLiveFiles,
      s"Leaf record_counts must sum to $totalLiveFiles; got ${leaves.map(_.record_count)}.")
    assert(leaves.map(_.manifest_info.existing_rows_count).sum == expectedTotalRows,
      s"Leaf existing_rows_counts must sum to $expectedTotalRows; got " +
        s"${leaves.map(_.manifest_info.existing_rows_count)}.")
    assertReconstructsLiveFileSet(context)
  }

  /** Parses the `delta.commit.stats` [[CommitStats]] logged for `version`, or fails. */
  private def commitStatsAt(f: => Unit, version: Long): CommitStats = {
    Log4jUsageLogger.track(f)
      .filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.commit.stats"))
      .map(e => JsonUtils.fromJson[CommitStats](e.blob))
      .find(_.commitVersion == version)
      .getOrElse(fail(s"No commit stats logged for version $version."))
  }

  test("the follow-up OPTIMIZE CHECKPOINT commit stats carry AMT write metrics") {
    withTable("amt_commit_stats") {
      val name = "amt_commit_stats"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1: below the interval, no maintenance.

      // v2 hits the interval boundary; the AMT is written by the follow-up commit at v3, so the
      // AMT write metrics are recorded on v3's stats, not v2's.
      val allStats = Log4jUsageLogger.track {
        sql(s"INSERT INTO $name VALUES (2)")
      }.filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
          e.tags.get("opType").contains("delta.commit.stats"))
        .map(e => JsonUtils.fromJson[CommitStats](e.blob))

      val v2Stats = allStats.find(_.commitVersion == 2).getOrElse(fail("No stats for v2."))
      assert(v2Stats.amtWriteMetrics.isEmpty, "v2 defers the AMT; its stats carry no AMT metrics.")

      val v3Stats = allStats.find(_.commitVersion == 3).getOrElse(fail("No stats for v3."))
      val metrics = v3Stats.amtWriteMetrics
        .getOrElse(fail("The follow-up commit's stats should carry AMT write metrics."))
      assert(metrics.attempts.size == 1, s"Expected one AMT write attempt, got ${metrics.attempts}")
      // The first AMT has no prior tree to build on, so it is always a full rewrite.
      assert(metrics.attempts.head.trigger == AMTTriggerMode.CheckpointIntervalFull.name)
      assert(metrics.attempts.head.materializeDurationMs >= 0L)
    }
  }

  test("commit stats carry no AMT write metrics when no AMT is emitted") {
    withTable("amt_no_commit_stats") {
      val name = "amt_no_commit_stats"
      createAMTTable(name, checkpointInterval = 100) // interval far away, so v1 emits no AMT.

      val commitStats = commitStatsAt(sql(s"INSERT INTO $name VALUES (1)"), version = 1)

      assert(commitStats.amtWriteMetrics.isEmpty,
        "Commit stats must not carry AMT write metrics when no AMT is emitted.")
    }
  }

  testAcrossAMTCheckpointScenarios(
      "an emitted AMT populates _last_checkpoint",
      "amt_last_checkpoint")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val info = deltaLog.readLastCheckpointFile().getOrElse {
      fail("An AMT emission must write _last_checkpoint.")
    }

    // `version` names the tree's content-root version, and `parts` the leaf count.
    assert(info.version == context.checkpoint.contentRoot.version,
      s"Expected content root v${context.checkpoint.contentRoot.version}, got v${info.version}.")
    assert(info.parts.contains(context.provider.leaves.size),
      s"Expected ${context.provider.leaves.size} leaves, got ${info.parts}.")
    assert(info.sizeInBytes.exists(_ > 0L), "The AMT size in bytes must be recorded.")
    // `size` (action count) and `numOfAddFiles` are intentionally unavailable for AMT.
    assert(info.size == -1, s"AMT size must be -1, got ${info.size}.")
    assert(info.numOfAddFiles.isEmpty,
      s"AMT numOfAddFiles must be empty, got ${info.numOfAddFiles}.")

    assert(info.checkpointType == Some(LastCheckpointInfo.CheckpointType.AMT))
    val amt = info.amtCheckpoint.getOrElse {
      fail("_last_checkpoint must carry amtCheckpoint.")
    }
    assert(amt.checkpoint == Some(context.checkpoint))
    assertLeavesEqual(amt.leaves.get, context.provider.leaves)
    assert(amt.manifestCommitVersion == context.manifestCommitVersion,
      s"Expected manifest at v${context.manifestCommitVersion}, got v${amt.manifestCommitVersion}.")
  }

  test("_last_checkpoint is not updated when no AMT is emitted") {
    withTable("amt_last_checkpoint_unchanged") {
      val name = "amt_last_checkpoint_unchanged"
      // Interval 3. Emission triggers only when the version distance from the last AMT is an exact
      // positive multiple of the interval, so the timeline is:
      //   v1: below the first boundary  -> no emission
      //   v2: below the first boundary  -> no emission
      //   v3: first boundary            -> deferred AMT recorded at v4, describing v3
      //   v4: OPTIMIZE CHECKPOINT      -> emits an AMT
      //   v5: one past the last AMT    -> no emission (the next boundary is v6)
      createAMTTable(name, checkpointInterval = 3)
      val deltaLog = deltaLogForName(name)

      // -- Before any emission: no pointer at all. --
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)")
      assert(deltaLog.update().version == 2, "Neither v1 nor v2 should schedule a follow-up AMT.")
      assert(checkpointAt(deltaLog, 1).isEmpty, "v1 emits no AMT.")
      assert(checkpointAt(deltaLog, 2).isEmpty, "v2 emits no AMT.")
      assert(deltaLog.readLastCheckpointFile().isEmpty,
        "Commits that emit no AMT must not write _last_checkpoint.")

      // -- The boundary commit emits, and the follow-up commit writes the pointer. --
      sql(s"INSERT INTO $name VALUES (3)")
      assert(deltaLog.update().version == 4, "The v3 boundary defers its AMT to v4.")
      val afterEmission = deltaLog.readLastCheckpointFile().getOrElse {
        fail("The deferred AMT must write _last_checkpoint.")
      }
      assert(afterEmission.version == 3, "The tree describes state as of the v3 boundary.")
      assert(afterEmission.amtCheckpoint.map(_.manifestCommitVersion) == Some(4L),
        "The manifest commit version should be v4.")

      // -- A later commit that emits nothing must leave the existing pointer untouched. --
      sql(s"INSERT INTO $name VALUES (4)")
      assert(deltaLog.update().version == 5, "v5 should not schedule a follow-up AMT.")
      assert(checkpointAt(deltaLog, 5).isEmpty, "v5 emits no AMT.")
      val afterNonAMTCommit = deltaLog.readLastCheckpointFile().getOrElse {
        fail("_last_checkpoint must survive commits that emit no AMT.")
      }
      assert(afterNonAMTCommit == afterEmission,
        "A non-AMT commit must not modify _last_checkpoint.")
    }
  }

  /** An ADDED tracking envelope with no lineage/sequence numbers, matching the AMT writer. */
  private def addedTracking: Tracking = Tracking(
    status = Tracking.Status.Added,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /** A zeroed [[ManifestInfo]]; tests set only the counts they assert on via `copy`. */
  private def emptyManifestInfo: ManifestInfo = ManifestInfo(
    added_files_count = 0,
    existing_files_count = 0,
    deleted_files_count = 0,
    replaced_files_count = 0,
    modified_files_count = 0,
    added_rows_count = 0L,
    existing_rows_count = 0L,
    deleted_rows_count = 0L,
    replaced_rows_count = 0L,
    modified_rows_count = 0L,
    min_sequence_number = 0L,
    dv = None,
    dv_cardinality = None)
}
