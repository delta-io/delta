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

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{Checkpoints, CommitStats, CurrentTransactionInfo, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, ContentRoot, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

class AMTCheckpointWriteSuite extends AMTCheckpointTestBase {

  import testImplicits._

  testAcrossAMTCheckpointScenarios(
      "checkpoint emission writes a manifest tree and carries the user action once",
      "amt_emit",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles),
      inlineCheckpointTriggerActionsOrSQL = Some(_ => Left((
        Seq(AddFile("missing-file.parquet", Map.empty, 0L, 0L, dataChange = true)
          .removeWithTimestamp(1L)),
        DeltaOperations.ManualUpdate)))) { context =>
    val actionsFromCheckpointedCommit =
      actionsAt(context.postCheckpointSnapshot.deltaLog, context.checkpoint.version)
    assert(actionsFromCheckpointedCommit.exists {
      case _: AddFile | _: RemoveFile => true
      case _ => false
    }, "The described business commit must carry the user file action.")

    val path = tablePath(context.tableName)
    val rootName = new File(context.checkpoint.contentRoot.path).getName
    assert(isRootFileName(rootName),
      s"a written tree's root must carry a root manifest name; got $rootName")
    assert(rootFiles(path).exists(_.getName == rootName))
    assertLeafCount(context.provider.leaves)

    // The emitted tree must reconstruct exactly the table's live files.
    assertReconstructsLiveFileSet(context)
  }

  testAcrossAMTCheckpointScenarios(
      "small full checkpoint promotes its single distributed manifest to the root",
      "amt_root_only",
      deferredScenarios = Seq(AMTCheckpointScenario.DeferredFull))(
      setup = name => {
        sql(s"INSERT INTO $name VALUES (1)")
        sql(s"INSERT INTO $name VALUES (2)")
      }) { context =>
    val rootDataEntries = {
      spark.read
        .parquet(context.checkpoint.contentRoot.getAbsolutePath(context.provider.tableRoot)
          .toString)
        .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
        .count()
    }

    assert(context.provider.leaves.isEmpty, "The root must contain no leaf pointers.")
    assert(rootDataEntries == 2L, "Both live files must be reachable as DATA entries in the root.")

    // The promoted root alone must reconstruct exactly the table's live files.
    assertReconstructsLiveFileSet(context)
  }

  testAcrossAMTCheckpointScenarios(
      "manifest pointers are stored relative to the table root",
      "amt_relative_pointers",
      // The leaf packing keeps several leaf pointers in the root to check, rather than the single
      // manifest a small table would promote.
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
    val leafLocations = {
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
    // `leafManifestAbsolutePaths`, and reconstruction driven off the manifest tree (root +
    // leaves, both stored relative) surfaces exactly the committed data files.
    val leafLocs = provider.leaves.map(_.location)
    assert(leafLocs.forall(loc =>
      loc == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(loc).getName}"),
      s"leaf pointer locations must be table-root-relative; got $leafLocs")
    assert(provider.leafManifestAbsolutePaths.forall(_.isAbsolute),
      s"resolved leaf manifest paths must be absolute; got ${provider.leafManifestAbsolutePaths}")
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
        provider.leafManifestAbsolutePaths.foreach { leafPath =>
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
        val df = spark.createDataset(rows)(enc).toDF()
        Checkpoints.writeAtomicCheckpointParquetFile(spark, df, file, hadoopConf, useRename = false)
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
      val checkpoint = base.copy(contentRoot = ContentRoot(path = rootLoc, sizeInBytes = rootSize))

      // The synthesized pointers really do carry spaces and are not URL-encoded.
      assert(rootLoc.contains("root with space.parquet") && !rootLoc.contains("%20"))
      assert(leafLoc.contains("leaf with space.parquet") && !leafLoc.contains("%20"))

      val provider = AMTCheckpointProvider.fromCheckpoint(spark, deltaLog, checkpoint)
      // The provider resolves the spaced pointers to absolute, raw paths under the table root.
      assert(provider.leafManifestAbsolutePaths.forall(p =>
        p.isAbsolute && p.toString.contains("leaf with space.parquet") &&
          !p.toString.contains("%20")),
        s"resolved leaf paths must stay raw; got ${provider.leafManifestAbsolutePaths}")

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
    assert(context.provider.leaves.forall(_.record_count <= 7),
      s"Every leaf must respect entriesPerLeaf=7: ${context.provider.leaves}")
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
    added_rows_count = 0L,
    existing_rows_count = 0L,
    deleted_rows_count = 0L,
    replaced_rows_count = 0L,
    min_sequence_number = 0L,
    dv = None,
    dv_cardinality = None)
}
