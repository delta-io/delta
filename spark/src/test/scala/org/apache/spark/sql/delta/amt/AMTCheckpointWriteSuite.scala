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
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, ContentRoot}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

class AMTCheckpointWriteSuite extends AMTCheckpointTestBase {

  import testImplicits._


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

      // Manifest tree exists on disk: exactly one root, at least one leaf.
      assert(rootFiles(path).size == 1, "Exactly one root manifest must be written.")
      assert(leafFiles(path).nonEmpty, "At least one leaf manifest must be written.")

      // The v2 business commit carries only the user AddFile, no Checkpoint.
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

      // The Checkpoint's contentRoot points at the on-disk root file.
      val rootName = new File(checkpoints.head.contentRoot.path).getName
      assert(isRootFileName(rootName),
        s"contentRoot must point at a root manifest file; got ${checkpoints.head.contentRoot.path}")
      assert(rootFiles(path).exists(_.getName == rootName))
    }
  }

  test("manifest pointers are stored relative to the table root") {
    withTable("amt_relative_pointers") {
      val name = "amt_relative_pointers"
      createAMTTable(name, checkpointInterval = 2)
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
        sql(s"INSERT INTO $name VALUES (1)") // v1: one data file.
        sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary -> triggers AMT emission.
      }

      val deltaLog = deltaLogForName(name)
      val snapshot = deltaLog.update()
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))

      // The Checkpoint's contentRoot pointer is stored relative to the table root, i.e.
      // `metadata/root-<uuid>.parquet`, not an absolute URI.
      val rootPointer = provider.checkpointAction.contentRoot.path
      assert(rootPointer == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(rootPointer).getName}",
        s"contentRoot.path must be table-root-relative; got $rootPointer")
      assert(!new Path(rootPointer).isAbsolute,
        s"contentRoot.path must not be absolute; got $rootPointer")
      assert(isRootFileName(new File(rootPointer).getName))

      // Every leaf pointer stored in the root manifest is likewise table-root-relative.
      val rootDf = spark.read.parquet(new File(new File(tablePath(name),
        FileNames.AMT_METADATA_DIR_NAME), new File(rootPointer).getName).toString)
      val leafLocations = rootDf
        .where(col("content_type") === AMTSingleAction.ContentType.Type.DataManifest)
        .select("location").as[String].collect().toSeq
      // The clustered writer decides leaf count by partitioning, so assert at least one leaf
      // pointer rather than an exact count; every pointer must be table-root-relative.
      assert(leafLocations.nonEmpty, s"Expected at least one leaf pointer, got $leafLocations")
      leafLocations.foreach { loc =>
        assert(loc == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(loc).getName}",
          s"leaf pointer must be table-root-relative; got $loc")
        assert(isLeafFileName(new File(loc).getName))
      }

      // The pointers are stored relative on disk; the provider re-absolutizes them via
      // `leafManifestAbsolutePaths`, and reconstruction driven off the manifest tree (root +
      // leaves, both stored relative) surfaces exactly the two committed data files.
      val leafLocs = provider.leaves.map(_.location)
      assert(leafLocs.forall(loc =>
        loc == s"${FileNames.AMT_METADATA_DIR_NAME}/${new File(loc).getName}"),
        s"leaf pointer locations must be table-root-relative; got $leafLocs")
      assert(provider.leafManifestAbsolutePaths.forall(_.isAbsolute),
        s"resolved leaf manifest paths must be absolute; got ${provider.leafManifestAbsolutePaths}")
      val reconstructed = provider.loadActionsForStateReconstruction(spark, deltaLog)
        .getOrElse(fail("AMT provider must contribute reconstructed actions."))
      assert(reconstructed.where("add is not null").count() == 2,
        "Reconstruction from the relative-pointer manifest tree must surface both committed files.")
      checkAnswer(spark.table(name), Seq(Row(1), Row(2)))
    }
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
      assert(checkpointsAt(deltaLog, 2).isEmpty, "No Checkpoint action on a vanilla table.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("no emission on a non-interval commit") {
    withTable("amt_non_interval") {
      val name = "amt_non_interval"
      createAMTTable(name, checkpointInterval = 10) // interval far from the versions we write.
      sql(s"INSERT INTO $name VALUES (1)") // v1: 1 % 10 != 0.

      val deltaLog = deltaLogForName(name)
      assert(checkpointsAt(deltaLog, 1).isEmpty, "v1 is not an interval boundary; no emission.")
      assert(rootFiles(tablePath(name)).isEmpty,
        "No manifest tree written off an interval boundary.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("leaf cardinality respects AMT_ENTRIES_PER_LEAF") {
    withTable("amt_entries_per_leaf") {
      val name = "amt_entries_per_leaf"
      // Interval far away so no automatic checkpoint fires; we drive the rewrite directly. With no
      // prior AMT tree, the rewrite clusters the live files across ceil(numFiles / entriesPerLeaf)
      // leaves, distributing them by hash. The clustered path does not guarantee a leaf per
      // contiguous group, but it targets exactly that many leaves and drops empty partitions -- so
      // with enough files per leaf that no hash bucket comes out empty, the leaf count lands on the
      // target. 21 files at 7 per leaf targets 3 leaves, and 21 paths spread across 3 buckets leave
      // none empty.
      createAMTTable(name, checkpointInterval = 100)
      // One commit of 21 rows, one row per file (maxRecordsPerFile = 1), so a single INSERT lands
      // 21 live data files. optimizeWrite is disabled so it does not coalesce them back.
      withSQLConf(
          "spark.sql.files.maxRecordsPerFile" -> "1",
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
        sql(s"INSERT INTO $name SELECT * FROM range(21)")
      }
      val deltaLog = deltaLogForName(name)
      assert(deltaLog.update().allFiles.count() == 21,
        s"Expected 21 live files, got ${deltaLog.update().allFiles.count()}.")

      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "7") {
        runIncrementalRewrite(name)
      }
      val path = tablePath(name)
      // 21 live AddFiles, ceil(21 / 7) = 3 leaves (none empty), one root.
      assert(leafFiles(path).size == 3, s"Expected 3 leaves, got ${leafFiles(path).size}.")
      assert(rootFiles(path).size == 1)
    }
  }

  /**
   * Rewrites the whole manifest tree of `tableName` from scratch (the OPTIMIZE-checkpoint write
   * path) and returns the write result plus the read snapshot it rewrote. `incremental` selects the
   * driver-side incremental path (packs live files into leaves in input order) vs. the clustered
   * full-rewrite path (repartitions the live files across executors).
   */
  private def runRewrite(
      tableName: String, incremental: Boolean): (AMTWriteResult, Snapshot) = {
    val triggerName =
      if (incremental) AMTTriggerMode.CheckpointIntervalIncremental.name
      else AMTTriggerMode.CheckpointIntervalFull.name
    val snapshot = deltaLogForName(tableName).update()
    val op = DeltaOperations.OptimizeCheckpoint(
      incremental = incremental, triggerName = triggerName)
    val manager = new AMTWriterManager(snapshot, op)
    val txnInfo = new CurrentTransactionInfo(
      txnId = "txn",
      readPredicates = Vector.empty,
      readFiles = Set.empty,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = snapshot.metadata,
      protocol = snapshot.protocol,
      actions = Seq.empty,
      readSnapshot = snapshot,
      commitInfo = None,
      readRowIdHighWatermark = 0L,
      catalogTable = None,
      domainMetadata = Seq.empty,
      op = op)
    val result = manager.writeAMT(
      commitVersion = snapshot.version + 1,
      currentTransactionInfo = txnInfo,
      preCommitLogSegment = snapshot.logSegment)
    assert(result.isDefined, "A rewrite must emit a manifest tree.")
    (result.get, snapshot)
  }

  /** Drives the clustered full-rewrite (OPTIMIZE-checkpoint) write path. */
  private def runFullRewrite(tableName: String): (AMTWriteResult, Snapshot) =
    runRewrite(tableName, incremental = false)

  /** Drives the driver-side incremental (OPTIMIZE-checkpoint) write path. */
  private def runIncrementalRewrite(tableName: String): (AMTWriteResult, Snapshot) =
    runRewrite(tableName, incremental = true)

  test("full rewrite reconstructs identical table state") {
    withTable("amt_full_rewrite") {
      val name = "amt_full_rewrite"
      createAMTTable(name, checkpointInterval = 100) // Interval far away: no incremental emission.
      sql(s"INSERT INTO $name VALUES (1)") // v1
      sql(s"INSERT INTO $name VALUES (2)") // v2
      sql(s"INSERT INTO $name VALUES (3)") // v3 -- 3 live files.

      // Small leaf size so the rewrite splits the files across multiple leaves.
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "2") {
        val (result, snapshot) = runFullRewrite(name)
        val before = snapshot.allFiles.collect().map(_.path).toSet

        // One root, at least one leaf, and the rewritten tree reconstructs the same file set.
        val provider =
          AMTCheckpointProvider.fromCheckpoint(spark, deltaLogForName(name), result.checkpoint)
        assert(rootFiles(tablePath(name)).size == 1)
        assert(provider.leaves.nonEmpty)
        val reconstructed = provider
          .loadActionsForStateReconstruction(spark, deltaLogForName(name)).get
          .where(col("add").isNotNull)
          .select("add.path")
          .collect()
          .map(_.getString(0))
          .toSet
        assert(reconstructed == before, s"File set changed: before=$before after=$reconstructed")
      }
    }
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
