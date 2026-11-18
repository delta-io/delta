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

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, ContentRoot, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.ManifestBitmap
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.functions.{col, lit}

/** Invariant enforcement: rejecting commits that break a write- or commit-shape invariant. */
class AMTIncrementalWriteInvariantSuite extends AMTIncrementalWriteTestBase {

  import testImplicits._

  private def processedActions(
      oldRootAdds: Seq[AddFile],
      actionsToCommit: Seq[Action]): ProcessedActions =
    new ProcessedActions(
      oldAMTVersion = 0L,
      oldRootAdds = oldRootAdds,
      nonContentFromOldCheckpoint = Seq[Action](Protocol(), Metadata()),
      windowCommits = Nil,
      windowCommitActions = Nil,
      attemptVersion = 1L,
      actionsToCommit = actionsToCommit,
      tableRoot = new org.apache.hadoop.fs.Path("s3://bucket/prefix/amt_test_table"),
      useDeletionVectorObjectIdentity = true)

  test("writeIncremental rejects intermediate commits with a hole up to attemptVersion") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(1), fakeAdd(2)))
        // A base AMT to build intermediate commits on.
        commitCheckpoint(amtDeltaLog, incremental = false)
        val snapshot = amtDeltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
        val oldAMTVersion = provider.checkpointAction.contentRoot.version
        val intermediateLogCommits = snapshot.logSegment.deltas
          .filter(f => FileNames.getFileVersion(f) > oldAMTVersion)
        // They only reach snapshot.version, so [oldAMTVersion+1, snapshot.version+5) has a
        // hole -> the Step-0 coverage assert must fire.
        intercept[AssertionError] {
          new IncrementalAMTWriter(spark, amtDeltaLog).writeIncremental(
            oldAMTActionsProvider =
              new BaseAMTCheckpointActionsProvider(spark, amtDeltaLog, provider),
            intermediateLogCommits = intermediateLogCommits,
            attemptVersion = snapshot.version + 5,
            actionsToCommit = Seq.empty,
            snapshotId = 1L,
            trigger = AMTTriggerMode.CheckpointIntervalIncremental.name)
        }
      }
    }
  }

  test("writeIncremental rejects a back reference to a leaf manifest not in the base tree") {
    withSQLConf(leafPackingConfs: _*) {
      withAmtTable { amtDeltaLog =>
        commitAmt(amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val (staleLeaf, staleLeafFiles) = leafToAddFileMap(amtDeltaLog).toSeq.sortBy(_._1).head
        val staleRemove = staleLeafFiles.head.remove
        // A full rewrite writes new leaf manifests, so the new base tree does not hold `staleLeaf`.
        commitCheckpoint(amtDeltaLog, incremental = false)
        val snapshot = amtDeltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
        assert(!provider.leaves.exists(_.location == staleLeaf))
        val oldAMTVersion = provider.checkpointAction.contentRoot.version
        val intermediateLogCommits = snapshot.logSegment.deltas
          .filter(f => FileNames.getFileVersion(f) > oldAMTVersion)
        // `verifyAmtBackReferences` rejects `staleRemove` at commit time, so the test calls the
        // writer directly.
        val ex = intercept[IllegalStateException] {
          new IncrementalAMTWriter(spark, amtDeltaLog).writeIncremental(
            oldAMTActionsProvider =
              new BaseAMTCheckpointActionsProvider(spark, amtDeltaLog, provider),
            intermediateLogCommits = intermediateLogCommits,
            attemptVersion = snapshot.version + 1,
            actionsToCommit = Seq(staleRemove),
            snapshotId = 1L,
            trigger = AMTTriggerMode.InlineWithLargeCommitIncremental.name)
        }
        assert(ex.getMessage.contains("not live leaves of the base tree") &&
          ex.getMessage.contains(staleLeaf),
          s"expected the base tree back reference invariant; got: ${ex.getMessage}")
      }
    }
  }

  test("an inline no-backref remove with no originating AddFile is rejected") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        // A no-backref remove for a path that was never added has no originating AddFile in
        // root + window, so buildRootRemoveEntries cannot build a CDF entry, and rejects it.
        val ex = intercept[IllegalStateException] {
          withInline {
            amtDeltaLog.startTransaction().commit(Seq(fakeAdd(999999).remove), writeOperation)
          }
        }
        assert(ex.getMessage.contains("No originating AddFile"),
          s"expected the missing-origin invariant; got: ${ex.getMessage}")
      }
    }
  }

  test("an inline same-key re-add of an already-live file with dataChange=true is rejected") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Re-commit a currently-live leaf file under its SAME (path, dv) key -- carrying the leaf
        // back reference it was reconstructed with -- in one inline commit with dataChange = true.
        // A same-key re-add of an already-live file is a metadata-only refresh
        // (dataChange = false); a data-changing re-add is rejected by the incremental writer.
        val liveLeafFile = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        assert(liveLeafFile.backReference.isDefined, "The re-added file must be leaf-resident.")
        val ex = withSQLConf(
          DeltaSQLConf.DELTA_COMMIT_VALIDATE_EXPECTED_DATA_CHANGE_MODE.key ->
            DeltaSQLConf.DataChangeValidationMode.OFF.toString) {
          intercept[IllegalStateException] {
            withInline {
              amtDeltaLog.startTransaction().commit(
                Seq(liveLeafFile.copy(dataChange = true)),
                DeltaOperations.ComputeStats(predicate = Nil))
            }
          }
        }
        assert(ex.getMessage.contains("dataChange=true is not allowed"),
          s"expected the data-changing re-add rejection; got: ${ex.getMessage}")
      }
    }
  }

  test("(1) a dataChange=true commit rejects a same-key re-add of an already-live file") {
    val ex = intercept[IllegalStateException] {
      processedActions(
        oldRootAdds = Seq(fakeAdd(1)),
        actionsToCommit = Seq(fakeAdd(1, dataChange = true)))
    }
    assert(ex.getMessage.contains("dataChange=true is not allowed"),
      s"expected invariant (1); got: ${ex.getMessage}")
  }

  test("(2) a dataChange=false commit with removes rejects a re-add of an already-live file") {
    val ex = intercept[IllegalStateException] {
      processedActions(
        oldRootAdds = Seq(fakeAdd(1), fakeAdd(2)),
        actionsToCommit = Seq(
          fakeAdd(1).remove.copy(dataChange = false),
          fakeAdd(2, dataChange = false)))
    }
    assert(ex.getMessage.contains("must not re-add an already-live file"),
      s"expected invariant (2); got: ${ex.getMessage}")
  }

  test("(3) a dataChange=false commit with no removes rejects a new (non-re-add) file") {
    val ex = intercept[IllegalStateException] {
      processedActions(
        oldRootAdds = Seq(fakeAdd(1)),
        actionsToCommit = Seq(fakeAdd(99, dataChange = false)))
    }
    assert(ex.getMessage.contains("must re-add only already-live files"),
      s"expected invariant (3); got: ${ex.getMessage}")
  }

  test("the three legal commit shapes construct and classify their adds correctly") {
    // (1)-legal: a data-changing append of a genuinely new file -> not a re-add.
    val append = processedActions(
      oldRootAdds = Seq(fakeAdd(1)),
      actionsToCommit = Seq(fakeAdd(2, dataChange = true)))
    assert(append.reCommittedLiveAdd.isEmpty)
    // (2)-legal: a metadata-only compaction removing a live file and adding a fresh one -> not a
    // re-add of an already-live file.
    val compaction = processedActions(
      oldRootAdds = Seq(fakeAdd(1)),
      actionsToCommit = Seq(
        fakeAdd(1).remove.copy(dataChange = false),
        fakeAdd(2, dataChange = false)))
    assert(compaction.reCommittedLiveAdd.isEmpty)
    // (3)-legal: a metadata-only stats refresh re-adding an already-live file under the same key.
    val refresh = processedActions(
      oldRootAdds = Seq(fakeAdd(1)),
      actionsToCommit = Seq(fakeAdd(1, dataChange = false)))
    assert(refresh.reCommittedLiveAdd.isDefined)
  }

  /**
   * A carried leaf pointer whose manifest_info carries the given per-status file counts and, if
   * `mdvPositions` is non-empty, a manifest DV that an earlier commit set at those positions.
   */
  private def carriedLeafPointer(
      addedFiles: Int = 0,
      existingFiles: Int = 0,
      deletedFiles: Int = 0,
      replacedFiles: Int = 0,
      modifiedFiles: Int = 0,
      mdvPositions: Seq[Int] = Seq.empty): DataManifestEntry =
    DataManifestEntry(
      location = "metadata/leaf-guard.parquet",
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = Tracking(
        status = Tracking.Status.Existing,
        snapshot_id = None, modified_snapshot_id = None, sequence_number = None,
        file_sequence_number = None, first_row_id = None,
        deleted_positions = None, replaced_positions = None),
      record_count =
        (addedFiles + existingFiles + deletedFiles + replacedFiles + modifiedFiles).toLong,
      file_size_in_bytes = 100L,
      manifest_info = ManifestInfo(
        format_version = AMTSingleAction.FormatVersionV4,
        added_files_count = addedFiles, existing_files_count = existingFiles,
        deleted_files_count = deletedFiles, replaced_files_count = replacedFiles,
        modified_files_count = modifiedFiles,
        added_rows_count = 0L, existing_rows_count = 0L,
        deleted_rows_count = 0L, replaced_rows_count = 0L, modified_rows_count = 0L,
        min_sequence_number = 0L,
        dv =
          if (mdvPositions.isEmpty) None
          else Some(AMTUtils.serializeMdv(ManifestBitmap.fromPositions(mdvPositions)))))

  // Other engines (e.g. Iceberg after a delete + insert) can write a leaf that holds both live
  // files and tombstones. The carried leaf's tracking must follow its live files only.
  test("carryForwardOneLeaf keeps an untouched leaf mixing live files and tombstones EXISTING") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1)
      val carried = writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq.empty,
        deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Existing)
      assert(carried.manifest_info == mixed.manifest_info)
    }
  }

  test("carryForwardOneLeaf marks a mixed leaf MODIFIED when some live files are masked") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // Positions 0 and 1 are the live files; position 2 is the tombstone.
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1)
      val carried = writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq(0),
        deletedPositions = Seq(0), replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Modified)
      assert(carried.manifest_info.dvCardinality == 1L)
    }
  }

  test("carryForwardOneLeaf marks a mixed leaf DELETED when all live files are masked") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1)
      val carried = writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq(0, 1),
        deletedPositions = Seq(0, 1), replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Deleted)
      assert(carried.tracking.snapshot_id.contains(1L))
    }
  }

  test("carryForwardOneLeaf drops a mixed leaf whose earlier MDV masks every live file") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // Another engine masked both live files (positions 0 and 1) but left the leaf live.
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1, mdvPositions = Seq(0, 1))
      val carried = writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq.empty,
        deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Deleted)
      assert(carried.tracking.snapshot_id.contains(1L))
      assert(carried.manifest_info == mixed.manifest_info)
    }
  }

  test("carryForwardOneLeaf keeps a mixed leaf EXISTING when an earlier MDV masks part of it") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // Position 1 is still live, so the leaf stays even though the MDV and tombstone cover two
      // of its three entries.
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1, mdvPositions = Seq(0))
      val carried = writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq.empty,
        deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Existing)
      assert(carried.manifest_info == mixed.manifest_info)
    }
  }

  test("carryForwardOneLeaf rejects an MDV that masks more entries than the live files") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // Three masked positions but only two live files: the MDV masks the tombstone too.
      val mixed =
        carriedLeafPointer(existingFiles = 2, deletedFiles = 1, mdvPositions = Seq(0, 1, 2))
      val ex = intercept[IllegalStateException] {
        writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq.empty,
          deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      }
      assert(ex.getMessage.contains("masks 3 entries but holds only 2 live files"),
        s"expected the MDV-exceeds-live-files invariant; got: ${ex.getMessage}")
    }
  }

  test("carryForwardOneLeaf drops a live-only leaf whose earlier MDV masks every entry") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      val masked = carriedLeafPointer(existingFiles = 2, mdvPositions = Seq(0, 1))
      val carried = writer.carryForwardOneLeaf(masked, newMdvPositions = Seq.empty,
        deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Deleted)
      assert(carried.tracking.snapshot_id.contains(1L))
    }
  }

  test("carryForwardOneLeaf drops a tombstone-only leaf") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      val tombstoneOnly = carriedLeafPointer(deletedFiles = 2, replacedFiles = 1)
      val carried = writer.carryForwardOneLeaf(tombstoneOnly, newMdvPositions = Seq.empty,
        deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      assert(carried.tracking.status == Tracking.Status.Deleted)
      assert(carried.manifest_info == tombstoneOnly.manifest_info)
    }
  }

  test("carryForwardOneLeaf rejects new MDV positions on a leaf with no live file") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // A tombstone-only carried leaf holds no live entry, so it cannot gain new MDV positions --
      // there is nothing left to mask.
      val tombstoneOnly = carriedLeafPointer(deletedFiles = 3)
      val ex = intercept[IllegalStateException] {
        writer.carryForwardOneLeaf(tombstoneOnly, newMdvPositions = Seq(0),
          deletedPositions = Seq.empty, replacedPositions = Seq.empty, snapshotId = 1L)
      }
      assert(ex.getMessage.contains("no live file but gained new MDV positions"),
        s"expected the dead-leaf-MDV invariant; got: ${ex.getMessage}")
    }
  }

  private val ghostLocation = "ghost-removed-by-other-engine.parquet"

  /** The leaf entry locations of `leaf`, in physical order. */
  private def leafLocations(deltaLog: DeltaLog, leaf: DataManifestEntry): Seq[String] =
    allowReadWithinDeltaLog {
      spark.read.parquet(leaf.getAbsolutePath(deltaLog.dataPath).toString)
        .orderBy(col("_metadata.row_index"))
        .select(col("location")).as[String].collect().toSeq
    }

  /**
   * Writes a copy of `leaf` with one DELETED entry appended after its live entries, and returns
   * the root pointer for the copy. The live entries keep their positions.
   */
  private def writeMixedLeaf(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      leaf: DataManifestEntry): DataManifestEntry = {
    val tableRoot = deltaLog.dataPath
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val schema = AMTSingleAction.persistedSchema(checkpoint.metaData, checkpoint.protocol)
    val mixedLeafFile = FileNames.newAMTLeafManifestFile(FileNames.amtMetadataDirPath(tableRoot))
    val fs = mixedLeafFile.getFileSystem(hadoopConf)
    val staging = new Path(tableRoot, "_staging_mixed_leaf")
    val ghostRecordCount = allowReadWithinDeltaLog {
      val entries = spark.read.schema(schema).parquet(leaf.getAbsolutePath(tableRoot).toString)
      // The read schema is the persisted one made nullable. Casting back to it keeps the Parquet
      // field ids (nested ones included) that the reader resolves columns by.
      val tombstone = entries.limit(1).select(entries.schema.fields.toIndexedSeq.map { field =>
        val value = field.name match {
          case "location" => lit(ghostLocation)
          case "tracking" =>
            col("tracking").withField("status", lit(Tracking.Status.Deleted))
          case _ => col(field.name)
        }
        value.cast(field.dataType).as(field.name, field.metadata)
      }: _*)
      // Union does not keep row order, so sort on each row's position with the tombstone last.
      val position = "_mixed_leaf_position"
      entries.withColumn(position, col("_metadata.row_index"))
        .union(tombstone.withColumn(position, lit(Long.MaxValue)))
        .repartition(1)
        .sortWithinPartitions(position)
        .drop(position)
        .write.parquet(staging.toString)
      tombstone.select(col("record_count")).as[Long].head()
    }
    val part = fs.listStatus(staging).map(_.getPath).find(_.getName.endsWith(".parquet"))
      .getOrElse(fail("the staged mixed leaf must have a parquet file"))
    assert(fs.rename(part, mixedLeafFile))
    fs.delete(staging, true)

    val mixed = leaf.copy(
      location = AMTUtils.relativizeLocation(tableRoot.toString, mixedLeafFile.toString),
      file_size_in_bytes = fs.getFileStatus(mixedLeafFile).getLen,
      record_count = leaf.record_count + 1,
      manifest_info = leaf.manifest_info.copy(
        deleted_files_count = leaf.manifest_info.deleted_files_count + 1,
        deleted_rows_count = leaf.manifest_info.deleted_rows_count + ghostRecordCount))
    assert(leafLocations(deltaLog, mixed) == leafLocations(deltaLog, leaf) :+ ghostLocation,
      "the mixed leaf must keep the live entries in place and append the tombstone")
    mixed
  }

  /**
   * Points the table's current manifest tree at a new root in which `leaf` is replaced by a copy
   * that also holds a tombstone. Returns the mixed leaf's root pointer.
   */
  private def installMixedLeaf(
      tableName: String,
      deltaLog: DeltaLog,
      leaf: DataManifestEntry): DataManifestEntry = {
    val snapshot = deltaLog.update()
    val provider = amtProvider(snapshot).getOrElse(fail("expected an AMT-backed snapshot"))
    val checkpoint = provider.checkpointAction
    val tableRoot = deltaLog.dataPath
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val oldRoot = checkpoint.contentRoot.getAbsolutePath(tableRoot).toString
    assert(withManifestDataEntries(Seq(oldRoot))(_.count()) == 0,
      "the root must hold only leaf pointers, so rebuilding it from them loses nothing")

    val mixed = writeMixedLeaf(deltaLog, checkpoint, leaf)
    val rootFile = FileNames.newAMTRootManifestFile(FileNames.amtMetadataDirPath(tableRoot))
    val rootEntries = provider.leaves.map(l => if (l.location == leaf.location) mixed else l)
    AMTWriteHelper.writeAMTParquet(
      spark, hadoopConf, rootFile, checkpoint.metaData, checkpoint.protocol,
      rootEntries.map(_.wrap))
    val newCheckpoint = updateCheckpointActions(checkpoint) {
      case root: ContentRoot => root.copy(
        path = AMTUtils.relativizeLocation(tableRoot.toString, rootFile.toString),
        sizeInBytes = rootFile.getFileSystem(hadoopConf).getFileStatus(rootFile).getLen)
    }

    // Backfill first, so the commit coordinator no longer serves the staged copy of the manifest
    // commit, and rewrite the backfilled file.
    snapshot.ensureCommitFilesBackfilled(Some(catalogTableFor(tableName)))
    val commitPath = FileNames.unsafeDeltaFile(deltaLog.logPath, provider.manifestCommitVersion)
    val actions = deltaLog.store.read(commitPath, hadoopConf).map(Action.fromJson)
    assert(actions.count(_.isInstanceOf[Checkpoint]) == 1)
    val rewritten = actions.map {
      case _: Checkpoint => newCheckpoint.json
      case action => action.json
    }
    deltaLog.store.write(commitPath, rewritten.iterator, overwrite = true, hadoopConf)
    // The hint carries the whole Checkpoint, so a stale one would point at the old root.
    deltaLog.LAST_CHECKPOINT.getFileSystem(hadoopConf).delete(deltaLog.LAST_CHECKPOINT, false)
    DeltaLog.clearCache()
    mixed
  }

  /** The current tree's pointer for the leaf at `location`, if the tree still lists it. */
  private def currentLeaf(tableName: String, location: String): Option[DataManifestEntry] =
    amtProvider(deltaLogForName(tableName).update())
      .getOrElse(fail("expected an AMT-backed snapshot"))
      .leaves.find(_.location == location)

  /** The decoded file name of a URL-encoded path, as AddFile, RemoveFile and _metadata use. */
  private def fileName(urlEncodedPath: String): String =
    SparkPath.fromUrlString(urlEncodedPath).toPath.getName

  /** File names of the RemoveFile and AddFile actions the current tree reconstructs. */
  private def reconstructedChanges(tableName: String): (Set[String], Set[String]) = {
    val deltaLog = deltaLogForName(tableName)
    val actions = amtProvider(deltaLog.update())
      .getOrElse(fail("expected an AMT-backed snapshot"))
      .reconstructFileActions(deltaLog, dataChange = true, commitTimestamp = 0L)
    def names(paths: Seq[String]): Set[String] = paths.map(fileName).toSet
    (names(actions.collect { case r: RemoveFile => r.path }),
      names(actions.collect { case a: AddFile => a.path }))
  }

  private def ids(tableName: String): Seq[Int] =
    sql(s"SELECT id FROM $tableName").as[Int].collect().toSeq.sorted

  // Unlike the carryForwardOneLeaf tests above, this runs real Delta writes on a table whose tree
  // holds a mixed leaf, so it also covers the change feed and the table's rows.
  test("Delta writes carry a leaf mixing live files and tombstones through every leaf state") {
    val tableName = "amt_mixed_leaf"
    withSQLConf(leafPackingConfs ++ Seq(
        DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key ->
          Int.MaxValue.toString): _*) {
      withTable(tableName) {
        createAMTTable(tableName, checkpointInterval = 1000)
        appendRowsAsSeparateFiles(tableName, leafPackedFiles)
        val originalLog = deltaLogForName(tableName)
        commitCheckpoint(originalLog, incremental = false)
        val leaves = amtProvider(originalLog.update())
          .getOrElse(fail("expected an AMT-backed snapshot")).leaves
        assertLeafCount(leaves)
        val mixed = installMixedLeaf(tableName, originalLog, leaves.head)
        // The cache was cleared, so this log loads the installed tree.
        val deltaLog = deltaLogForName(tableName)
        val idByFileName = sql(s"SELECT id, _metadata.file_path FROM $tableName")
          .as[(Int, String)].collect().map { case (id, path) => fileName(path) -> id }.toMap
        // Leaf entries store decoded paths.
        val liveFileNames = leafLocations(deltaLog, mixed).dropRight(1).map(new Path(_).getName)
        val liveIds = liveFileNames.map(idByFileName)
        val allIds = (0 until leafPackedFiles)

        // The reader sees the mixed leaf and skips its tombstone.
        val installed = currentLeaf(tableName, mixed.location)
          .getOrElse(fail("the installed tree must list the mixed leaf"))
        assert(installed.manifest_info.liveFilesCount == liveIds.size)
        assert(installed.manifest_info.tombstoneFilesCount == 1)
        assert(ids(tableName) == allIds)

        // An insert leaves the mixed leaf untouched: it is carried forward EXISTING, and the
        // write logs that it carried one mixed leaf.
        val insertedId = leafPackedFiles
        val mixedLeafEvents = Log4jUsageLogger.track {
          sql(s"INSERT INTO $tableName VALUES ($insertedId)")
          commitCheckpoint(deltaLog, incremental = true)
        }.filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
          e.tags.get("opType").contains(AMTUsageLogs.INCREMENTAL_WRITE_MIXED_LEAVES_CARRIED))
        assert(mixedLeafEvents.size == 1,
          s"expected one mixed-leaf usage log, got $mixedLeafEvents")
        val mixedLeafEventData = JsonUtils.fromJson[Map[String, Any]](mixedLeafEvents.head.blob)
        assert(mixedLeafEventData("numMixedLeaves").toString == "1")
        assert(mixedLeafEventData("numCarriedLeaves").toString == leaves.size.toString)
        val untouched = currentLeaf(tableName, mixed.location)
          .getOrElse(fail("an untouched mixed leaf must stay in the tree"))
        assert(untouched.tracking.status == Tracking.Status.Existing)
        assert(untouched.manifest_info == mixed.manifest_info)
        assert(ids(tableName) == allIds :+ insertedId)

        // Deleting one live file masks it: MODIFIED, and the change feed removes only that file.
        // The DELETEs write their trees inline, since only then does a tree carry the commit's
        // change-feed positions.
        withInline {
          sql(s"DELETE FROM $tableName WHERE id = ${liveIds.head}")
        }
        val modified = currentLeaf(tableName, mixed.location)
          .getOrElse(fail("a partly masked mixed leaf must stay in the tree"))
        assert(modified.tracking.status == Tracking.Status.Modified)
        assert(modified.manifest_info.dvCardinality == 1L)
        val deletedPositions = modified.tracking.deleted_positions
          .map(ManifestBitmap.fromSerializedByteArray)
          .getOrElse(fail("the MODIFIED leaf must record this commit's deleted positions"))
        assert(deletedPositions.cardinality == 1L && deletedPositions.contains(0))
        assert(reconstructedChanges(tableName) == ((Set(liveFileNames.head), Set.empty[String])))
        assert(ids(tableName) == (allIds :+ insertedId).filterNot(_ == liveIds.head))

        // Deleting the remaining live files masks every live entry: DELETED, even though the
        // leaf still holds its tombstone.
        withInline {
          sql(s"DELETE FROM $tableName WHERE id IN (${liveIds.tail.mkString(", ")})")
        }
        val deleted = currentLeaf(tableName, mixed.location)
          .getOrElse(fail("the DELETED pointer must be emitted once"))
        assert(deleted.tracking.status == Tracking.Status.Deleted)
        assert(reconstructedChanges(tableName) ==
          ((liveFileNames.tail.toSet, Set.empty[String])))
        val remainingIds = (allIds :+ insertedId).filterNot(liveIds.contains)
        assert(ids(tableName) == remainingIds)

        // The next tree drops the DELETED leaf.
        sql(s"INSERT INTO $tableName VALUES (${insertedId + 1})")
        commitCheckpoint(deltaLog, incremental = true)
        assert(currentLeaf(tableName, mixed.location).isEmpty)
        assert(ids(tableName) == remainingIds :+ (insertedId + 1))
      }
    }
  }
}
