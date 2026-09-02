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

import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.util.FileNames

/** Invariant enforcement: rejecting commits that break a write- or commit-shape invariant. */
class AMTIncrementalWriteInvariantSuite extends AMTIncrementalWriteTestBase {

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
            oldAMTVersion = oldAMTVersion,
            oldAMTCheckpointProvider = provider,
            intermediateLogCommits = intermediateLogCommits,
            attemptVersion = snapshot.version + 5,
            actionsToCommit = Seq.empty,
            trigger = AMTTriggerMode.CheckpointIntervalIncremental.name)
        }
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
        val ex = intercept[IllegalStateException] {
          withInline {
            amtDeltaLog.startTransaction().commit(
              Seq(liveLeafFile.copy(dataChange = true)),
              DeltaOperations.ComputeStats(predicate = Nil))
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

  /** A carried leaf pointer whose manifest_info carries the given per-status file counts. */
  private def carriedLeafPointer(
      addedFiles: Int = 0,
      existingFiles: Int = 0,
      deletedFiles: Int = 0,
      replacedFiles: Int = 0,
      modifiedFiles: Int = 0): DataManifestEntry =
    DataManifestEntry(
      location = "metadata/leaf-guard.parquet",
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = Tracking(
        status = Tracking.Status.Existing,
        snapshot_id = None, dv_snapshot_id = None, sequence_number = None,
        file_sequence_number = None, first_row_id = None,
        deleted_positions = None, replaced_positions = None),
      record_count =
        (addedFiles + existingFiles + deletedFiles + replacedFiles + modifiedFiles).toLong,
      file_size_in_bytes = 100L,
      manifest_info = ManifestInfo(
        added_files_count = addedFiles, existing_files_count = existingFiles,
        deleted_files_count = deletedFiles, replaced_files_count = replacedFiles,
        modified_files_count = modifiedFiles,
        added_rows_count = 0L, existing_rows_count = 0L,
        deleted_rows_count = 0L, replaced_rows_count = 0L, modified_rows_count = 0L,
        min_sequence_number = 0L, dv = None, dv_cardinality = None))

  test("carryForwardOneLeaf rejects a carried leaf mixing live files and tombstones") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // A carried leaf whose manifest_info counts both a live file and a tombstone is a corrupt
      // shape the writer does not support.
      val mixed = carriedLeafPointer(existingFiles = 2, deletedFiles = 1)
      val ex = intercept[IllegalStateException] {
        writer.carryForwardOneLeaf(mixed, newMdvPositions = Seq.empty,
          deletedPositions = Seq.empty, replacedPositions = Seq.empty)
      }
      assert(ex.getMessage.contains("mix of live files and tombstones"),
        s"expected the live+tombstone-mix invariant; got: ${ex.getMessage}")
    }
  }

  test("carryForwardOneLeaf rejects new MDV positions on a leaf with no live file") {
    withTables() { (_, amtDeltaLog) =>
      val writer = new IncrementalAMTWriter(spark, amtDeltaLog)
      // A tombstone-only carried leaf holds no live entry, so it cannot gain new MDV positions --
      // there is nothing left to mask.
      val tombstoneOnly = carriedLeafPointer(deletedFiles = 3)
      val ex = intercept[IllegalStateException] {
        writer.carryForwardOneLeaf(tombstoneOnly, newMdvPositions = Seq(0L),
          deletedPositions = Seq.empty, replacedPositions = Seq.empty)
      }
      assert(ex.getMessage.contains("no live file but gained new MDV positions"),
        s"expected the dead-leaf-MDV invariant; got: ${ex.getMessage}")
    }
  }
}
