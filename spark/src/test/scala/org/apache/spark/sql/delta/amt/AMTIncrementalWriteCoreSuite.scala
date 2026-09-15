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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * The incremental-write engine: leafless/promoted-root basics, intermediate/window-commit
 * assembly and replay correctness, and version bookkeeping.
 */
class AMTIncrementalWriteCoreSuite extends AMTIncrementalWriteTestBase {

  test("deleting a file from a promoted root drops it via replay, no leaf, no tombstone") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // Add two more files; all three live in the promoted root (no leaves, under the cap).
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(2), fakeAdd(3)))
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(leafPointers(amtDeltaLog.update()).isEmpty,
          "The tree must stay leafless after a small append to a promoted root.")
        // Delete one root-resident file. It has no back reference, so replay drops it (as on
        // the deferred path): no leaf is touched and, deferred, no root tombstone is written.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, 2)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numRootEntriesExistingStatus = 2))
        val (rootLiveAdds, tombstones) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)
        assert(rootLiveAdds == 2, s"Two files must remain live in the root; got $rootLiveAdds.")
        assert(tombstones == 0,
          s"A deferred remove of a root-resident file writes no tombstone; got $tombstones.")
      }
    }
  }

  test("deleting every file from a promoted root yields an empty, still-readable tree") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(2), fakeAdd(3)))
        commitCheckpoint(amtDeltaLog, incremental = true)
        // Remove all three root-resident files across two intermediate commits. Every remove is a
        // no-backref replay drop, so the new root ends with no live adds and the tree, having never
        // had a leaf, reconstructs an empty live set.
        commitBoth(
          baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, 1), removeOf(amtDeltaLog, 2)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, 3)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics())
        assert(leafPointers(amtDeltaLog.update()).isEmpty,
          "The tree must remain leafless.")
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).isEmpty,
          "The AMT tree must reconstruct an empty live set.")
      }
    }
  }

  test("deleting an old root-resident file drops it via replay, no MDV") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        // Append id=31 -> becomes root-resident (no leaf, no backref).
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = numLeafCountBefore))
        // Delete id=31 (root-resident): remove has NO backref -> dropped by replay, no MDV bit.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numLeafMdvBitsAdded = 0,
            numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("an add then delete of the same file within the intermediate commits is net-zero") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        // The intermediate commits add a file and remove it again: net-zero, so nothing reaches
        // the new tree.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numLeafMdvBitsAdded = 0,
            numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("a leaf file removed then re-added at the same path is masked once") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Window: remove a leaf-resident file (it carries a backref) then re-add the SAME path. The
        // old leaf entry is MDV-masked and the re-added copy becomes a root-resident EXISTING entry
        // (a re-commit of an already-live key, not a new add); reconstructed once. The re-add keeps
        // the back reference the file was stamped with: its path is still the one the leaf holds,
        // and a commit reusing a leaf-resident path must carry that leaf's reference.
        val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numRootEntriesExistingStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
      }
    }
  }

  test("a leaf file re-added and removed again is masked once, not double-counted") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Remove a leaf-resident file, re-add the same path, then remove it again. Both removes
        // carry the same back reference, so both target the same (leaf, position). Note the re-add
        // can only follow a remove: re-adding a path that is still live would be an in-place file
        // metadata update, which a WRITE is not allowed to perform.
        val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        val victimLeaf = leafToAddFileMapping.toSeq.sortBy(_._1).head._1
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        // The two removes share one (leaf, position), which the writer holds as a set, so the write
        // reports a single MDV bit, matching what the leaf's bitmap actually gains. That
        // agreement is what lets this go through the shared validator, whose second check
        // derives the bits from the on-disk dv_cardinality delta.
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
        val maskedLeaf = leafPointers(amtDeltaLog.update()).getOrElse(victimLeaf,
          fail(s"Leaf $victimLeaf must still be carried forward."))
        assert(mdvCardinality(maskedLeaf) == 1L,
          s"The twice-removed position must be masked once; " +
            s"got ${mdvCardinality(maskedLeaf)} bits.")
      }
    }
  }

  test("a re-add at a different path leaves both files live, no double-count") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Delete a leaf-resident file, then add a NEW file at a different path. The leaf gets one
        // MDV bit, the new file is a root-resident live add; both reconstruct exactly once.
        val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numRootEntriesExistingStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
      }
    }
  }

  test("an incremental AMT with no intermediate commits reconstructs unchanged") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        // Immediately checkpoint again with no intervening write: no shape change.
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("a deferred incremental AMT folds in every intermediate commit and counts them") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        // Three separate writes between the full AMT and the deferred incremental. Each
        // adds a distinct root-resident file; the deferred incremental must fold all three in.
        val businessCommits = 3
        (leafPackedFiles + 1 to leafPackedFiles + businessCommits)
          .foreach(fileID => commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(fileID))))
        // The intermediate commits span [oldAMTVersion+1, attemptVersion): the three writes plus
        // the OPTIMIZE CHECKPOINT commit that landed the bootstrap tree. Every appended file stays
        // root-resident.
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = businessCommits,
            numLeavesExistingStatus = numLeafCountBefore),
          expectedNumIntermediateCommits = Some(businessCommits + 1))
      }
    }
  }

  test("replay re-derives the root across a chain of incremental AMTs") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val numLeafCountBefore = leafToAddFileMapping.size

        // incr 1: append one file, which stays root-resident (carried pointers + 1 add <= cap).
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = numLeafCountBefore))

        // incr 2: delete a leaf-resident file -> its leaf gets one MDV bit. The file appended by
        // incr 1 must survive as a root-resident EXISTING entry, which is replay re-deriving the
        // root's live set from the PREVIOUS incremental's root (part 1a).
        val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = numLeafCountBefore - 1,
            numRootEntriesExistingStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = numLeafCountBefore - 1))

        // incr 3: append enough files to push the root past the cap, forcing a spill.
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          (leafPackedFiles + 2 to leafPackedFiles + 15).map(fakeAdd))
        // Spills; the exact shape depends on spill order, so only the live set is asserted.
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("a long mixed chain of writes folds into ONE incremental AMT") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")

        // The opposite packing: NOTHING is checkpointed until the very end, so instead of
        // one write per incremental, all these interleaved appends and deletes land in a single
        // incremental's intermediate commits, to be folded in at once.
        val leafVictims = leafToAddFileMapping.toSeq.sortBy(_._1).flatMap(_._2.take(1))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(leafVictims.head.remove))
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          Seq(fakeAdd(leafPackedFiles + 2), fakeAdd(leafPackedFiles + 3)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(leafVictims(1).remove))
        // A root-resident file added and removed inside the same stream is net-zero.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 4)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, leafPackedFiles + 4)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 5)))

        // One incremental for all seven writes: the 4 surviving net-new files stay root-resident
        // (2 leaf pointers + 4 adds is under the cap of 10), and the two leaf victims each
        // contribute one MDV bit to their own leaf.
        // The intermediate commits are the 7 writes plus the bootstrap's own OPTIMIZE CHECKPOINT.
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 2,
            numOldLeavesUntouched = leafToAddFileMapping.size - 2,
            numRootEntriesExistingStatus = 4,
            numLeafMdvBitsAdded = 2,
            numLeavesModifiedStatus = 2,
            numLeavesExistingStatus = leafToAddFileMapping.size - 2),
          expectedNumIntermediateCommits = Some(8))
      }
    }
  }

  test("re-adding an already-live file leaves it live exactly once, no double-count") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val livePathsBefore = livePathsInLatestAMTCheckpoint(amtDeltaLog)

        // Re-commit a currently-live leaf file's AddFile with NO remove.
        // The re-added AddFile carries the leaf back reference it was reconstructed with.
        val liveLeafFile = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        assert(liveLeafFile.backReference.isDefined, "The re-added file must be leaf-resident.")
        commitBoth(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFile),
          operation = DeltaOperations.ComputeStats(predicate = Nil))
        // The re-added AddFile carries the leaf back reference it was reconstructed with, so the
        // writer recognizes it as a re-commit of an already-live file and keeps it EXISTING in the
        // root (numRootEntriesExistingStatus = 1), not ADDED. The back reference marks the original
        // leaf slot as superseded, so that leaf's MDV masks it (numOldLeavesUpdated = 1,
        // numLeafMdvBitsAdded = 1). The root EXISTING copy and the masked leaf slot net to no
        // change in the live set, and -- crucially -- the file is surfaced exactly once rather
        // than once from the leaf and once from the root.
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numRootEntriesExistingStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog) == livePathsBefore,
          "Re-adding an already-live file must not change the reconstructed live set.")
      }
    }
  }

  test("a deferred incremental AMT describes attemptVersion - 1") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        val lastCommitted = amtDeltaLog.update().version
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = numLeafCountBefore))
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        // A deferred OPTIMIZE CHECKPOINT (no user actions) describes the last committed version,
        // i.e. attemptVersion - 1.
        assert(provider.checkpointAction.version == lastCommitted,
          s"Deferred checkpoint must describe the last committed version $lastCommitted; " +
            s"got ${provider.checkpointAction.version}.")
      }
    }
  }

  test("lastManifestCommitWithFullRewrite is pinned across a chain of incremental AMTs") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need a tree-shaped bootstrap; got $numLeafCountBefore leaves.")
        val fullMarker = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
          .checkpointAction.contentRoot.lastManifestCommitWithFullRewrite
        assert(fullMarker.isDefined, "The full rewrite must set the last-full-rewrite marker.")
        // CREATE TABLE is v0 and the 30-file write is v1, so the full rewrite committed at v2 and
        // describes v1.
        assert(fullMarker.contains(1L), s"The full rewrite must be pinned to v1; got $fullMarker.")
        assertCheckpointDescribesVersion(amtDeltaLog, expectedVersion = 1L)

        // Two successive incrementals must both carry the SAME marker (pinned to the full rewrite),
        // while each describes the write it followed: the write lands at v3 and its deferred
        // checkpoint at v4 describes v3, then v5 / v6 describes v5.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = numLeafCountBefore))
        assertCheckpointDescribesVersion(amtDeltaLog, expectedVersion = 3L)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 2)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numRootEntriesExistingStatus = 2,
            numLeavesExistingStatus = numLeafCountBefore))
        assertCheckpointDescribesVersion(amtDeltaLog, expectedVersion = 5L)
        val marker = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
          .checkpointAction.contentRoot.lastManifestCommitWithFullRewrite
        assert(marker == fullMarker,
          s"Incrementals must carry the full-rewrite marker forward unchanged: " +
            s"full=$fullMarker incr=$marker.")
      }
    }
  }
}
