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
import org.apache.spark.sql.delta.test.shims.GridTestShim

/** The inline emission path: statuses, tombstones, DELETE vs REPLACE, and inline re-adds. */
class AMTIncrementalWriteInlineSuite extends AMTIncrementalWriteTestBase with GridTestShim {

  test("an inline write emits an incremental AMT that describes its own commit version") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leavesBefore.size} leaves.")

        // Inline-append one net-new file. It stays root-resident (carried pointers + 1 add is under
        // the cap), so no leaf is added or rewritten.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesAddedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(Seq(fakeAdd(31))))
        assert(leafPointers(amtDeltaLog.update()).keySet == leavesBefore,
          "An append below the spill threshold must add no leaf and rewrite none.")
      }
    }
  }

  test("an inline-appended file is ADDED, then EXISTING once carried by the next commit") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        // A full-rewrite root holds only leaf pointers, so it has no root-resident DATA entries.
        val bootstrapCounts = rootDataEntryStatusToCount(amtDeltaLog)
        assert(bootstrapCounts.isEmpty,
          s"A full-rewrite root must hold no DATA entries; got $bootstrapCounts.")
        // Append one net-new file INLINE: this commit inserts it, so it is ADDED. (A deferred fold
        // would carry it forward as EXISTING, since that insert belongs to its own commit's CDF.)
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(31))) }
        val afterFirst = rootDataEntryStatusToLocations(amtDeltaLog)
        assert(afterFirst == Map(Tracking.Status.Added -> Set(fakeAdd(31).path)),
          s"the freshly appended file 31 must be ADDED; got $afterFirst.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
        // Append a second file INLINE: this commit inserts the second (32) as ADDED and carries the
        // first (31), which decays to EXISTING.
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(32))) }
        val afterSecond = rootDataEntryStatusToLocations(amtDeltaLog)
        assert(afterSecond == Map(
          Tracking.Status.Added -> Set(fakeAdd(32).path),
          Tracking.Status.Existing -> Set(fakeAdd(31).path)),
          s"file 32 must be ADDED and file 31 EXISTING; got $afterSecond.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("an inline delete of a leaf-resident file masks it via a cumulative MDV") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leavesBefore.size} leaves.")
        val physicalEntriesBefore = leafLiveDataEntryCount(amtDeltaLog.update())
        assert(physicalEntriesBefore == 30,
          "Every bootstrap file is spread across the AMT's leaves.")

        // Inline delete of a leaf-resident file: the owning leaf is carried forward by pointer and
        // masked with one cumulative MDV bit; the leaf parquet keeps every physical entry.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leavesBefore.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeafDeleteCDFBitsAdded = 1,
            numLeavesExistingStatus = leavesBefore.size - 1,
            numLeavesModifiedStatus = 1),
          inlineAMTCommitActions = Some(Seq(removeOf(amtDeltaLog, 1))))
        assert(leafPointers(amtDeltaLog.update()).keySet == leavesBefore,
          "The delete must carry every leaf forward, not rewrite or drop one.")
        assert(leafLiveDataEntryCount(amtDeltaLog.update()) == physicalEntriesBefore,
          "The carried leaf keeps every physical entry; a delete only sets the MDV.")
      }
    }
  }

  test("an inline delete of a root-resident file writes a root tombstone for CDF") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leavesBefore.size} leaves.")

        // Inline-append one net-new file; it stays root-resident (under the spill threshold), so
        // its later remove carries no back reference.
        val rootId = 31
        withInline {
          commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(rootId)))
        }
        val (rootLiveAddsBefore, tombstonesBefore) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)
        assert(rootLiveAddsBefore == 1L,
          s"The appended file must be the one root-resident live add; got $rootLiveAddsBefore.")
        assert(tombstonesBefore == 0L, "An append must not write a tombstone.")

        // Inline delete of the root-resident file. The deferred path drops such a file through
        // replay with NO tombstone; inline instead has the remove in actionsToCommit, so it
        // becomes a tracking=removed root entry for CDF.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesDeletedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(Seq(removeOf(amtDeltaLog, rootId))))
        val (rootLiveAdds, tombstones) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)
        assert(tombstones == 1L,
          s"Removing a root-resident file inline must leave one root tombstone; got $tombstones.")
        assert(rootLiveAdds == 0L,
          s"The removed file must no longer be a live root add; got $rootLiveAdds.")
        // A no-backref remove is replay-resolved; it must not touch any leaf's MDV.
        leafPointers(amtDeltaLog.update()).foreach { case (location, leaf) =>
          assert(mdvCardinality(leaf) == 0L,
            s"Removing a root-resident file must not touch leaf $location's MDV.")
        }
      }
    }
  }

  test("an inline leaf delete stamps deleted_positions, and resets it on the next commit") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leavesBefore.size} leaves.")

        // Inline delete of a leaf-resident file stamps this commit's deleted position on the owning
        // leaf. deleted_positions is sourced from this commit's with-backref removes, so only the
        // inline path populates it.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leavesBefore.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeafDeleteCDFBitsAdded = 1,
            numLeavesExistingStatus = leavesBefore.size - 1,
            numLeavesModifiedStatus = 1),
          inlineAMTCommitActions = Some(Seq(removeOf(amtDeltaLog, 1))))
        val stamped =
          leafPointers(amtDeltaLog.update()).values.filter(leafDeletedPositions(_).nonEmpty).toSeq
        assert(stamped.size == 1,
          s"Exactly one leaf must carry this commit's deleted_positions; got ${stamped.size}.")
        assert(leafDeletedPositions(stamped.head).size == 1,
          s"deleted_positions must hold this commit's single deletion; " +
            s"got ${leafDeletedPositions(stamped.head)}.")
        assert(mdvCardinality(stamped.head) == 1L,
          "The same leaf's cumulative MDV must also carry that one bit.")

        // A following inline commit that deletes from no leaf must RESET deleted_positions (it is
        // per-commit, never the stale prior value); the cumulative MDV persists.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesAddedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(Seq(fakeAdd(31))))
        val afterAppend = amtDeltaLog.update()
        leafPointers(afterAppend).foreach { case (location, leaf) =>
          assert(leafDeletedPositions(leaf).isEmpty,
            s"deleted_positions must reset on leaf $location when this commit deletes nothing " +
              s"from it; got ${leafDeletedPositions(leaf)}.")
        }
        assert(leafPointers(afterAppend).values.map(mdvCardinality).sum == 1L,
          "The cumulative MDV bit from the earlier delete must survive the append.")
      }
    }
  }


  test("an inline leaf-resident REPLACE stamps replaced_positions and re-adds MODIFIED") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2, s"Need a tree-shaped bootstrap; got ${leavesBefore.size}.")

        // Inline REPLACE of a leaf file: remove f, re-add it under a new DV, in one commit.
        // The owning leaf is carried forward with the position masked in its cumulative MDV and
        // recorded as this commit's replaced_positions (not deleted_positions). The re-added
        // copy is a live MODIFIED root DATA entry.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leavesBefore.size - 1,
            numRootEntriesModifiedStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeafReplaceCDFBitsAdded = 1,
            numLeavesExistingStatus = leavesBefore.size - 1,
            numLeavesModifiedStatus = 1),
          inlineAMTCommitActions = Some(removeAndReAddWithDV(amtDeltaLog, Left(1))))
        val replaced = leafPointers(amtDeltaLog.update()).values
          .filter(leafReplacedPositions(_).nonEmpty).toSeq
        assert(replaced.size == 1,
          s"Exactly one leaf must carry this commit's replaced_positions; got ${replaced.size}.")
        assert(leafReplacedPositions(replaced.head).size == 1 &&
          leafDeletedPositions(replaced.head).isEmpty,
          s"REPLACE sets replaced_positions, not deleted; got ${replaced.head.tracking}.")
        assert(
          rootDataEntryStatusToCount(amtDeltaLog).getOrElse(Tracking.Status.Modified, 0L) == 1L,
          "The re-added copy must be a MODIFIED root DATA entry.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("an inline root-resident REPLACE writes a REPLACED root entry, not DELETED") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2, s"Need a tree-shaped bootstrap; got ${leavesBefore.size}.")
        // Append a net-new file; being root-resident, its later remove carries no back reference.
        val rootId = 31
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(rootId))) }

        // Inline REPLACE of the root-resident file: a no-backref remove whose path is re-added this
        // commit becomes a REPLACED root DataEntry (buildRootRemoveEntries), and the re-added copy
        // is MODIFIED -- unlike a pure root delete, which leaves a DELETED tombstone.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesModifiedStatus = 1,
            numRootEntriesReplacedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(removeAndReAddWithDV(amtDeltaLog, Left(rootId))))
        val rootCounts = rootDataEntryStatusToCount(amtDeltaLog)
        assert(rootCounts.getOrElse(Tracking.Status.Replaced, 0L) == 1L,
          s"A re-added root file must leave one REPLACED root entry; got $rootCounts.")
        assert(rootCounts.getOrElse(Tracking.Status.Modified, 0L) == 1L,
          s"The re-added copy must be MODIFIED; got $rootCounts.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("a root DATA entry goes ADDED -> EXISTING -> MODIFIED across incremental writes") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        val rootId = 31

        // Append rootId inline: it is an ADDED root DATA entry.
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(rootId))) }
        assert(rootDataEntryStatusToCount(amtDeltaLog) == Map(Tracking.Status.Added -> 1L),
          s"the appended root file must start ADDED; got " +
            s"${rootDataEntryStatusToCount(amtDeltaLog)}.")

        // Carry rootId forward under an unrelated inline append: rootId decays ADDED -> EXISTING
        // while the filler is the new ADDED entry.
        val fillerId = 32
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesAddedStatus = 1,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(Seq(fakeAdd(fillerId))))
        assert(rootDataEntryStatusToCount(amtDeltaLog) ==
          Map(Tracking.Status.Added -> 1L, Tracking.Status.Existing -> 1L),
          s"rootId must decay to EXISTING while the filler is ADDED; " +
            s"got ${rootDataEntryStatusToCount(amtDeltaLog)}.")

        // Re-add rootId with a new DV: its EXISTING copy becomes a REPLACED root entry and the
        // re-added copy is MODIFIED.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = leavesBefore.size,
            numRootEntriesExistingStatus = 1,
            numRootEntriesModifiedStatus = 1,
            numRootEntriesReplacedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size),
          inlineAMTCommitActions = Some(removeAndReAddWithDV(amtDeltaLog, Left(rootId))))
        val finalCounts = rootDataEntryStatusToCount(amtDeltaLog)
        assert(finalCounts.getOrElse(Tracking.Status.Modified, 0L) == 1L &&
          finalCounts.getOrElse(Tracking.Status.Replaced, 0L) == 1L,
          s"the re-added rootId must be MODIFIED with a REPLACED prior entry; got $finalCounts.")
      }
    }
  }

  test("a freshly spilled leaf can have ADDED/MODIFIED/EXISTING and its manifest_info " +
    "counts partition its entries") {
    // A cap of 10 entries per leaf: 20 files pack into 2 whole leaves, and the 8 live entries this
    // commit produces spill whole into one new leaf.
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Old tree: 5 root-resident DATA entries over 2 leaves (files 1..20 packed into 2 whole
        // leaves, then files 21..25 appended root-resident under the cap of 10).
        val oldLeafLocations = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 20, initialIdRangeInRoot = 21 to 25,
          rowCountPerDataEntry = 20).map(_.location).toSet
        assert(rootDataEntryStatusToCount(amtDeltaLog).values.sum == 5,
          "the old root must hold exactly the 5 root-resident DATA entries.")

        // A deferred (window) log commit appends files 26..28 (10 rows each); not checkpointed, so
        // they enter the next incremental write as intermediate window commits.
        val windowIds = 26 to 28
        commitBoth(baselineDeltaLog, amtDeltaLog,
          windowIds.map(id => fakeAdd(id, dataChange = true, numRecords = 10L)))

        // One inline commit exercising every live-status class:
        //   - REPLACE 3 root files (remove + re-add under a new DV): 3 REPLACED + 3 MODIFIED.
        //   - DELETE 1 root file (remove, no re-add): 1 DELETED.
        //   - REPLACE 2 window files: 2 REPLACED + 2 MODIFIED.
        //   - REPLACE 1 leaf-resident file: masks its old slot in that leaf (which becomes
        //     MODIFIED) and re-adds it live as 1 MODIFIED.
        //   - 1 net-new insert: 1 ADDED.
        // The untouched 5th root file and 3rd window file stay EXISTING. The 9 live entries
        // (1 ADDED + 6 MODIFIED + 2 EXISTING) overflow the cap of 10 and spill whole into one new
        // leaf; the 6 root tombstones (5 REPLACED + 1 DELETED) stay root-resident, while the
        // replaced leaf slot is masked in its now-MODIFIED leaf.
        val replaceRoots =
          Seq(21, 22, 23).flatMap(id => removeAndReAddWithDV(amtDeltaLog, Left(id)))
        val deleteRoot = Seq(removeOf(amtDeltaLog, 24))
        val replaceWindows =
          Seq(26, 27).flatMap(id => removeAndReAddWithDV(amtDeltaLog, Left(id)))
        // File 1 is leaf-resident (one of the 20 files packed into the 2 full-rewrite leaves).
        val replaceLeaf = removeAndReAddWithDV(amtDeltaLog, Left(1))
        val netNewAdd = Seq(fakeAdd(29, dataChange = true, numRecords = 30L))
        val inlineActions =
          replaceRoots ++ deleteRoot ++ replaceWindows ++ replaceLeaf ++ netNewAdd

        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = 1,
            numNewLeaves = 1,
            numRootEntriesReplacedStatus = 5,
            numRootEntriesDeletedStatus = 1,
            numLeafMdvBitsAdded = 1,
            numLeafReplaceCDFBitsAdded = 1,
            numLeavesAddedStatus = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = 1),
          expectedNumIntermediateCommits = Some(2),
          inlineAMTCommitActions = Some(inlineActions))

        // Exactly one freshly spilled leaf, pointer ADDED, holding all 8 live entries.
        val newLeaves = leafPointers(amtDeltaLog.update())
        val spilled = (newLeaves.keySet -- oldLeafLocations).toSeq
        assert(spilled.size == 1, s"exactly one new leaf must spill; got $spilled.")
        val spilledLeaf = newLeaves(spilled.head)
        assert(spilledLeaf.tracking.status == Tracking.Status.Added,
          s"a freshly spilled leaf pointer must be ADDED; got ${spilledLeaf.tracking.status}.")
        assert(spilledLeaf.record_count == 9,
          s"the spilled leaf must hold 9 entries; got ${spilledLeaf.record_count}.")

        // The leaf parquet's raw per-entry statuses: ADDED, MODIFIED, and EXISTING all present.
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        val leafStatusToCount = trackingStatusToAddFileCountMap(
          spilledLeaf.getAbsolutePath(provider.tableRoot).toString)
        assert(leafStatusToCount == Map(
          Tracking.Status.Added -> 1L,
          Tracking.Status.Modified -> 6L,
          Tracking.Status.Existing -> 2L),
          s"leaf entries must be 1 ADDED + 6 MODIFIED + 2 EXISTING; got $leafStatusToCount.")

        // manifest_info counts each status separately: 1 ADDED, 6 MODIFIED, 2 EXISTING, and a live
        // spilled leaf carries no tombstone counts.
        val mi = spilledLeaf.manifest_info
        assert(mi.added_files_count == 1 && mi.modified_files_count == 6 &&
          mi.existing_files_count == 2 &&
          mi.deleted_files_count == 0 && mi.replaced_files_count == 0,
          s"leaf manifest_info must be added=1, modified=6, existing=2, no tombstones; got $mi.")
        assert(
          mi.added_files_count + mi.modified_files_count + mi.existing_files_count ==
            spilledLeaf.record_count.toInt,
          s"added + modified + existing must partition the leaf's ${spilledLeaf.record_count} " +
            s"entries; got $mi.")

        // Row counts sum physical records, so they diverge from the file counts here. ADDED rows =
        // the 30-row net-new file 29; MODIFIED rows = 6 single-row re-adds; EXISTING rows = the
        // 20-row root file 25 + the 10-row window file 28 = 30; no tombstone rows.
        assert(mi.added_rows_count == 30L && mi.modified_rows_count == 6L &&
          mi.existing_rows_count == 30L &&
          mi.deleted_rows_count == 0L && mi.replaced_rows_count == 0L,
          s"leaf row counts must be added=30, modified=6, existing=30, no tombstones; got $mi.")
      }
    }
  }

  test("an inline commit deleting one leaf file and replacing another on the same leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        val (targetLeaf, files) = leafToAddFileMapping.toSeq.sortBy(_._1).find(_._2.size >= 2)
          .getOrElse(fail("need a leaf holding at least two files."))
        val leavesBefore = leafToAddFileMapping.keySet
        val fileToDelete = files.head
        val fileToReplace = files(1)

        // One inline commit: DELETE fileToDelete (no re-add) and REPLACE fileToReplace (remove +
        // re-add with a new DV). Both land on the same leaf, so its pointer carries this commit's
        // deleted_positions AND replaced_positions (MODIFIED), with two masked MDV bits.
        val replaceActions = removeAndReAddWithDV(amtDeltaLog, Right(fileToReplace))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leavesBefore.size - 1,
            numRootEntriesModifiedStatus = 1,
            numLeafMdvBitsAdded = 2,
            numLeafDeleteCDFBitsAdded = 1,
            numLeafReplaceCDFBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leavesBefore.size - 1),
          inlineAMTCommitActions = Some(fileToDelete.remove +: replaceActions))
        val touched = leafPointers(amtDeltaLog.update()).values
          .filter(l => leafDeletedPositions(l).nonEmpty || leafReplacedPositions(l).nonEmpty).toSeq
        assert(touched.size == 1, s"exactly one leaf must be touched; got ${touched.size}.")
        assert(leafDeletedPositions(touched.head).size == 1 &&
          leafReplacedPositions(touched.head).size == 1,
          s"leaf must carry a deleted AND replaced position; got ${touched.head.tracking}.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("an overflowing tombstone leaf is born ADDED, decays to DELETED, then is dropped") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = 31 to 36,
          rowCountPerDataEntry = 10)
        // File 37 arrives as a separate (uncheckpointed) log commit carrying 20 rows.
        commitBoth(baselineDeltaLog, amtDeltaLog,
          Seq(fakeAdd(37, dataChange = true, numRecords = 20L)))
        // Inline: delete every root-resident file (no-backref -> DELETED tombstones) and append
        // enough net-new files that the live adds spill into new leaves; the extra pointers push
        // the tombstones past the cap, so they spill into their own leaf.
        val actions =
          (31 to 37).map(id => removeOf(amtDeltaLog, id)) ++ (130 to 150).map(fakeAdd)
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, actions) }

        // The freshly spilled tombstone leaf (7 DELETED entries, no live file) is born ADDED, not
        // DELETED, and no leaf is DELETED on the commit that writes it.
        val bornLeaves = leafPointers(amtDeltaLog.update())
        val tombstoneLeaves =
          bornLeaves.values.filter(_.manifest_info.deleted_files_count > 0).toSeq
        assert(tombstoneLeaves.size == 1,
          s"exactly one spilled tombstone leaf; got ${tombstoneLeaves.size}.")
        val tombstoneLeaf = tombstoneLeaves.head
        assert(tombstoneLeaf.tracking.status == Tracking.Status.Added,
          s"a freshly spilled tombstone leaf is born ADDED; got ${tombstoneLeaf.tracking.status}.")
        assert(tombstoneLeaf.manifest_info.added_files_count == 0 &&
          tombstoneLeaf.manifest_info.existing_files_count == 0 &&
          tombstoneLeaf.manifest_info.modified_files_count == 0,
          s"the tombstone leaf holds no live entries; got ${tombstoneLeaf.manifest_info}.")
        // The 7 DELETED tombstones carry their files' physical rows: six 10-row root files plus the
        // 20-row window file 37 = 80 deleted rows, versus 7 deleted files, and no live rows.
        val tombstoneMi = tombstoneLeaf.manifest_info
        assert(tombstoneMi.deleted_files_count == 7 && tombstoneMi.deleted_rows_count == 80L &&
          tombstoneMi.replaced_rows_count == 0L,
          s"tombstone leaf must count 7 deleted files and 80 deleted rows; got $tombstoneMi.")
        assert(tombstoneMi.added_rows_count == 0L && tombstoneMi.existing_rows_count == 0L &&
          tombstoneMi.modified_rows_count == 0L,
          s"the tombstone leaf holds no live rows; got $tombstoneMi.")
        assert(!bornLeaves.values.exists(_.tracking.status == Tracking.Status.Deleted),
          "no leaf is DELETED on the commit that spills the tombstone leaf.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)

        // Next AMT: the carried tombstone leaf holds no live file, so it decays to DELETED.
        val tombstoneLoc = tombstoneLeaf.location
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(leafPointers(amtDeltaLog.update())(tombstoneLoc).tracking.status ==
          Tracking.Status.Deleted,
          "a carried leaf with no live file must decay to DELETED.")

        // Next AMT: the DELETED leaf is dropped.
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(!leafPointers(amtDeltaLog.update()).contains(tombstoneLoc),
          "a DELETED leaf must be dropped by the next AMT.")
      }
    }
  }

  test("a spilled tombstone leaf counts a REPLACED + DELETED mix") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = 31 to 36,
          rowCountPerDataEntry = 10)
        // File 37 arrives as a separate (uncheckpointed) log commit carrying 20 rows.
        commitBoth(baselineDeltaLog, amtDeltaLog,
          Seq(fakeAdd(37, dataChange = true, numRecords = 20L)))
        // Inline: DELETE 3 root-resident files (31..33) and REPLACE the other 4 (34..37, remove +
        // re-add with a new DV), plus append net-new files so the tombstones overflow into a
        // spilled leaf.
        val actions =
          Seq(31, 32, 33).map(id => removeOf(amtDeltaLog, id)) ++
            Seq(34, 35, 36, 37).flatMap(id => removeAndReAddWithDV(amtDeltaLog, Left(id))) ++
            (130 to 150).map(fakeAdd)
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, actions) }
        // The 7 tombstones spill into exactly one leaf, born ADDED (not DELETED) with no live
        // entry; its manifest_info counts the 3 DELETED + 4 REPLACED mix.
        val tombstoneLeaves = leafPointers(amtDeltaLog.update()).values.filter(l =>
          l.manifest_info.deleted_files_count + l.manifest_info.replaced_files_count > 0).toSeq
        assert(tombstoneLeaves.size == 1,
          s"the 7 tombstones must spill into exactly one leaf; got ${tombstoneLeaves.size}.")
        val mi = tombstoneLeaves.head.manifest_info
        assert(tombstoneLeaves.head.tracking.status == Tracking.Status.Added,
          s"a freshly spilled tombstone leaf is born ADDED; got ${tombstoneLeaves.head.tracking}.")
        assert(mi.deleted_files_count == 3 && mi.replaced_files_count == 4,
          s"the tombstone leaf must count 3 DELETED + 4 REPLACED entries; got $mi.")
        assert(mi.added_files_count == 0 && mi.existing_files_count == 0 &&
          mi.modified_files_count == 0,
          s"the tombstone leaf holds no live entries; got $mi.")
        // Row counts sum physical records, diverging from the file counts: the 3 DELETED tombstones
        // are 10-row root files (30 rows); the 4 REPLACED tombstones are three 10-row root files
        // plus the 20-row window file 37 (50 rows).
        assert(mi.deleted_rows_count == 30L && mi.replaced_rows_count == 50L,
          s"tombstone row counts must be 30 deleted + 50 replaced; got $mi.")
        assert(mi.added_rows_count == 0L && mi.existing_rows_count == 0L &&
          mi.modified_rows_count == 0L,
          s"the tombstone leaf holds no live rows; got $mi.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("one inline write spills live leaves and a tombstone leaf, all born ADDED") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = 31 to 37)
        val actions =
          (31 to 37).map(id => removeOf(amtDeltaLog, id)) ++ (130 to 150).map(fakeAdd)
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, actions) }
        // The single write spills freshly ADDED live leaves (from the net-new adds) and an ADDED
        // tombstone leaf (from the overflowing DELETED tombstones); every newly written leaf is
        // born ADDED, distinguished only by its manifest_info counts, and none is born DELETED.
        val newLeaves = leafPointers(amtDeltaLog.update()).values
          .filter(_.tracking.status == Tracking.Status.Added).toSeq
        assert(newLeaves.exists(_.manifest_info.added_files_count > 0),
          s"the net-new adds must spill into ADDED live leaves; got $newLeaves.")
        assert(newLeaves.count(_.manifest_info.deleted_files_count > 0) == 1,
          s"the overflowing tombstones must spill into one ADDED tombstone leaf; got $newLeaves.")
        assert(!leafPointers(amtDeltaLog.update()).values.exists(
          _.tracking.status == Tracking.Status.Deleted),
          "no leaf is born DELETED.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("an inline same-key re-add of an already-live file with dataChange=false is allowed") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val livePathsBefore = livePathsInLatestAMTCheckpoint(amtDeltaLog)
        // The allowed counterpart to a rejected re-add, and the happy path of invariant (3):
        // ComputeStats re-commits a currently-live leaf file with recomputed stats and
        // dataChange=false. It stays EXISTING and the file is surfaced exactly once -- one
        // EXISTING root entry, its old leaf slot MDV-masked.
        val liveLeafFile = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        assert(liveLeafFile.backReference.isDefined, "The re-added file must be leaf-resident.")
        withInline {
          amtDeltaLog.startTransaction().commit(
            Seq(liveLeafFile.copy(dataChange = false)),
            DeltaOperations.ComputeStats(predicate = Nil))
        }
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog) == livePathsBefore,
          "Re-adding an already-live file must not change the reconstructed live set.")
        assert(rootDataEntryStatusToCount(amtDeltaLog) == Map(Tracking.Status.Existing -> 1L),
          s"The re-added file must be a single EXISTING root entry; got " +
            s"${rootDataEntryStatusToCount(amtDeltaLog)}.")
      }
    }
  }

  /**
   * A leaf-resident file F (back reference -> its leaf slot) AND a root-resident file R (no back
   * reference) are each re-committed under their SAME (path, dv) key with dataChange=false in TWO
   * commits -- first a deferred window commit, then an inline commit (metadata-only refreshes, e.g.
   * ComputeStats). Both must stay live EXACTLY once as EXISTING root entries (F's old leaf slot
   * MDV-masked), no matter what the inline re-add carries.
   *
   * `dropInlineBackReference` toggles how F's inline re-add is recognized as an already-live key:
   *   - false: it carries F's leaf back reference (recognized via the back reference);
   *   - true:  it carries NO back reference (recognized via the pre-commit live set, since the
   *            window commit already put F's key there -- the branch that would silently regress if
   *            preCommitLiveKeys were dropped).
   * R is root-resident (no leaf slot), so it is always recognized via the pre-commit live set --
   * every variant exercises that path for the root file.
   */
  private def assertWindowThenInlineSameKeyReAddStaysExisting(
      dropInlineBackReference: Boolean): Unit = {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        // Append one net-new file that stays root-resident (no leaf slot -> no back reference).
        val rootId = 31
        withInline { commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(rootId))) }

        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val leafFile = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        assert(leafFile.backReference.isDefined, "F must be leaf-resident (has a back reference).")
        val rootPath = fakeAdd(rootId).path
        val rootFile = liveAddFilesInLatestSnapshot(amtDeltaLog).find(_.path == rootPath)
          .getOrElse(fail(s"root file $rootId is not live."))
        assert(rootFile.backReference.isEmpty, "R must be root-resident (no back reference).")
        val livePathsBefore = livePathsInLatestAMTCheckpoint(amtDeltaLog)

        // Deferred window commit: re-add both F and R (same key, dataChange=false). F carries its
        // leaf back reference; R carries none.
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          Seq(leafFile.copy(dataChange = false), rootFile.copy(dataChange = false)),
          operation = DeltaOperations.ComputeStats(predicate = Nil))

        // Inline commit: re-add both again. The variant decides whether F's inline action carries
        // its back reference; R never has one. F becomes a root EXISTING entry with its old leaf
        // slot masked; R stays a root EXISTING entry -- two EXISTING root entries, one masked bit.
        val inlineLeafReAdd = leafFile.copy(
          dataChange = false,
          backReference = if (dropInlineBackReference) None else leafFile.backReference)
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numRootEntriesExistingStatus = 2,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1),
          inlineAMTCommitActions =
            Some(Seq(inlineLeafReAdd, rootFile.copy(dataChange = false))),
          inlineOperation = DeltaOperations.ComputeStats(predicate = Nil))
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog) == livePathsBefore,
          "Re-adding already-live files across a window + inline commit must not change the " +
            "reconstructed live set.")
        assert(rootDataEntryStatusToCount(amtDeltaLog) == Map(Tracking.Status.Existing -> 2L),
          s"F and R must be two EXISTING root entries; got " +
            s"${rootDataEntryStatusToCount(amtDeltaLog)}.")
      }
    }
  }

  test("a window then inline same-key re-add (dataChange=false) of a leaf and a root file, " +
      "the inline actions carrying back references, keeps both EXISTING") {
    assertWindowThenInlineSameKeyReAddStaysExisting(dropInlineBackReference = false)
  }

  test("a window then inline same-key re-add (dataChange=false) where the inline leaf " +
      "action carries no back reference still keeps both EXISTING (recognized via the live set)") {
    assertWindowThenInlineSameKeyReAddStaysExisting(dropInlineBackReference = true)
  }

  test("a restore round-trip re-adds removed leaf and root files and drops the interim " +
      "add, returning the reconstructed live set to the prior committed state") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Commit-A / tree: the bootstrap adds files into a mixed committed state A that holds both
        // leaf-resident files and root-resident files (it fills the root exactly, so adding more
        // would spill those root entries into leaves -- keep it as the state we restore back to).
        val rootIds = 31 to 37
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = rootIds)
        assert(rootIds.nonEmpty, "need at least one root-resident file.")
        val stateA = livePathsInLatestAMTCheckpoint(amtDeltaLog)

        // Pick a leaf-resident (back-referenced) and a root-resident (no back reference) victim.
        val leafVictim = leafToAddFileMap(amtDeltaLog).toSeq.sortBy(_._1).head._2.head
        assert(leafVictim.backReference.isDefined, "leaf victim must be leaf-resident.")
        val rootPaths = rootIds.map(id => fakeAdd(id).path).toSet
        val rootVictim = liveAddFilesInLatestSnapshot(amtDeltaLog)
          .find(a => a.backReference.isEmpty && rootPaths.contains(a.path))
          .getOrElse(fail("need a live root-resident file to remove."))
        val newId = 330

        // Commit: remove both victims from the tree and add a new file; checkpoint -> state B, so
        // the victims are gone (leaf slot masked, root tombstoned) and the new file is live.
        commitBoth(baselineDeltaLog, amtDeltaLog,
          Seq(leafVictim.remove, rootVictim.remove, fakeAdd(newId)))
        commitCheckpoint(amtDeltaLog, incremental = true)
        val stateB = livePathsInLatestAMTCheckpoint(amtDeltaLog)
        assert(!stateB.contains(leafVictim.path) && !stateB.contains(rootVictim.path),
          "both victims must be gone in state B.")
        assert(stateB.contains(fakeAdd(newId).path), "the new file must be live in state B.")

        // Commit_Inline (restore): re-add both victims as fresh adds -- their tree slots are gone,
        // so they carry no back reference and land as new root entries -- and remove the interim
        // new file, restoring the reconstructed live set to state A.
        withInline {
          commitBoth(baselineDeltaLog, amtDeltaLog, Seq(
            leafVictim.copy(backReference = None, dataChange = true),
            rootVictim.copy(backReference = None, dataChange = true),
            removeOf(amtDeltaLog, newId)))
        }
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog) == stateA,
          s"the restore must return the reconstructed live set to state A; " +
            s"got ${livePathsInLatestAMTCheckpoint(amtDeltaLog).diff(stateA)} extra / " +
            s"${stateA.diff(livePathsInLatestAMTCheckpoint(amtDeltaLog))} missing.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  /**
   * A mixed old root deleted across every file-residence at once, folded into one incremental AMT
   * on both the deferred and the inline route (the gridTest parameter). Steps:
   *   - Full-rewrite a tree of 2 leaves holding 20 [[DataEntry]]s; the root holds no DataEntry.
   *   - Add 8 more files and take a deferred manifest commit: they stay root-resident, so the root
   *     is now MIXED -- 8 live root DataEntries alongside the 2 leaf pointers.
   *   - IC-1: add 10 files.
   *   - IC-2: remove one IC-1 file -- cancels against its own add in replay.
   *   - IC-3: remove across three sources -- an old-root, two leaf and two IC-1 files.
   *   - IC-4: the same three-source remove, plus 10 new adds in the one commit.
   *   - IC-5: add 4 files and remove two more old-root files.
   *   - Final write: remove one file from each source (IC-4, IC-1, old-root, a leaf) plus 4 new
   *     adds, folded either as a deferred manifest commit (empty actionsToCommit) or inline (the
   *     final commit carries the actions -- the only route that writes tombstones).
   *   - Assert the folded tree reconstructs the expected live set and the per-route metrics.
   *
   * Residence decides how a remove resolves: a leaf-resident file is masked by an MDV bit on its
   * carried pointer (no leaf rewrite); a root-resident file is dropped by replay (and only inline
   * leaves a tracking=removed tombstone); an intermediate add's remove cancels against its own add.
   */
  gridTest("a mixed old root plus multi-source deletes across intermediate commits" +
    " folds into one incremental AMT")(Seq(false, true)) { inlineFinalWrite =>
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // ---- Old root: 20 files packed into leaves, then 8 more added root-resident. ----
        // The full rewrite puts all 20 in leaves; the incremental that follows keeps its 8 net-new
        // adds in the root (2 carried pointers + 8 adds == the cap of 10, so nothing spills). That
        // leaves the old root genuinely mixed, which a full checkpoint alone can never produce.
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 20)
        val oldLeaves = leafToAddFileMap(amtDeltaLog)

        val rootIds = 101 to 108
        commitBoth(baselineDeltaLog, amtDeltaLog, rootIds.map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = oldLeaves.size,
            numRootEntriesExistingStatus = rootIds.size,
            numLeavesExistingStatus = oldLeaves.size))
        val (rootLiveAddsBefore, _) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)
        assert(rootLiveAddsBefore == rootIds.size,
          s"The old root must hold ${rootIds.size} live adds of its own; got $rootLiveAddsBefore.")

        // Leaf victims are picked against the ACTUAL hash-based assignment rather than assumed, so
        // the removes carry the back references the writer really stamped. IC-3 and IC-4 take two
        // each and the final write takes one more, so five are needed; taking three per leaf keeps
        // that satisfied without depending on how the rewrite distributed the files.
        val leafVictims = oldLeaves.toSeq.sortBy(_._1).flatMap(_._2.take(3))
        assert(leafVictims.size >= 5, s"Need >= 5 leaf victims; got ${leafVictims.size}.")

        // ---- IC-1: add 10 files. Root-resident (they do not spill until the fold). ----
        val ic1Ids = 201 to 210
        commitBoth(baselineDeltaLog, amtDeltaLog, ic1Ids.map(fakeAdd))

        // ---- IC-2: delete one of IC-1's files -> cancels against its own add in replay. ----
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, ic1Ids.head)))

        // ---- IC-3: delete from THREE sources at once: old root, leaves, IC-1. ----
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          Seq(removeOf(amtDeltaLog, rootIds.head)) ++
            leafVictims.take(2).map(_.remove) ++
            ic1Ids.slice(1, 3).map(removeOf(amtDeltaLog, _)))

        // ---- IC-4: the same three-source delete, plus 10 more adds in the same commit. ----
        val ic4Ids = 301 to 310
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          Seq(removeOf(amtDeltaLog, rootIds(1))) ++
            leafVictims.slice(2, 4).map(_.remove) ++
            ic1Ids.slice(3, 5).map(removeOf(amtDeltaLog, _)) ++
            ic4Ids.map(fakeAdd))

        // ---- IC-5: add 4, and delete 2 more of the old root's files. ----
        val ic5Ids = 401 to 404
        commitBoth(
          baselineDeltaLog, amtDeltaLog,
          ic5Ids.map(fakeAdd) ++ rootIds.slice(2, 4).map(removeOf(amtDeltaLog, _)))

        // ---- The final write: one file from each source removed, plus 4 new adds. ----
        val finalIds = 501 to 504
        val finalActions =
          Seq(
            removeOf(amtDeltaLog, ic4Ids.head),
            removeOf(amtDeltaLog, ic1Ids(5)),
            removeOf(amtDeltaLog, rootIds(4))) ++
            Seq(leafVictims(4).remove) ++
            finalIds.map(fakeAdd)

        // Live net-new adds surviving into the new root, by origin:
        //   old root  8 - 1 (IC-3) - 1 (IC-4) - 2 (IC-5) - 1 (final) = 3
        //   IC-1     10 - 1 (IC-2) - 2 (IC-3) - 2 (IC-4) - 1 (final) = 4
        //   IC-4     10 - 1 (final)                                  = 9
        //   IC-5      4                                              = 4
        //   final     4                                              = 4
        val expectedLiveAdds = 3 + 4 + 9 + 4 + 4
        // Five leaf-resident removes across IC-3, IC-4 and the final write, each contributing one
        // MDV bit. Which leaves those five land on follows the rewrite's hash placement, so derive
        // the updated/untouched split from the victims actually chosen rather than assuming every
        // leaf is hit.
        val expectedMdvBits = 5
        val victimLeaves = leafVictims.take(expectedMdvBits)
          .flatMap(_.backReference.map(_.manifest)).toSet
        val leavesUpdated = victimLeaves.size
        val leavesUntouched = oldLeaves.size - leavesUpdated
        // Only the inline route sources CDF from actionsToCommit, so only it writes tombstones --
        // one per no-backref remove in the final commit (the IC-4, IC-1 and old-root files; the
        // leaf one is masked instead).
        val expectedTombstones = if (inlineFinalWrite) 3 else 0
        // spillIfNeeded moves whole cap-sized batches of live adds out until the root fits:
        //   while (fixedRootCount + spilled + remaining > cap && remaining.nonEmpty)
        // with fixedRootCount = carried pointers + tombstones.
        val fixedRootCount = oldLeaves.size + expectedTombstones
        var spilled = 0
        var remaining = expectedLiveAdds
        while (fixedRootCount + spilled + remaining > 10 && remaining > 0) {
          remaining = math.max(0, remaining - 10)
          spilled += 1
        }
        // Under this branch's enriched metrics the writer also reports the new tree's leaf-pointer
        // status mix (untouched -> EXISTING, MDV-grown -> MODIFIED, freshly spilled -> ADDED) and,
        // on the inline route only, the per-commit deleted CDF bit for the one leaf-resident remove
        // in the final commit (leafVictims(4)); the deferred route sources no CDF from its fold.
        val expectedLeafDeleteCDFBits = if (inlineFinalWrite) 1 else 0
        // Of the live adds that remain in the root after spilling, the deferred fold proposes no
        // insert (empty actionsToCommit), so every remaining root live add is EXISTING; the inline
        // route spills them all (remaining == 0).
        val rootExistingLiveAdds = if (inlineFinalWrite) 0 else remaining
        val expectedMetrics = createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = leavesUpdated,
          numOldLeavesUntouched = leavesUntouched,
          numNewLeaves = spilled,
          numRootEntriesAddedStatus = remaining - rootExistingLiveAdds,
          numRootEntriesExistingStatus = rootExistingLiveAdds,
          numRootEntriesDeletedStatus = expectedTombstones,
          numLeafMdvBitsAdded = expectedMdvBits,
          numLeafDeleteCDFBitsAdded = expectedLeafDeleteCDFBits,
          numLeavesAddedStatus = spilled,
          numLeavesExistingStatus = leavesUntouched,
          numLeavesModifiedStatus = leavesUpdated)

        if (inlineFinalWrite) {
          // The inline route commits the actions and the tree in ONE commit, so the metrics come
          // off that commit rather than a follow-up OPTIMIZE CHECKPOINT.
          createIncrementalAMTAndValidate(
            baselineDeltaLog, amtDeltaLog, expectedMetrics,
            inlineAMTCommitActions = Some(finalActions))
        } else {
          // The deferred route commits the actions as one more intermediate commit, then folds
          // everything in with an empty actionsToCommit.
          commitBoth(baselineDeltaLog, amtDeltaLog, finalActions)
          createIncrementalAMTAndValidate(
            baselineDeltaLog, amtDeltaLog, expectedMetrics,
            // IC-1..IC-5, the final write, and the bootstrap incremental's own commit.
            expectedNumIntermediateCommits = Some(7))
        }

        // The differential oracle above already pins the live set; state the total explicitly too,
        // since the whole point of the scenario is that 39 files survive this churn.
        val expectedLiveTotal = expectedLiveAdds + (20 - expectedMdvBits)
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).size == expectedLiveTotal,
          s"The tree must reconstruct exactly $expectedLiveTotal live files; got " +
            s"${livePathsInLatestAMTCheckpoint(amtDeltaLog).size}.")
      }
    }
  }
}
