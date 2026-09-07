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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

/** Root-vs-leaf spill decisions and leaf packing on the incremental AMT write path. */
class AMTIncrementalWriteSpillSuite extends AMTIncrementalWriteTestBase {

  test("deferred append below the cap stays root-resident, no spill, leaf untouched") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
        // One net-new add stays root-resident: the root holds 3 leaf pointers plus this add = 4,
        // which is under the cap of 8, so spillIfNeeded does not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numRootEntriesExistingStatus = 1,
            numLeavesExistingStatus = 3))
      }
    }
  }

  test("deferred append exactly filling the root to the cap does not spill") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
        // Fill the root exactly to the cap: 3 leaf pointers + 5 adds = 8 == cap. spillIfNeeded
        // loops while the total is `> cap`, so filling it exactly must not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 29).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numRootEntriesExistingStatus = 5,
            numLeavesExistingStatus = 3))
      }
    }
  }

  test("deferred append over the cap spills whole cap-sized batches") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
        // 12 net-new adds on a root already holding 3 leaf pointers, cap 8. spillIfNeeded trace:
        //   3(fixed) + 0(spilled) + 12(remaining) = 15 > 8 -> spill a batch of 8 (remaining = 4)
        //   3(fixed) + 1(spilled) +  4(remaining) =  8 == 8 -> stop.
        // => 1 new leaf, and the 4 leftover adds stay root-resident.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 36).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numNewLeaves = 1,
            numRootEntriesExistingStatus = 4,
            numLeavesExistingStatus = 3,
            numLeavesAddedStatus = 1))
      }
    }
  }

  test("deferred append with entriesPerLeaf=1 spills every net-new add into its own leaf") {
    withTables() { (baselineDeltaLog, amtDeltaLog) =>
      // Bootstrap at a cap that clusters into several leaves (a single manifest would be promoted
      // into the root), then drop the cap to one so every net-new add spills into its own leaf.
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
      }
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
        // At a cap of one, each of the 3 adds spills into its own leaf and none stays in the root.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25), fakeAdd(26), fakeAdd(27)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numNewLeaves = 3,
            numRootEntriesAddedStatus = 0,
            numLeavesExistingStatus = 3,
            numLeavesAddedStatus = 3))
      }
    }
  }

  test("a large deferred append spills multiple cap-sized leaves, each within the cap") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val oldExistingLeaves =
          setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
            .map(_.location).toSet
        // 30 net-new adds on a root already holding 3 leaf pointers, cap 8. spillIfNeeded trace:
        //   3 + 0 + 30 = 33 > 8 -> spill 8 (remaining 22)
        //   3 + 1 + 22 = 26 > 8 -> spill 8 (remaining 14)
        //   3 + 2 + 14 = 19 > 8 -> spill 8 (remaining  6)
        //   3 + 3 +  6 = 12 > 8 -> spill 6 (remaining  0)
        // => 4 new leaves, 0 root-resident adds.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 54).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numNewLeaves = 4,
            numRootEntriesAddedStatus = 0,
            numLeavesExistingStatus = 3,
            numLeavesAddedStatus = 4))
        // Every leaf this write SPILLED holds at most `cap` physical DATA entries, because
        // spillIfNeeded moves whole cap-sized batches. The bootstrap's own leaves are excluded: a
        // clustered full rewrite derives a leaf count from the cap but does not bound each leaf, so
        // an uneven hash distribution can leave one holding more.
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        val spilledLeaves = provider.leaves.filterNot(l => oldExistingLeaves.contains(l.location))
        assert(spilledLeaves.size == 4,
          s"Expected 4 spilled leaves; found ${spilledLeaves.size}.")
        spilledLeaves.foreach { leaf =>
          val entries =
            trackingStatusToAddFileCountMap(leaf.getAbsolutePath(provider.tableRoot).toString)
            .values.sum
          assert(entries <= 8,
            s"Spilled leaf ${leaf.location} holds $entries entries, over the cap of 8.")
        }
      }
    }
  }

  test("spill accounting includes carried leaf pointers in fixedRootCount") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "2") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Seed enough files that the full rewrite writes MORE THAN ONE leaf; the exact count is
        // non-deterministic (the clustered rewrite hashes files across partitions), so read it back
        // rather than assume it. spillIfNeeded must then base its overflow on the carried count
        // plus the new adds.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 6).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val numLeafCountBefore =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(numLeafCountBefore >= 2,
          s"Need >= 2 carried leaves to exercise fixedRootCount; got $numLeafCountBefore.")
        // Append one file. All carried pointers stay untouched. The carried leaf pointers already
        // fill the root to the cap (fixedRootCount counts them, and numLeafCountBefore >= cap = 2),
        // so the appended file has no root capacity and must spill into exactly one new leaf --
        // which is the accounting this test guards.
        val oldAMTLeafToMDV = leafToLeafMDVCardinalityMap(amtDeltaLog)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(7)))
        val actualIncrementalMetrics = commitIncrementalCheckpointAndReturnMetrics(amtDeltaLog)
          .getOrElse(fail("An incremental checkpoint must log metrics."))
        assert(actualIncrementalMetrics.numOldLeavesUntouched == numLeafCountBefore,
          s"All $numLeafCountBefore carried leaves must be untouched by an append; got " +
            s"${actualIncrementalMetrics.numOldLeavesUntouched}.")
        assert(actualIncrementalMetrics.numNewLeaves == 1,
          s"The appended file must spill into one new leaf; got " +
            s"${actualIncrementalMetrics.numNewLeaves}.")
        assert(actualIncrementalMetrics.numRootEntriesAddedStatus == 0 &&
          actualIncrementalMetrics.numRootEntriesExistingStatus == 0 &&
          actualIncrementalMetrics.numRootEntriesModifiedStatus == 0,
          "The appended file must not land in the root (the carried pointers fill it).")
        // The old tree is a fresh full rewrite, so it carries no DELETED tombstones to drop.
        assertMetricsMatchTreeDelta(amtDeltaLog, oldAMTLeafToMDV, oldDeletedLeafCount = 0,
          metrics = actualIncrementalMetrics)
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("consecutive intermediate insert commits accumulate their adds in the root") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 8, initialIdRangeInLeaf = 1 to 24)
        // Several separate INSERT commits, none checkpointed, each adding one net-new file: the
        // adding analog of accumulating deferred leaf removes. The deferred incremental must fold
        // every one into the root -- 3 leaf pointers + 4 adds = 7 <= cap 8, so all four stay
        // root-resident and nothing spills.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(26)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(27)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(28)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numRootEntriesExistingStatus = 4,
            numLeavesExistingStatus = 3),
          expectedNumIntermediateCommits = Some(5))
      }
    }
  }

  test("added actions fit in the root of a leafless AMT and are not spilled") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // No leaf pointers, so fixedRootCount is 0. The replay carries the promoted root's own file
        // forward as a root-resident add, so the new root holds that one plus the 3 appended = 4,
        // well under the cap of 8: nothing spills and every live add stays root-resident.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 4).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numRootEntriesExistingStatus = 4))
      }
    }
  }

  test("added actions exactly fill the root of a leafless AMT") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // fixedRootCount is 0 and the replay carries the bootstrap's own file forward as a
        // root-resident add, so 8 live adds fill the root exactly to the cap. spillIfNeeded loops
        // while the total is `> cap`, so an exactly-full root must not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 8).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numRootEntriesExistingStatus = 8))
      }
    }
  }

  test("added actions overflow the root of a leafless AMT and spill into one leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // 9 live adds (the bootstrap's file + 8 new) on a root with no pointers, cap 8:
        //   0(fixed) + 0(spilled) + 9(remaining) = 9 > 8 -> spill a batch of 8 (remaining = 1)
        //   0(fixed) + 1(spilled) + 1(remaining) = 2 <= 8 -> stop.
        // => 1 new leaf, 1 root-resident add.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 9).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numNewLeaves = 1,
            numRootEntriesExistingStatus = 1,
            numLeavesAddedStatus = 1))
      }
    }
  }

  test("added actions overflow the root of a leafless AMT and spill into three leaves") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // 24 live adds (the bootstrap's file + 23 new) on a root with no pointers, cap 8:
        //   0 + 0 + 24 = 24 > 8 -> spill 8 (remaining 16)
        //   0 + 1 + 16 = 17 > 8 -> spill 8 (remaining  8)
        //   0 + 2 +  8 = 10 > 8 -> spill 8 (remaining  0)
        // => 3 new leaves, 0 root-resident adds.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 24).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numNewLeaves = 3,
            numRootEntriesAddedStatus = 0,
            numLeavesAddedStatus = 3))
      }
    }
  }
}
