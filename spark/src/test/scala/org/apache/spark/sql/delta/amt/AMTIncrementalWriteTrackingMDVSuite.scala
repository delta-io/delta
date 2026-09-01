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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Leaf lifecycle: carry-forward, MDV masking of removed files, and the leaf-pointer
 * tracking.status transition chains across a sequence of incremental rewrites.
 */
class AMTIncrementalWriteTrackingMDVSuite extends AMTIncrementalWriteTestBase {

  test("deleting one leaf-resident file sets a single MDV bit on its leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves; a bootstrap that produced one manifest
        // would be promoted into the root, leaving no leaf to mask.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount = leafToAddFileMap(amtDeltaLog).size
        assert(leafCount >= 2, s"Need a tree-shaped bootstrap; got $leafCount leaves.")
        // Delete one file, so exactly its own leaf is updated and every sibling stays untouched.
        val victim = leafToAddFileMap(amtDeltaLog).toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafCount - 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafCount - 1))
      }
    }
  }

  test("MDV masking applies independently across two distinct leaves") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 -> ceil(15/5)=3 leaves, all non-empty (the full-rewrite fills them).
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need >= 2 leaves; got ${leafToAddFileMapping.size}.")
        // Pick one file from each of two distinct leaves, so exactly two leaves are updated.
        val twoLeaves = leafToAddFileMapping.toSeq.sortBy(_._1).take(2)
        val victims = twoLeaves.map { case (_, files) => files.head }
        val untouchedLeaves = leafToAddFileMapping.size - 2
        commitBoth(baselineDeltaLog, amtDeltaLog, victims.map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 2,
            numOldLeavesUntouched = untouchedLeaves,
            numLeafMdvBitsAdded = 2,
            numLeavesModifiedStatus = 2,
            numLeavesExistingStatus = untouchedLeaves))
      }
    }
  }

  test("deleting two files from the SAME leaf adds two bits to one leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        // A leaf holding >= 2 files; delete two of them -> one leaf updated, two bits.
        val (_, files) = leafToAddFileMapping.toSeq.sortBy(_._1).find(_._2.size >= 2)
          .getOrElse(fail("Expected some leaf to hold >= 2 files at cap 5 with 15 files."))
        val untouchedLeaves = leafToAddFileMapping.size - 1
        commitBoth(baselineDeltaLog, amtDeltaLog, files.take(2).map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = untouchedLeaves,
            numLeafMdvBitsAdded = 2,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = untouchedLeaves))
      }
    }
  }

  test("multiple deferred intermediate commits accumulate all their leaf removes in the MDV") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves rather than one promoted root manifest.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // One victim from each of two distinct leaves, so the two intermediate commits land their
        // bits on different carried pointers.
        val twoLeaves = leafToAddFileMapping.toSeq.sortBy(_._1).take(2)
        val victimFromLeaf1 = twoLeaves.head._2.head
        val victimFromLeaf2 = twoLeaves(1)._2.head
        // Two separate intermediate commits each remove one leaf file; the deferred incremental
        // must accumulate both bits though neither is this commit's own action.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFromLeaf1.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFromLeaf2.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 2,
            numOldLeavesUntouched = leafToAddFileMapping.size - 2,
            numLeafMdvBitsAdded = 2,
            numLeavesModifiedStatus = 2,
            numLeavesExistingStatus = leafToAddFileMapping.size - 2))
      }
    }
  }

  test("consecutive incremental AMT deletes accumulate MDV bits across writes") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves rather than one promoted root manifest.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Both victims share one leaf, so each write updates that same carried pointer.
        val victims = leafToAddFileMapping.toSeq.sortBy(_._1).find(_._2.size >= 2)
          .getOrElse(fail("Expected some leaf to hold >= 2 files."))._2.take(2)
        val untouched = leafToAddFileMapping.size - 1
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victims.head.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = untouched,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = untouched))
        // The second write adds only its own bit; the leaf's cumulative MDV covers both, checked
        // by the live-set baselineDeltaLog.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victims(1).remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = untouched,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = untouched))
      }
    }
  }

  test("incremental AMT writes handles paths with spaces correctly") {
    withTempDir { baseDir =>
      // A leaf pointer's `location` is relativized against the table root as a URI, so a space in
      // the root becomes %20 there. The MDV update matches a remove's stamped
      // `backReference.manifest` to that `location` by string equality, and a mismatch would
      // silently no-op the MDV -- leaving the removed file live, which the live-set oracle inside
      // createIncrementalAMTAndValidate catches.
      val tableRoot = new java.io.File(baseDir, "amt tbl")
      withTables(amtTableLocation = Some(tableRoot.toString)) {
          (baselineDeltaLog, amtDeltaLog) =>
        withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
          // 15 files at cap 5 clusters into several leaves rather than one promoted root manifest.
          commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
          commitCheckpoint(amtDeltaLog, incremental = false)
          val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
          assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
          // Every leaf key must be table-root-relative, never the absolute path that would make the
          // string match below succeed only by coincidence.
          leafToAddFileMapping.keys.foreach { leaf =>
            assert(leaf.startsWith("metadata/") && !leaf.contains(baseDir.toString),
              s"Leaf key must be table-root-relative; got $leaf.")
          }
          val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
          commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
          createIncrementalAMTAndValidate(
            baselineDeltaLog,
            amtDeltaLog,
            createIncrementalAMTWriteMetrics(
              numOldLeavesUpdated = 1,
              numOldLeavesUntouched = leafToAddFileMapping.size - 1,
              numLeafMdvBitsAdded = 1,
              numLeavesModifiedStatus = 1,
              numLeavesExistingStatus = leafToAddFileMapping.size - 1))
        }
      }
    }
  }

  test("an incremental AMT carries the leaf parquet forward byte-for-byte") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves; a single manifest would be promoted into
        // the root, leaving no leaf parquet to carry forward.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        assert(provider.leaves.size >= 2,
          s"Need a tree-shaped bootstrap; got ${provider.leaves.size} leaves.")
        // Fingerprint every leaf parquet on disk (path, length, modification time) pre-write.
        val fsRoot = provider.tableRoot.getFileSystem(amtDeltaLog.newDeltaHadoopConf())
        def leafFingerprints(p: AMTCheckpointProvider): Set[(String, Long, Long)] =
          p.leaves.map { leaf =>
            val st = fsRoot.getFileStatus(leaf.getAbsolutePath(p.tableRoot))
            (leaf.location, st.getLen, st.getModificationTime)
          }.toSet
        val before = leafFingerprints(provider)
        // A delete of a leaf-resident file: the pointer's MDV changes, but the leaf FILE must not.
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        val victim = leafToAddFileMapping.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
        val after =
          leafFingerprints(amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")))
        assert(after == before,
          s"Incremental must carry leaves forward untouched.\n  before=$before\n  after=$after")
      }
    }
  }

  test("an untouched sibling leaf keeps an empty MDV while another is masked") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd)) // 3 leaves at cap 5.
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need >= 2 leaves; got ${leafToAddFileMapping.size}.")
        // Delete one file from exactly one leaf; the other leaves must keep an empty MDV.
        val (victimLeaf, victimFiles) = leafToAddFileMapping.toSeq.minBy(_._1)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFiles.head.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = leafToAddFileMapping.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = leafToAddFileMapping.size - 1))
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        provider.leaves.foreach { leaf =>
          val card = leaf.manifest_info.dv_cardinality.getOrElse(0L)
          if (leaf.location == victimLeaf) {
            assert(card == 1L, s"The victim leaf must carry exactly one MDV bit; got $card.")
          } else {
            assert(card == 0L, s"Sibling leaf ${leaf.location} must keep an empty MDV; got $card.")
          }
        }
      }
    }
  }

  test("a stale DELETED leaf is dropped by the next incremental rewrite") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // First reach the fully-masked state: every leaf carried as a DELETED tombstone.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount = leafToAddFileMap(amtDeltaLog).size
        assert(leafCount >= 2, s"Need a tree-shaped bootstrap; got $leafCount leaves.")
        commitBoth(baselineDeltaLog, amtDeltaLog,
          leafToAddFileMap(amtDeltaLog).values.flatten.toSeq.map(_.remove))
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
          .leaves.count(_.tracking.status == Tracking.Status.Deleted) == leafCount,
          "precondition: the first incremental rewrite must leave DELETED tombstones.")

        // A second bare incremental rewrite carries nothing new, so it must drop the stale DELETED
        // pointers and report them as numStaleDeletedLeavesDropped, leaving an empty tree.
        val metrics = commitIncrementalCheckpointAndReturnMetrics(amtDeltaLog)
          .getOrElse(fail("An incremental checkpoint must log metrics."))
        assert(metrics.numStaleDeletedLeavesDropped == leafCount,
          s"All $leafCount stale DELETED leaves must be dropped; got " +
            s"${metrics.numStaleDeletedLeavesDropped}.")
        assert(amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.isEmpty,
          "The stale DELETED pointers must be gone from the new tree.")
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).isEmpty,
          "The AMT tree must reconstruct an empty live set.")
      }
    }
  }

  test("deleting every leaf-resident file masks every leaf fully, marking each DELETED") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // The extreme end of per-leaf MDV masking: mask EVERY position on EVERY leaf, not one.
        // Each leaf's cumulative MDV then covers all its entries, so its pointer is carried as a
        // DELETED tombstone (the reader skips it) and the tree reconstructs an empty live set.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val allLeafFiles = leafToAddFileMapping.values.flatten.toSeq
        commitBoth(baselineDeltaLog, amtDeltaLog, allLeafFiles.map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = leafToAddFileMapping.size,
            numLeafMdvBitsAdded = allLeafFiles.size,
            numLeavesDeletedStatus = leafToAddFileMapping.size))
        // Every carried leaf pointer is now a DELETED tombstone.
        val statuses = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
          .leaves.map(_.tracking.status)
        assert(statuses.forall(_ == Tracking.Status.Deleted) &&
          statuses.size == leafToAddFileMapping.size,
          s"Every fully-masked leaf must be DELETED; got $statuses.")
        assert(livePathsInLatestSnapshot(baselineDeltaLog).isEmpty,
          "The baseline table must have no live files.")
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).isEmpty,
          "The AMT tree must reconstruct an empty live set.")

        // The next incremental rewrite drops every DELETED tombstone, leaving a fully empty tree:
        // no leaf pointers, and a root that holds no DATA entries.
        commitCheckpoint(amtDeltaLog, incremental = true)
        val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
        assert(provider.leaves.isEmpty,
          s"The next rewrite must drop all DELETED leaves; got ${provider.leaves.size}.")
        assert(liveAddsAndTombstonesCountInRoot(amtDeltaLog) == (0L, 0L),
          "The new root must hold no DATA entries (no live adds, no tombstones).")
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).isEmpty,
          "The fully empty tree must reconstruct an empty live set.")
      }
    }
  }

  /** Human-readable [[Tracking.Status]] name, used in transition-failure messages. */
  private def statusName(status: Int): String = status match {
    case Tracking.Status.Added => "ADDED"
    case Tracking.Status.Existing => "EXISTING"
    case Tracking.Status.Modified => "MODIFIED"
    case Tracking.Status.Deleted => "DELETED"
    case Tracking.Status.Replaced => "REPLACED"
    case other => s"status($other)"
  }

  /** The leaf pointer's tracking.status at `location`, or None if it is no longer listed. */
  private def leafStatusAt(amtDeltaLog: DeltaLog, location: String): Option[Int] =
    amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
      .leaves.find(_.location == location).map(_.tracking.status)

  /** The leaf at `location` must currently carry `expected`; `step` labels the transition edge. */
  private def assertLeafStatus(
      amtDeltaLog: DeltaLog, location: String, expected: Int, step: String): Unit = {
    val actual = leafStatusAt(amtDeltaLog, location)
    assert(actual.contains(expected),
      s"$step: leaf $location must be ${statusName(expected)}; " +
        s"got ${actual.map(statusName).getOrElse("<dropped>")}.")
  }

  /** The leaf's still-live files (read via back reference), sorted by path. */
  private def liveLeafFiles(amtDeltaLog: DeltaLog, location: String): Seq[AddFile] =
    leafToAddFileMap(amtDeltaLog).getOrElse(location, Seq.empty).sortBy(_.path)

  /**
   * Bootstraps a multi-leaf full AMT ([[leafPackedFiles]] files) and returns the location of its
   * largest leaf, asserting that leaf starts ADDED. The largest leaf packs at least
   * `ceil(leafPackedFiles / numLeaves)` files -- headroom to mask across several commits before it
   * empties. Every transition chain below begins from this freshly written ADDED leaf.
   */
  private def bootstrapTargetLeaf(baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog): String = {
    commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
    commitCheckpoint(amtDeltaLog, incremental = false)
    val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
    assert(leafToAddFileMapping.size >= 2,
      s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
    val target =
      leafToAddFileMapping.toSeq.sortBy { case (loc, files) => (-files.size, loc) }.head._1
    assertLeafStatus(amtDeltaLog, target, Tracking.Status.Added, "bootstrap")
    target
  }

  /** Removes `files` from both tables, then lands one incremental AMT checkpoint. */
  private def removeFilesAndCheckpoint(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog, files: Seq[AddFile]): Unit = {
    commitBoth(baselineDeltaLog, amtDeltaLog, files.map(_.remove))
    commitCheckpoint(amtDeltaLog, incremental = true)
  }

  test("ADDED -> EXISTING (a freshly written leaf carried untouched)") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        // A bare incremental rewrite carries the leaf forward with no new masking.
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> DELETED -> removed (fully masked, then dropped)") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        // Mask every entry of the leaf: its cumulative MDV covers the whole leaf -> DELETED.
        removeFilesAndCheckpoint(baselineDeltaLog, amtDeltaLog, liveLeafFiles(amtDeltaLog, leaf))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Deleted, "ADDED -> DELETED")
        // The next rewrite drops the stale DELETED tombstone.
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(leafStatusAt(amtDeltaLog, leaf).isEmpty,
          s"DELETED -> removed: leaf $leaf must be dropped by the next rewrite.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> MODIFIED (a freshly written leaf partially masked)") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        // Mask one entry: some live, some masked -> MODIFIED.
        removeFilesAndCheckpoint(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFiles(amtDeltaLog, leaf).head))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Modified, "ADDED -> MODIFIED")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> EXISTING -> EXISTING (carried untouched twice)") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "EXISTING -> EXISTING")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> EXISTING -> DELETED -> removed") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        // Mask every entry of the carried leaf -> DELETED.
        removeFilesAndCheckpoint(baselineDeltaLog, amtDeltaLog, liveLeafFiles(amtDeltaLog, leaf))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Deleted, "EXISTING -> DELETED")
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(leafStatusAt(amtDeltaLog, leaf).isEmpty,
          s"DELETED -> removed: leaf $leaf must be dropped by the next rewrite.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> EXISTING -> MODIFIED -> EXISTING") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        removeFilesAndCheckpoint(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFiles(amtDeltaLog, leaf).head))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Modified, "EXISTING -> MODIFIED")
        // A carried leaf whose MDV does not grow this commit falls back to EXISTING.
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "MODIFIED -> EXISTING")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> EXISTING -> MODIFIED -> MODIFIED") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        removeFilesAndCheckpoint(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFiles(amtDeltaLog, leaf).head))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Modified, "EXISTING -> MODIFIED")
        // Mask one more still-live entry: the MDV grows again but some entries remain live.
        removeFilesAndCheckpoint(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFiles(amtDeltaLog, leaf).head))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Modified, "MODIFIED -> MODIFIED")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("ADDED -> EXISTING -> MODIFIED -> DELETED") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leaf = bootstrapTargetLeaf(baselineDeltaLog, amtDeltaLog)
        commitCheckpoint(amtDeltaLog, incremental = true)
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Existing, "ADDED -> EXISTING")
        removeFilesAndCheckpoint(
          baselineDeltaLog, amtDeltaLog, Seq(liveLeafFiles(amtDeltaLog, leaf).head))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Modified, "EXISTING -> MODIFIED")
        // Mask every remaining live entry: the cumulative MDV now covers the whole leaf -> DELETED.
        removeFilesAndCheckpoint(baselineDeltaLog, amtDeltaLog, liveLeafFiles(amtDeltaLog, leaf))
        assertLeafStatus(amtDeltaLog, leaf, Tracking.Status.Deleted, "MODIFIED -> DELETED")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }
}
