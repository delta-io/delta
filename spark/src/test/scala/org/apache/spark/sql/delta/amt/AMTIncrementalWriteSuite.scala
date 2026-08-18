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

import java.util.UUID

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

/** Tests for AMT incremental write path */
class AMTIncrementalWriteSuite extends AMTCheckpointTestBase {

  import testImplicits._

  /////////////////////////////////////////////////////////////
  // Test Helpers:                                           //
  //     The two tables, the actions committed to them, and  //
  //      the validators every section below asserts through //
  /////////////////////////////////////////////////////////////

  /** A deterministic fake data file. Paths are unique per id so live sets are easy to reason on. */
  private def fakeAdd(fileID: Int): AddFile =
    AddFile(
      path = f"part-$fileID%05d.parquet",
      partitionValues = Map.empty,
      size = 100L + fileID,
      modificationTime = 1000L + fileID,
      dataChange = true,
      stats = s"""{"numRecords":1}""")

  /** Creates the non-AMT never-checkpointed baseline table and the AMT-backed subject table. */
  private def createTables(
      baselineNonAMTDeltaTable: String,
      amtDeltaTable: String,
      amtTableLocation: Option[String]): (DeltaLog, DeltaLog) = {
    // Baseline: interval huge so it is never checkpointed; its allFiles is pure log replay.
    sql(
      s"""CREATE TABLE $baselineNonAMTDeltaTable (id INT) USING DELTA
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'id',
         |  'delta.enableDeletionVectors' = 'true',
         |  'delta.checkpointInterval' = '1000000')""".stripMargin)
    // Subject: AMT-backed; we drive its checkpoints explicitly, so interval is huge too.
    createAMTTable(amtDeltaTable, checkpointInterval = 1000000, location = amtTableLocation)
    (deltaLogForName(baselineNonAMTDeltaTable), deltaLogForName(amtDeltaTable))
  }

  /** The operation every write in this suite is committed under. */
  private def writeOperation: DeltaOperations.Operation =
    DeltaOperations.Write(SaveMode.Append)

  /** Commits the identical `actions` as a "WRITE" commit to both tables. */
  private def commitBoth(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog, actions: Seq[Action]): Unit = {
    val baselineActions = actions.map {
      case a: AddFile => a.copy(backReference = None)
      case r: RemoveFile => r.copy(backReference = None)
      case other => other
    }
    baselineDeltaLog.startTransaction().commit(baselineActions, writeOperation)
    amtDeltaLog.startTransaction().commit(actions, writeOperation)
  }

  /** The live-file path set of a table via its normal snapshot (log replay for the baseline). */
  private def livePathsInLatestSnapshot(deltaLog: DeltaLog): Set[String] =
    liveAddFilesInLatestSnapshot(deltaLog).map(_.path).toSet

  /**
   * The live-file path set the AMT provider reconstructs from its manifest tree (root + leaves,
   * MDV / tracking.status honored).
   */
  private def livePathsInLatestAMTCheckpoint(deltaLog: DeltaLog): Set[String] = {
    val provider = amtProvider(deltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    provider.loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null").select("add.path").as[String].collect().toSet
  }

  /** DATA-entry tracking statuses that reconstruct as live files (added/existing). */
  private val liveDataEntryStatuses = Set(Tracking.Status.Existing, Tracking.Status.Added)

  /** DATA-entry tracking statuses that mark a root-resident entry as a CDF tombstone. */
  private val tombstoneTrackingStatuses = Set(Tracking.Status.Deleted, Tracking.Status.Replaced)

  /**
   * Physical DATA-row counts of a manifest parquet keyed by `tracking.status`.
   */
  private def trackingStatusToAddFileCountMap(absManifestPath: String): Map[Int, Long] =
    allowReadWithinDeltaLog {
      spark.read.parquet(absManifestPath)
        .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
        .groupBy(col("tracking.status").as("status")).count()
        .as[(Int, Long)].collect().toMap
    }

  /** The live AddFiles of a table's latest snapshot, for building real removes. */
  private def liveAddFilesInLatestSnapshot(deltaLog: DeltaLog): Seq[AddFile] =
    deltaLog.update().allFiles.collect().toSeq

  /**
   * The leaf-resident live files of the AMT table grouped by their leaf's relative location, read
   * from the reconstructed AddFiles' back references. Root-resident live files (no back reference)
   * are excluded. Lets a scenario pick removes against the *actual* leaf assignment.
   */
  private def leafToAddFileMap(amtDeltaLog: DeltaLog): Map[String, Seq[AddFile]] =
    liveAddFilesInLatestSnapshot(amtDeltaLog)
      .flatMap(add => add.backReference.map(br => br.manifest -> add))
      .groupBy(_._1).map { case (leaf, pairs) => leaf -> pairs.map(_._2) }

  /** Asserts the table's current AMT checkpoint describes exactly `expectedVersion`. */
  private def assertCheckpointDescribesVersion(
      amtDeltaLog: DeltaLog, expectedVersion: Long): Unit = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    assert(provider.checkpointAction.version == expectedVersion,
      s"Checkpoint must describe version $expectedVersion; " +
        s"got ${provider.checkpointAction.version}.")
  }

  /** A real remove for the live file with the given id, carrying its stamped back reference. */
  private def removeOf(amtDeltaLog: DeltaLog, fileID: Int): RemoveFile = {
    val path = fakeAdd(fileID).path
    val add = liveAddFilesInLatestSnapshot(amtDeltaLog).find(_.path == path)
      .getOrElse(fail(s"fileID=$fileID ($path) is not live in the AMT table."))
    add.remove
  }

  /** The current tree's leaf pointers keyed by relative `location`, with their MDV cardinality. */
  private def leafToLeafMDVCardinalityMap(amtDeltaLog: DeltaLog): Map[String, Long] = {
    val provider = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    provider.leaves.map(l => l.location -> l.manifest_info.dv_cardinality.getOrElse(0L)).toMap
  }

  /** Root-resident DATA-entry counts (live adds, tombstones) read straight off the new root. */
  private def liveAddsAndTombstonesCountInRoot(amtDeltaLog: DeltaLog): (Long, Long) = {
    val provider = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    val byStatus =
      trackingStatusToAddFileCountMap(
        provider.checkpointAction.contentRoot.getAbsolutePath(provider.tableRoot).toString)
    val liveAdds =
      byStatus.filter { case (status, _) => liveDataEntryStatuses.contains(status) }.values.sum
    val tombstones = tombstoneTrackingStatuses.toSeq.map(byStatus.getOrElse(_, 0L)).sum
    (liveAdds, tombstones)
  }

  /** The current snapshot's leaf pointers, keyed by relative location. */
  private def leafPointers(snapshot: Snapshot): Map[String, DataManifestEntry] = {
    val provider = amtProvider(snapshot).getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
    provider.leaves.map(l => l.location -> l).toMap
  }

  /** The MDV cardinality numLeafCountBefore on a leaf pointer, 0 when it has none. */
  private def mdvCardinality(leaf: DataManifestEntry): Long =
    leaf.manifest_info.dv_cardinality.getOrElse(0L)

  /**
   * Emits a deferred incremental checkpoint and cross-checks its metrics three ways: the
   * hand-specified `expectedAMTWriteMetrics` (author intent), the metrics the writer actually
   * reported, and a structural derivation from the old tree vs. the new tree read straight off
   * disk.
   *
   * Also compares the live files on baselineDeltaLog and amtDeltaLog.
   */
  private def createIncrementalAMTAndValidate(
      baselineDeltaLog: DeltaLog,
      amtDeltaLog: DeltaLog,
      expectedAMTWriteMetrics: IncrementalAMTWriteMetrics,
      expectedNumIntermediateCommits: Option[Int] = None): Unit = {
    // Snapshot the OLD tree's leaf pointers before the write: their MDV cardinality (for the shape
    // derivation), the full pointers (to assert manifest_info counts stay immutable), and the count
    // of DELETED tombstone pointers this rewrite is expected to drop.
    val leafToLeafMDVCardinality = leafToLeafMDVCardinalityMap(amtDeltaLog)
    val oldLeaves = leafPointers(amtDeltaLog.update())
    val oldDeletedLeafCount = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
      .leaves.count(_.tracking.status == Tracking.Status.Deleted)

    val actualIncrementalAMTWriteMetrics = commitCheckpoint(amtDeltaLog, incremental = true)
      .getOrElse(fail("An incremental checkpoint must log IncrementalAMTWriteMetrics."))

    // 1) Reported metrics match the author's hand-specified expectation.
    assertIncrementalAMTWriteMetrics(actualIncrementalAMTWriteMetrics, expectedAMTWriteMetrics)
    // 1b) The intermediate-commit count, for the tests that know exactly how many commits the
    // window spans. Left unpinned otherwise, since it is the mechanical window size.
    expectedNumIntermediateCommits.foreach { expected =>
      assert(actualIncrementalAMTWriteMetrics.numIntermediateCommits == expected,
        s"Expected $expected intermediate commits; " +
          s"got ${actualIncrementalAMTWriteMetrics.numIntermediateCommits}.")
    }
    // 2) Reported metrics match a structural derivation from old-tree -> new-tree on disk.
    assertMetricsMatchTreeDelta(
      amtDeltaLog, leafToLeafMDVCardinality, oldDeletedLeafCount,
      actualIncrementalAMTWriteMetrics)
    // 3) Differential live set + white-box tree invariants.
    assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
    // 4) Carried-forward leaves keep their immutable manifest_info file counts.
    assertCarriedLeafCountsImmutable(oldLeaves, amtDeltaLog)
  }

  /**
   * Carried-forward leaves (present in both the old and new tree by location) keep their
   * manifest_info file/row counts unchanged -- those counts describe the leaf as written and are
   * immutable across trees; only the MDV and tracking evolve.
   */
  private def assertCarriedLeafCountsImmutable(
      oldLeaves: Map[String, DataManifestEntry], amtDeltaLog: DeltaLog): Unit = {
    def fileAndRowCounts(e: DataManifestEntry): (Int, Int, Int, Int, Long, Long, Long, Long) =
      (e.manifest_info.added_files_count, e.manifest_info.existing_files_count,
        e.manifest_info.deleted_files_count, e.manifest_info.replaced_files_count,
        e.manifest_info.added_rows_count, e.manifest_info.existing_rows_count,
        e.manifest_info.deleted_rows_count, e.manifest_info.replaced_rows_count)
    val newLeaves = leafPointers(amtDeltaLog.update())
    oldLeaves.foreach { case (location, oldLeaf) =>
      newLeaves.get(location).foreach { newLeaf =>
        assert(fileAndRowCounts(newLeaf) == fileAndRowCounts(oldLeaf),
          s"Carried leaf $location: manifest_info file/row counts must be immutable; " +
            s"old=${fileAndRowCounts(oldLeaf)} new=${fileAndRowCounts(newLeaf)}")
      }
    }
  }


  /** Asserts `actual` matches `expected` on all fields but the derived numIntermediateCommits. */
  private def assertIncrementalAMTWriteMetrics(
      actualMetrics: IncrementalAMTWriteMetrics,
      expectedMetrics: IncrementalAMTWriteMetrics): Unit = {
    val normalized =
      expectedMetrics.copy(numIntermediateCommits = actualMetrics.numIntermediateCommits)
    assert(actualMetrics == normalized,
      s"Incremental shape mismatch.\n  expected: $normalized\n  actual:   $actualMetrics")
  }

  /**
   * Derives the incremental write's shape from the structural difference between the old tree's
   * leaf pointers (`oldAMTLeafToMDV`: location -> MDV cardinality, captured before the write)
   * and the
   * new tree read straight off disk, then asserts the reported `metrics` equal that derivation.
   * This is independent of the writer's self-reported counters -- a leaf carried forward but
   * silently re-packed, or a miscounted metric, diverges here.
   *   - a carried leaf whose MDV grew  -> existing-leaf-updated, and the growth is MDV bits added;
   *   - a carried leaf whose MDV is unchanged -> existing-leaf-untouched;
   *   - a leaf location absent from the old tree -> a new (spilled) leaf;
   *   - root-resident live adds / tombstones are read directly off the new root.
   */
  private def assertMetricsMatchTreeDelta(
      amtDeltaLog: DeltaLog,
      oldAMTLeafToMDV: Map[String, Long],
      oldDeletedLeafCount: Int,
      metrics: IncrementalAMTWriteMetrics): Unit = {
    val newAMTLeafToMDV = leafToLeafMDVCardinalityMap(amtDeltaLog)
    // Materialize the carried leaves once (a lazy key-view would give inconsistent re-traversals).
    val carriedLeafLocations: Seq[String] =
      newAMTLeafToMDV.keys.filter(oldAMTLeafToMDV.contains).toSeq
    val perLeafBitsAdded: Seq[Long] =
      carriedLeafLocations.map(loc => newAMTLeafToMDV(loc) - oldAMTLeafToMDV(loc))
    val updated = perLeafBitsAdded.count(_ != 0L)
    val untouched = perLeafBitsAdded.count(_ == 0L)
    val newLeaves = newAMTLeafToMDV.keys.count(loc => !oldAMTLeafToMDV.contains(loc))
    val mdvBitsAdded = perLeafBitsAdded.sum
    val (rootLiveAdds, rootTombstones) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)

    // Per-status breakdown over every leaf pointer in the new tree, derived independently of the
    // writer's self-report; numStaleDeletedLeavesDropped is the old tree's DELETED tombstone count.
    val newLeafPointers = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed.")).leaves
    val statusToLeafCountMapping =
      newLeafPointers.groupBy(_.tracking.status).map { case (s, ps) => s -> ps.size }

    val derived = metrics.copy(
      numOldLeavesUpdated = updated,
      numOldLeavesUntouched = untouched,
      numNewLeaves = newLeaves,
      numRootLiveAdds = rootLiveAdds.toInt,
      numRootTombstones = rootTombstones.toInt,
      numLeafMdvBitsAdded = mdvBitsAdded.toInt,
      numLeavesAddedStatus = statusToLeafCountMapping.getOrElse(Tracking.Status.Added, 0),
      numLeavesExistingStatus = statusToLeafCountMapping.getOrElse(Tracking.Status.Existing, 0),
      numLeavesModifiedStatus = statusToLeafCountMapping.getOrElse(Tracking.Status.Modified, 0),
      numLeavesDeletedStatus = statusToLeafCountMapping.getOrElse(Tracking.Status.Deleted, 0),
      numStaleDeletedLeavesDropped = oldDeletedLeafCount)
    assert(metrics == derived,
      s"Reported metrics disagree with the old->new tree delta.\n" +
        s"  reported: $metrics\n  derived:  $derived\n" +
        s"  oldAMTLeafToMDV: $oldAMTLeafToMDV\n  newAMTLeafToMDV: $newAMTLeafToMDV")
  }

  /**
   * Asserts the AMT tree reconstructs exactly the baseline's (and its own) live path set, and runs
   * the white-box structural validation ([[assertTreeInvariants]]) on the same tree.
   */
  private def assertLiveAddFilesEquals(baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog): Unit = {
    assert(
      livePathsInLatestAMTCheckpoint(amtDeltaLog) == livePathsInLatestSnapshot(baselineDeltaLog),
      "The AMT tree must reconstruct exactly the baseline table's live file set.")
    assert(livePathsInLatestAMTCheckpoint(amtDeltaLog) == livePathsInLatestSnapshot(amtDeltaLog),
      "The AMT table's reconstruction must equal its own allFiles.")
    assertTreeInvariants(amtDeltaLog)
  }

  /**
   * Performs following validations on the underlying AMT (from latest snapshot):
   *   - each leaf pointer's `dv_cardinality` equals its decoded MDV bitmap size, and every MDV
   *     position is in range of that leaf's physical entry count (no stale / out-of-range mask);
   *   - conservation: the reader's live count equals root-resident live adds plus, over every leaf,
   *     (physical leaf entries - MDV cardinality)
   *   - no path is surfaced more than once by the reader.
   */
  private def assertTreeInvariants(amtDeltaLog: DeltaLog): Unit = {
    val snapshot = amtDeltaLog.update()
    val provider = amtProvider(snapshot)
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    val tableRoot = provider.tableRoot

    // Root: only its live-status DATA entries (added/existing) reconstruct as live files.
    val rootPath = provider.checkpointAction.contentRoot.getAbsolutePath(tableRoot).toString
    val statusToCountMap = trackingStatusToAddFileCountMap(rootPath)
    val rootLiveAdds =
      statusToCountMap
        .filter { case (status, _) => liveDataEntryStatuses.contains(status) }.values.sum

    // Leaves: each pointer's MDV must be internally consistent, and the unmasked entries are the
    // leaf's live contribution to the reconstruction.
    val leafLiveContribution = provider.leaves.map { leaf =>
      val leafPath = leaf.getAbsolutePath(tableRoot).toString
      val statusToCountMapForLeaf = trackingStatusToAddFileCountMap(leafPath)
      val physicalEntries = statusToCountMapForLeaf.values.sum
      val mdvDeclaredCardinality = leaf.manifest_info.dv_cardinality.getOrElse(0L)
      val decoded =
        leaf.manifest_info.dv.map(RoaringBitmapArray.readFrom).getOrElse(new RoaringBitmapArray)
      assert(decoded.cardinality == mdvDeclaredCardinality,
        s"Leaf ${leaf.location}: dv_cardinality=$mdvDeclaredCardinality but the decoded MDV has " +
          s"${decoded.cardinality} bits.")
      assert(mdvDeclaredCardinality <= physicalEntries,
        s"Leaf ${leaf.location}: MDV cardinality ($mdvDeclaredCardinality) exceeds physical " +
          s"entries " +
          s"($physicalEntries).")
      decoded.toArray.foreach { pos =>
        assert(pos >= 0 && pos < physicalEntries,
          s"Leaf ${leaf.location}: MDV position $pos is out of range [0, $physicalEntries).")
      }
      physicalEntries - mdvDeclaredCardinality
    }.sum

    val reconstructed = provider.loadActionsForStateReconstruction(spark, amtDeltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null").select("add.path").as[String].collect().toSeq
    assert(reconstructed.distinct.size == reconstructed.size,
      s"The reader must not surface a duplicate path: " +
        s"${reconstructed.diff(reconstructed.distinct).distinct}")
    assert(reconstructed.size.toLong == rootLiveAdds + leafLiveContribution,
      s"Tree conservation mismatch: reconstructed=${reconstructed.size}, " +
        s"rootLiveAdds=$rootLiveAdds, leafLiveContribution=$leafLiveContribution.")
  }

  /**
   * Runs `body` with a fresh baseline+AMT table pair, cleaning both up afterward.
   *
   * Every test gets its own pair rather than sharing one: `withTable` drops the catalog entry but
   * these tables are catalog-managed, so commit backfill can still be writing files when the next
   * test CREATEs at the same path, which fails with DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION.
   * A UUID keeps the names unique across shards and across reruns, where an earlier run's files may
   * still sit under the warehouse directory.
   */
  private def withTables(amtTableLocation: Option[String] = None)(
      body: (DeltaLog, DeltaLog) => Unit): Unit = {
    val uniqueSuffix = UUID.randomUUID().toString.replace("-", "_")
    val baselineNonAMTDeltaTable = s"amt_diff_baseline_$uniqueSuffix"
    val amtDeltaTable = s"amt_diff_subject_$uniqueSuffix"
    withTable(baselineNonAMTDeltaTable, amtDeltaTable) {
      val (baselineDeltaLog, amtDeltaLog) =
        createTables(baselineNonAMTDeltaTable, amtDeltaTable, amtTableLocation)
      body(baselineDeltaLog, amtDeltaLog)
    }
  }

  /** Metrics with fields defaulted to 0; numIntermediateCommits is derived by the assertion. */
  // scalastyle:off argcount
  private def createIncrementalAMTWriteMetrics(
      numOldLeavesUpdated: Int = 0,
      numOldLeavesUntouched: Int = 0,
      numNewLeaves: Int = 0,
      numRootLiveAdds: Int = 0,
      numRootTombstones: Int = 0,
      numLeafMdvBitsAdded: Int = 0,
      numLeavesAddedStatus: Int = 0,
      numLeavesExistingStatus: Int = 0,
      numLeavesModifiedStatus: Int = 0,
      numLeavesDeletedStatus: Int = 0,
      numStaleDeletedLeavesDropped: Int = 0): IncrementalAMTWriteMetrics =
    IncrementalAMTWriteMetrics(
      numIntermediateCommits = 0,
      numOldLeavesUpdated = numOldLeavesUpdated,
      numOldLeavesUntouched = numOldLeavesUntouched,
      numNewLeaves = numNewLeaves,
      numRootLiveAdds = numRootLiveAdds,
      numRootTombstones = numRootTombstones,
      numLeafMdvBitsAdded = numLeafMdvBitsAdded,
      numLeavesAddedStatus = numLeavesAddedStatus,
      numLeavesExistingStatus = numLeavesExistingStatus,
      numLeavesModifiedStatus = numLeavesModifiedStatus,
      numLeavesDeletedStatus = numLeavesDeletedStatus,
      numStaleDeletedLeavesDropped = numStaleDeletedLeavesDropped)
  // scalastyle:on argcount

  /////////////////////////////////////////////////////////////
  // Section-A:                                              //
  //     Incremental AMT tests for validating actions land   //
  //      in root and spill to leafs when needed             //
  /////////////////////////////////////////////////////////////

  test("A1: deferred append below the cap stays root-resident, no spill, leaf untouched") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Force 24 files (3 leaf * 8 files) so that none of the leaf is empty
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
        // One net-new add stays root-resident: the root holds 3 leaf pointers plus this add = 4,
        // which is under the cap of 8, so spillIfNeeded does not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3, numRootLiveAdds = 1,
          numLeavesExistingStatus = 3))
      }
    }
  }

  test("A2: deferred append exactly filling the root to the cap does not spill") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Force 24 files (3 leaf * 8 files) so that none of the leaf is empty
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
        // Fill the root exactly to the cap: 3 leaf pointers + 5 adds = 8 == cap. spillIfNeeded
        // loops while the total is `> cap`, so filling it exactly must not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 29).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3, numRootLiveAdds = 5,
          numLeavesExistingStatus = 3))
      }
    }
  }

  test("A3: deferred append over the cap spills whole cap-sized batches") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Force 24 files (3 leaf * 8 files) so that none of the leaf is empty
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
        // 12 net-new adds on a root already holding 3 leaf pointers, cap 8. spillIfNeeded trace:
        //   3(fixed) + 0(spilled) + 12(remaining) = 15 > 8 -> spill a batch of 8 (remaining = 4)
        //   3(fixed) + 1(spilled) +  4(remaining) =  8 == 8 -> stop.
        // => 1 new leaf, and the 4 leftover adds stay root-resident.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 36).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3,
          numNewLeaves = 1,
          numRootLiveAdds = 4,
          numLeavesExistingStatus = 3,
          numLeavesAddedStatus = 1))
      }
    }
  }

  test("A4: deferred append with entriesPerLeaf=1 spills every net-new add into its own leaf") {
    withTables() { (baselineDeltaLog, amtDeltaLog) =>
      // Bootstrap at a cap that clusters into several leaves (a single manifest would be promoted
      // into the root), then drop the cap to one so every net-new add spills into its own leaf.
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
        // Force 24 files (3 leaf * 8 files) so that none of the leaf is empty
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
      }
      val leafCount = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
      assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
        // At a cap of one, each of the 3 adds spills into its own leaf and none stays in the root.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25), fakeAdd(26), fakeAdd(27)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3, numNewLeaves = 3, numRootLiveAdds = 0,
          numLeavesExistingStatus = 3, numLeavesAddedStatus = 3))
      }
    }
  }

  test("A5: a large deferred append spills multiple cap-sized leaves, each within the cap") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Force 24 files (3 leaf * 8 files) so that none of the leaf is empty
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val oldExistingLeaves =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
            .leaves.map(_.location).toSet
        val leafCount = oldExistingLeaves.size
        assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
        // 30 net-new adds on a root already holding 3 leaf pointers, cap 8. spillIfNeeded trace:
        //   3 + 0 + 30 = 33 > 8 -> spill 8 (remaining 22)
        //   3 + 1 + 22 = 26 > 8 -> spill 8 (remaining 14)
        //   3 + 2 + 14 = 19 > 8 -> spill 8 (remaining  6)
        //   3 + 3 +  6 = 12 > 8 -> spill 6 (remaining  0)
        // => 4 new leaves, 0 root-resident adds.
        commitBoth(baselineDeltaLog, amtDeltaLog, (25 to 54).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3,
          numNewLeaves = 4,
          numRootLiveAdds = 0,
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

  test("A6: spill accounting includes carried leaf pointers in fixedRootCount") {
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
        // Append one file. All carried pointers stay untouched; whether the new file lands in the
        // root or a spilled leaf depends only on the carried count vs the cap. The cross-check
        // derives the exact split from the on-disk old->new tree, so pin only what we control.
        val oldAMTLeafToMDV = leafToLeafMDVCardinalityMap(amtDeltaLog)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(7)))
        val actual = commitCheckpoint(amtDeltaLog, incremental = true)
          .getOrElse(fail("An incremental checkpoint must log metrics."))
        assert(actual.numOldLeavesUntouched == numLeafCountBefore,
          s"All $numLeafCountBefore carried leaves must be untouched by an append; got " +
            s"${actual.numOldLeavesUntouched}.")
        assert(actual.numRootLiveAdds + actual.numNewLeaves == 1,
          "The one appended file must land in exactly one place (root or a spilled leaf).")
        // The old tree is a fresh full rewrite, so it carries no DELETED tombstones to drop.
        assertMetricsMatchTreeDelta(amtDeltaLog, oldAMTLeafToMDV, oldDeletedLeafCount = 0,
          metrics = actual)
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("A7: consecutive intermediate insert commits accumulate their adds in the root") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Force 24 files (3 leaf * 8) so the bootstrap is tree-shaped, not a promoted root.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 24).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafCount =
          amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
        assert(leafCount == 3, s"Expected 3 leaves in the bootstrap tree; got $leafCount.")
        // The adding analog of C4: several separate INSERT commits, none checkpointed, each adding
        // one net-new file. The deferred incremental must fold every one into the root -- 3 leaf
        // pointers + 4 adds = 7 <= cap 8, so all four stay root-resident and nothing spills.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(25)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(26)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(27)))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(28)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = 3, numRootLiveAdds = 4,
          numLeavesExistingStatus = 3),
          expectedNumIntermediateCommits = Some(5))
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // Section-B:                                              //
  //     Incremental AMT tests on top of a full AMT that     //
  //      has no leafs.                                      //
  /////////////////////////////////////////////////////////////

  /**
   * Bootstraps a leafless full AMT: a single live file clusters into one manifest, which the full
   * rewrite promotes to the root. Returns after asserting the tree really has no leaf pointers.
   */
  private def fullCheckpointPromotedToRoot(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog): Unit = {
    commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(1)))
    commitCheckpoint(amtDeltaLog, incremental = false)
    val leafCount = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
    assert(leafCount == 0,
      s"A single-manifest full rewrite must be promoted to the root; got $leafCount leaves.")
  }

  test("B1: added actions fit in the root of a leafless AMT and are not spilled") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // No leaf pointers, so fixedRootCount is 0. The replay carries the promoted root's own file
        // forward as a root-resident add, so the new root holds that one plus the 3 appended = 4,
        // well under the cap of 8: nothing spills and every live add stays root-resident.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 4).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numRootLiveAdds = 4))
      }
    }
  }

  test("B2: added actions exactly fill the root of a leafless AMT") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // fixedRootCount is 0 and the replay carries the bootstrap's own file forward as a
        // root-resident add, so 8 live adds fill the root exactly to the cap. spillIfNeeded loops
        // while the total is `> cap`, so an exactly-full root must not spill.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 8).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numRootLiveAdds = 8))
      }
    }
  }

  test("B3: added actions overflow the root of a leafless AMT and spill into one leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // 9 live adds (the bootstrap's file + 8 new) on a root with no pointers, cap 8:
        //   0(fixed) + 0(spilled) + 9(remaining) = 9 > 8 -> spill a batch of 8 (remaining = 1)
        //   0(fixed) + 1(spilled) + 1(remaining) = 2 <= 8 -> stop.
        // => 1 new leaf, 1 root-resident add.
        commitBoth(baselineDeltaLog, amtDeltaLog, (2 to 9).map(fakeAdd))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numNewLeaves = 1, numRootLiveAdds = 1,
          numLeavesAddedStatus = 1))
      }
    }
  }

  test("B4: added actions overflow the root of a leafless AMT and spill into three leaves") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numNewLeaves = 3, numRootLiveAdds = 0,
          numLeavesAddedStatus = 3))
      }
    }
  }

  test("B5: deleting a file from a promoted root drops it via replay, no leaf, no tombstone") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "8") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        fullCheckpointPromotedToRoot(baselineDeltaLog, amtDeltaLog)
        // Add two more files; all three live in the promoted root (no leaves, under the cap).
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(2), fakeAdd(3)))
        commitCheckpoint(amtDeltaLog, incremental = true)
        assert(leafPointers(amtDeltaLog.update()).isEmpty,
          "The tree must stay leafless after a small append to a promoted root.")
        // Delete one root-resident file. It has no back reference, so replay drops it (as in
        // deferred D1): no leaf is touched and, deferred, no root tombstone is written.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, 2)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numRootLiveAdds = 2))
        val (rootLiveAdds, tombstones) = liveAddsAndTombstonesCountInRoot(amtDeltaLog)
        assert(rootLiveAdds == 2, s"Two files must remain live in the root; got $rootLiveAdds.")
        assert(tombstones == 0,
          s"A deferred remove of a root-resident file writes no tombstone; got $tombstones.")
      }
    }
  }

  test("B6: deleting every file from a promoted root yields an empty, still-readable tree") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics())
        assert(leafPointers(amtDeltaLog.update()).isEmpty,
          "The tree must remain leafless.")
        assert(livePathsInLatestAMTCheckpoint(amtDeltaLog).isEmpty,
          "The AMT tree must reconstruct an empty live set.")
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // Section-C:                                              //
  //     Incremental AMT tests for validating leaf           //
  //      carry-forward and MDV masking of removed files     //
  /////////////////////////////////////////////////////////////

  test("C1: deleting one leaf-resident file sets a single MDV bit on its leaf") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = leafCount - 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = leafCount - 1))
      }
    }
  }

  test("C2: MDV masking applies independently across two distinct leaves") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 -> ceil(15/5)=3 leaves, all non-empty (the full-rewrite fills them).
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need >= 2 leaves; got ${byLeaf.size}.")
        // Pick one file from each of two distinct leaves, so exactly two leaves are updated.
        val twoLeaves = byLeaf.toSeq.sortBy(_._1).take(2)
        val victims = twoLeaves.map { case (_, files) => files.head }
        val untouchedLeaves = byLeaf.size - 2
        commitBoth(baselineDeltaLog, amtDeltaLog, victims.map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 2,
          numOldLeavesUntouched = untouchedLeaves,
          numLeafMdvBitsAdded = 2,
          numLeavesModifiedStatus = 2,
          numLeavesExistingStatus = untouchedLeaves))
      }
    }
  }

  test("C3: deleting two files from the SAME leaf adds two bits to one leaf") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        // A leaf holding >= 2 files; delete two of them -> one leaf updated, two bits.
        val (_, files) = byLeaf.toSeq.sortBy(_._1).find(_._2.size >= 2)
          .getOrElse(fail("Expected some leaf to hold >= 2 files at cap 5 with 15 files."))
        val untouchedLeaves = byLeaf.size - 1
        commitBoth(baselineDeltaLog, amtDeltaLog, files.take(2).map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = untouchedLeaves,
          numLeafMdvBitsAdded = 2,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = untouchedLeaves))
      }
    }
  }

  test("C4: multiple deferred intermediate commits accumulate all their leaf removes in the MDV") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves rather than one promoted root manifest.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        // One victim from each of two distinct leaves, so the two intermediate commits land their
        // bits on different carried pointers.
        val twoLeaves = byLeaf.toSeq.sortBy(_._1).take(2)
        val victimFromLeaf1 = twoLeaves.head._2.head
        val victimFromLeaf2 = twoLeaves(1)._2.head
        // Two separate intermediate commits each remove one leaf file; the deferred incremental
        // must accumulate both bits though neither is this commit's own action.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFromLeaf1.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFromLeaf2.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 2,
          numOldLeavesUntouched = byLeaf.size - 2,
          numLeafMdvBitsAdded = 2,
          numLeavesModifiedStatus = 2,
          numLeavesExistingStatus = byLeaf.size - 2))
      }
    }
  }

  test("C5: consecutive incremental AMT deletes accumulate MDV bits across writes") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // 15 files at cap 5 clusters into several leaves rather than one promoted root manifest.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        // Both victims share one leaf, so each write updates that same carried pointer.
        val victims = byLeaf.toSeq.sortBy(_._1).find(_._2.size >= 2)
          .getOrElse(fail("Expected some leaf to hold >= 2 files."))._2.take(2)
        val untouched = byLeaf.size - 1
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victims.head.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = untouched,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = untouched))
        // The second write adds only its own bit; the leaf's cumulative MDV covers both, checked
        // by the live-set baselineDeltaLog.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victims(1).remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = untouched,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = untouched))
      }
    }
  }

  test("C6: incremental AMT writes handles paths with spaces correctly") {
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
          val byLeaf = leafToAddFileMap(amtDeltaLog)
          assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
          // Every leaf key must be table-root-relative, never the absolute path that would make the
          // string match below succeed only by coincidence.
          byLeaf.keys.foreach { leaf =>
            assert(leaf.startsWith("metadata/") && !leaf.contains(baseDir.toString),
              s"Leaf key must be table-root-relative; got $leaf.")
          }
          val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
          commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
          createIncrementalAMTAndValidate(
            baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
            numOldLeavesUpdated = 1,
            numOldLeavesUntouched = byLeaf.size - 1,
            numLeafMdvBitsAdded = 1,
            numLeavesModifiedStatus = 1,
            numLeavesExistingStatus = byLeaf.size - 1))
        }
      }
    }
  }

  test("C7: an incremental AMT carries the leaf parquet forward byte-for-byte") {
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
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = byLeaf.size - 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = byLeaf.size - 1))
        val after =
          leafFingerprints(amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")))
        assert(after == before,
          s"Incremental must carry leaves forward untouched.\n  before=$before\n  after=$after")
      }
    }
  }

  test("C8: an untouched sibling leaf keeps an empty MDV while another is masked") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "5") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to 15).map(fakeAdd)) // 3 leaves at cap 5.
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need >= 2 leaves; got ${byLeaf.size}.")
        // Delete one file from exactly one leaf; the other leaves must keep an empty MDV.
        val (victimLeaf, victimFiles) = byLeaf.toSeq.minBy(_._1)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victimFiles.head.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = byLeaf.size - 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = byLeaf.size - 1))
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

  test("C9: a stale DELETED leaf is dropped by the next incremental rewrite") {
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
        val metrics = commitCheckpoint(amtDeltaLog, incremental = true)
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

  test("C10: deleting every leaf-resident file masks every leaf fully, marking each DELETED") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // The extreme end of C1-C3: instead of masking one position on one leaf, mask EVERY
        // position on EVERY leaf. Each leaf's cumulative MDV then covers all its entries, so the
        // pointer is carried this commit as a DELETED tombstone (the reader skips it), and the tree
        // reconstructs an empty live set.
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        val allLeafFiles = byLeaf.values.flatten.toSeq
        commitBoth(baselineDeltaLog, amtDeltaLog, allLeafFiles.map(_.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = byLeaf.size,
          numLeafMdvBitsAdded = allLeafFiles.size,
          numLeavesDeletedStatus = byLeaf.size))
        // Every carried leaf pointer is now a DELETED tombstone.
        val statuses = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
          .leaves.map(_.tracking.status)
        assert(statuses.forall(_ == Tracking.Status.Deleted) && statuses.size == byLeaf.size,
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

  /////////////////////////////////////////////////////////////
  // Section-D:                                              //
  //     Incremental AMT tests for the intermediate          //
  //      commits between the old AMT root and the           //
  //      proposed commit: how they are assembled, and       //
  //      that replaying them is correct, both for one       //
  //      write and across a chain of them                   //
  /////////////////////////////////////////////////////////////

  test("D1: deleting an old root-resident file drops it via replay, no MDV") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numRootLiveAdds = 1,
          numLeavesExistingStatus = numLeafCountBefore))
        // Delete id=31 (root-resident): remove has NO backref -> dropped by replay, no MDV bit.
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(removeOf(amtDeltaLog, leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numLeafMdvBitsAdded = 0,
          numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("D2: an add then delete of the same file within the intermediate commits is net-zero") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numLeafMdvBitsAdded = 0,
          numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("D3: a leaf file removed then re-added at the same path is masked once") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        // Window: remove a leaf-resident file (it carries a backref) then re-add the SAME path. The
        // old leaf entry is MDV-masked and the re-added copy is root-resident; reconstructed once.
        // The re-add keeps the back reference the file was stamped with: its path is still the one
        // the leaf holds, and a commit reusing a leaf-resident path must carry that leaf's
        // reference.
        val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = byLeaf.size - 1,
          numRootLiveAdds = 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = byLeaf.size - 1))
      }
    }
  }

  test("D4: a leaf file re-added and removed again is masked once, not double-counted") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        // Remove a leaf-resident file, re-add the same path, then remove it again. Both removes
        // carry the same back reference, so both target the same (leaf, position). Note the re-add
        // can only follow a remove: re-adding a path that is still live would be an in-place file
        // metadata update, which a WRITE is not allowed to perform.
        val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
        val victimLeaf = byLeaf.toSeq.sortBy(_._1).head._1
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        // The two removes share one (leaf, position), which the writer holds as a set, so the write
        // reports a single MDV bit, matching what the leaf's bitmap actually gains. That
        // agreement is what lets this go through the shared validator, whose second check
        // derives the bits from the on-disk dv_cardinality delta.
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = byLeaf.size - 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = byLeaf.size - 1))
        val maskedLeaf = leafPointers(amtDeltaLog.update()).getOrElse(victimLeaf,
          fail(s"Leaf $victimLeaf must still be carried forward."))
        assert(mdvCardinality(maskedLeaf) == 1L,
          s"The twice-removed position must be masked once; " +
            s"got ${mdvCardinality(maskedLeaf)} bits.")
      }
    }
  }

  test("D5: a re-add at a different path leaves both files live, no double-count") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        // Delete a leaf-resident file, then add a NEW file at a different path. The leaf gets one
        // MDV bit, the new file is a root-resident live add; both reconstruct exactly once.
        val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = byLeaf.size - 1,
          numRootLiveAdds = 1,
          numLeafMdvBitsAdded = 1,
          numLeavesModifiedStatus = 1,
          numLeavesExistingStatus = byLeaf.size - 1))
      }
    }
  }

  test("D6: an incremental AMT with no intermediate commits reconstructs unchanged") {
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

  test("D7: a deferred incremental AMT folds in every intermediate commit and counts them") {
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
            numRootLiveAdds = businessCommits,
            numLeavesExistingStatus = numLeafCountBefore),
          expectedNumIntermediateCommits = Some(businessCommits + 1))
      }
    }
  }


  test("D8: writeIncremental rejects intermediate commits with a hole up to attemptVersion") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(1), fakeAdd(2)))
        // A base AMT to build intermediate commits on.
        commitCheckpoint(amtDeltaLog, incremental = false)
        val snapshot = amtDeltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
        val oldAMTVersion = provider.checkpointAction.version
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

  test("D10: replay re-derives the root across a chain of incremental AMTs") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
        val numLeafCountBefore = byLeaf.size

        // incr 1: append one file, which stays root-resident (carried pointers + 1 add <= cap).
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 1)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numRootLiveAdds = 1,
          numLeavesExistingStatus = numLeafCountBefore))

        // incr 2: delete a leaf-resident file -> its leaf gets one MDV bit. numRootLiveAdds is
        // still 1: the file appended by incr 1 must survive as a root-resident live add, which is
        // replay re-deriving the root's live set from the PREVIOUS incremental's root (part 1a).
        val victim = byLeaf.toSeq.sortBy(_._1).head._2.head
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(victim.remove))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUpdated = 1,
          numOldLeavesUntouched = numLeafCountBefore - 1,
          numRootLiveAdds = 1,
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

  test("D11: a long mixed chain of writes folds into ONE incremental AMT") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val byLeaf = leafToAddFileMap(amtDeltaLog)
        assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")

        // The opposite packing of D10: NOTHING is checkpointed until the very end, so instead of
        // one write per incremental, all these interleaved appends and deletes land in a single
        // incremental's intermediate commits, to be folded in at once.
        val leafVictims = byLeaf.toSeq.sortBy(_._1).flatMap(_._2.take(1))
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
            numOldLeavesUntouched = byLeaf.size - 2,
            numRootLiveAdds = 4,
            numLeafMdvBitsAdded = 2,
            numLeavesModifiedStatus = 2,
            numLeavesExistingStatus = byLeaf.size - 2),
          expectedNumIntermediateCommits = Some(8))
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // Section-E:                                              //
  //     Incremental AMT tests for version bookkeeping       //
  /////////////////////////////////////////////////////////////

  test("E1: a deferred incremental AMT describes attemptVersion - 1") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numRootLiveAdds = 1,
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

  test("E2: lastManifestCommitWithFullRewrite is pinned across a chain of incremental AMTs") {
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
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numRootLiveAdds = 1,
          numLeavesExistingStatus = numLeafCountBefore))
        assertCheckpointDescribesVersion(amtDeltaLog, expectedVersion = 3L)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(leafPackedFiles + 2)))
        createIncrementalAMTAndValidate(
          baselineDeltaLog, amtDeltaLog, createIncrementalAMTWriteMetrics(
          numOldLeavesUntouched = numLeafCountBefore, numRootLiveAdds = 2,
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

  /////////////////////////////////////////////////////////////
  // Section-F:                                              //
  //     Leaf-pointer tracking.status transition chains      //
  //      followed across a sequence of incremental rewrites //
  /////////////////////////////////////////////////////////////

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
    val byLeaf = leafToAddFileMap(amtDeltaLog)
    assert(byLeaf.size >= 2, s"Need a tree-shaped bootstrap; got ${byLeaf.size} leaves.")
    val target = byLeaf.toSeq.sortBy { case (loc, files) => (-files.size, loc) }.head._1
    assertLeafStatus(amtDeltaLog, target, Tracking.Status.Added, "bootstrap")
    target
  }

  /** Removes `files` from both tables, then lands one incremental AMT checkpoint. */
  private def removeFilesAndCheckpoint(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog, files: Seq[AddFile]): Unit = {
    commitBoth(baselineDeltaLog, amtDeltaLog, files.map(_.remove))
    commitCheckpoint(amtDeltaLog, incremental = true)
  }

  test("F1: ADDED -> EXISTING (a freshly written leaf carried untouched)") {
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

  test("F2: ADDED -> DELETED -> removed (fully masked, then dropped)") {
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

  test("F3: ADDED -> MODIFIED (a freshly written leaf partially masked)") {
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

  test("F4: ADDED -> EXISTING -> EXISTING (carried untouched twice)") {
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

  test("F5: ADDED -> EXISTING -> DELETED -> removed") {
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

  test("F6: ADDED -> EXISTING -> MODIFIED -> EXISTING") {
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

  test("F7: ADDED -> EXISTING -> MODIFIED -> MODIFIED") {
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

  test("F8: ADDED -> EXISTING -> MODIFIED -> DELETED") {
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
