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
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, Metadata, Protocol, RemoveFile}
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
  private def fakeAdd(fileID: Int): AddFile = fakeAdd(fileID, dataChange = true)

  private def fakeAdd(fileID: Int, dataChange: Boolean): AddFile =
    AddFile(
      path = f"part-$fileID%05d.parquet",
      partitionValues = Map.empty,
      size = 100L + fileID,
      modificationTime = 1000L + fileID,
      dataChange = dataChange,
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
      baselineDeltaLog: DeltaLog,
      amtDeltaLog: DeltaLog,
      actions: Seq[Action],
      operation: DeltaOperations.Operation = writeOperation): Unit = {
    val baselineActions = actions.map {
      case a: AddFile => a.copy(backReference = None)
      case r: RemoveFile => r.copy(backReference = None)
      case other => other
    }
    baselineDeltaLog.startTransaction().commit(baselineActions, operation)
    amtDeltaLog.startTransaction().commit(actions, operation)
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

  /**
   * DATA-entry `location`s (each `add.path`, a file's unique id) in a manifest parquet, keyed by
   * `tracking.status`. Lets a test assert *which* file carries each status, not just the counts.
   */
  private def trackingStatusToLocationsMap(absManifestPath: String): Map[Int, Set[String]] =
    allowReadWithinDeltaLog {
      spark.read.parquet(absManifestPath)
        .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
        .select(col("tracking.status").as("status"), col("location"))
        .as[(Int, String)].collect()
        .groupBy(_._1).map { case (status, rows) => status -> rows.map(_._2).toSet }
    }

  /** Root-resident DATA-entry counts keyed by tracking.status (DATA_MANIFEST pointers excluded). */
  private def rootDataEntryStatusToCount(amtDeltaLog: DeltaLog): Map[Int, Long] = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    trackingStatusToAddFileCountMap(
      provider.checkpointAction.contentRoot.getAbsolutePath(provider.tableRoot).toString)
  }

  /** Root-resident DATA-entry `location`s keyed by tracking.status (pointers excluded). */
  private def rootDataEntryStatusToLocations(amtDeltaLog: DeltaLog): Map[Int, Set[String]] = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    trackingStatusToLocationsMap(
      provider.checkpointAction.contentRoot.getAbsolutePath(provider.tableRoot).toString)
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

  /**
   * A descriptor-only synthetic deletion vector named `name` (a 'p'-type path descriptor pointing
   * at no real bytes).
   */
  private def syntheticDv(name: String): DeletionVectorDescriptor =
    DeletionVectorDescriptor.onDiskWithAbsolutePath(
      // A `p` DV path must parse as an absolute URI (scheme required); the file is never read.
      path = s"file:/$name", sizeInBytes = 5, cardinality = 5L, offset = Some(1))

  /**
   * Generates a Remove of the current file path (with whatever DV it has) plus an Add of the same
   * file under a given DV. E.g. if the table holds file f1 with DV dv4, this returns a remove of
   * (f1, dv4) and an add of (f1, given_dv).
   */
  private def removeAndReAddWithDV(
      amtDeltaLog: DeltaLog, fileIdOrAddFile: Either[Int, AddFile], dvID: Int = 1): Seq[Action] = {
    val liveAddFile = fileIdOrAddFile match {
      case Left(fileID) =>
        val path = fakeAdd(fileID).path
        liveAddFilesInLatestSnapshot(amtDeltaLog).find(_.path == path)
          .getOrElse(fail(s"fileID=$fileID ($path) is not live in the AMT table."))
      case Right(add) => add
    }
    val newDv = syntheticDv(s"${liveAddFile.path}_dv_${dvID}")
    assert(newDv != liveAddFile.deletionVector,
      "new DV must differ from the file's current DV to change its (path, dv) key.")
    // The re-add drops the back reference -- the Remove already carries it (masking the old leaf
    // slot), and the re-added copy is a fresh (path, dv) key, not an existing leaf entry. It
    // reports non-tight stats bounds (required for a DV-bearing file).
    Seq(
      liveAddFile.remove,
      liveAddFile.copy(
        deletionVector = newDv,
        backReference = None,
        stats = s"""{"numRecords":1,"tightBounds":false}""",
        dataChange = true))
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
      byStatus.filter { case (status, _) =>
        AMTCheckpointProvider.liveDataEntryStatuses.contains(status) }.values.sum
    val tombstones = tombstoneTrackingStatuses.toSeq.map(byStatus.getOrElse(_, 0L)).sum
    (liveAdds, tombstones)
  }

  /** The per-commit CDF `tracking.deleted_positions` off a leaf pointer (empty if unset). */
  private def leafDeletedPositions(leaf: DataManifestEntry): Set[Long] =
    leaf.tracking.deleted_positions
      .map(RoaringBitmapArray.readFrom(_).toArray.toSet).getOrElse(Set.empty)

  /** The per-commit CDF `tracking.replaced_positions` off a leaf pointer (empty if unset). */
  private def leafReplacedPositions(leaf: DataManifestEntry): Set[Long] =
    leaf.tracking.replaced_positions
      .map(RoaringBitmapArray.readFrom(_).toArray.toSet).getOrElse(Set.empty)

  /** The current snapshot's leaf pointers, keyed by relative location. */
  private def leafPointers(snapshot: Snapshot): Map[String, DataManifestEntry] = {
    val provider = amtProvider(snapshot).getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
    provider.leaves.map(l => l.location -> l).toMap
  }

  /** The MDV cardinality numLeafCountBefore on a leaf pointer, 0 when it has none. */
  private def mdvCardinality(leaf: DataManifestEntry): Long =
    leaf.manifest_info.dv_cardinality.getOrElse(0L)

  /**
   * Emits one incremental AMT and cross-checks its metrics three ways: the hand-specified
   * `expectedAMTWriteMetrics` (author intent), the metrics the writer actually reported, and a
   * structural derivation from the old tree vs. the new tree read straight off disk. Also compares
   * the live files on baselineDeltaLog and amtDeltaLog.
   *
   * The emission route depends on `inlineAMTCommitActions`:
   *   - `None` (deferred): a follow-up OPTIMIZE CHECKPOINT folds the already-committed intermediate
   *     commits in with an empty actionsToCommit.
   *   - `Some(actions)` (inline): the actions ride the same commit that emits the tree
   *     ([[withInline]] sets the inline threshold to 1), so the metrics come off that commit and
   *     the checkpoint describes its OWN version -- unlike a deferred write (E1), which describes
   *     attemptVersion - 1.
   */
  private def createIncrementalAMTAndValidate(
      baselineDeltaLog: DeltaLog,
      amtDeltaLog: DeltaLog,
      expectedAMTWriteMetrics: IncrementalAMTWriteMetrics,
      expectedNumIntermediateCommits: Option[Int] = None,
      inlineAMTCommitActions: Option[Seq[Action]] = None,
      inlineOperation: DeltaOperations.Operation = writeOperation): Unit = {
    // Snapshot the OLD tree's leaf pointers before the write: their MDV cardinality (for the shape
    // derivation), the full pointers (to assert manifest_info counts stay immutable), and the count
    // of DELETED tombstone pointers this rewrite is expected to drop.
    val leafToLeafMDVCardinality = leafToLeafMDVCardinalityMap(amtDeltaLog)
    val oldLeaves = leafPointers(amtDeltaLog.update())
    val oldDeletedLeafCount = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
      .leaves.count(_.tracking.status == Tracking.Status.Deleted)

    val actualIncrementalAMTWriteMetrics = inlineAMTCommitActions match {
      case Some(actions) =>
        val attemptVersion = amtDeltaLog.update().version + 1
        val metrics = withInline {
          trackIncrementalAMTWriteMetrics(attemptVersion) {
            commitBoth(baselineDeltaLog, amtDeltaLog, actions, inlineOperation)
          }
        }.getOrElse(fail("An inline incremental write must log IncrementalAMTWriteMetrics."))
        assertCheckpointDescribesVersion(amtDeltaLog, attemptVersion)
        metrics
      case None =>
        commitCheckpoint(amtDeltaLog, incremental = true)
          .getOrElse(fail("An incremental checkpoint must log IncrementalAMTWriteMetrics."))
    }

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
    val rootDataEntryStatusToCountMap = rootDataEntryStatusToCount(amtDeltaLog)

    // Per-status breakdown over every leaf pointer in the new tree, derived independently of the
    // writer's self-report; numStaleDeletedLeavesDropped is the old tree's DELETED tombstone count.
    val newLeafPointers = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed.")).leaves
    val statusToLeafCountMapping =
      newLeafPointers.groupBy(_.tracking.status).map { case (s, ps) => s -> ps.size }
    val deleteCDFBitsAdded = newLeafPointers.map(p => leafDeletedPositions(p).size).sum
    val replaceCDFBitsAdded = newLeafPointers.map(p => leafReplacedPositions(p).size).sum

    val derived = metrics.copy(
      numOldLeavesUpdated = updated,
      numOldLeavesUntouched = untouched,
      numNewLeaves = newLeaves,
      numRootEntriesAddedStatus =
        rootDataEntryStatusToCountMap.getOrElse(Tracking.Status.Added, 0L).toInt,
      numRootEntriesExistingStatus =
        rootDataEntryStatusToCountMap.getOrElse(Tracking.Status.Existing, 0L).toInt,
      numRootEntriesModifiedStatus =
        rootDataEntryStatusToCountMap.getOrElse(Tracking.Status.Modified, 0L).toInt,
      numRootEntriesReplacedStatus =
        rootDataEntryStatusToCountMap.getOrElse(Tracking.Status.Replaced, 0L).toInt,
      numRootEntriesDeletedStatus =
        rootDataEntryStatusToCountMap.getOrElse(Tracking.Status.Deleted, 0L).toInt,
      numLeafMdvBitsAdded = mdvBitsAdded.toInt,
      numLeafDeleteCDFBitsAdded = deleteCDFBitsAdded.toInt,
      numLeafReplaceCDFBitsAdded = replaceCDFBitsAdded.toInt,
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
        .filter { case (status, _) =>
          AMTCheckpointProvider.liveDataEntryStatuses.contains(status) }.values.sum

    // Leaves: each pointer's MDV must be internally consistent, and the unmasked LIVE entries are
    // the leaf's live contribution. A tombstone-only leaf (born ADDED but holding no live entry)
    // and a DELETED leaf both contribute nothing live, matching the reader.
    val leafLiveContribution = provider.leaves
      .filter(_.tracking.status != Tracking.Status.Deleted)
      .map { leaf =>
        val leafPath = leaf.getAbsolutePath(tableRoot).toString
        val statusToCountMapForLeaf = trackingStatusToAddFileCountMap(leafPath)
        val physicalEntries = statusToCountMapForLeaf.values.sum
        val livePhysicalEntries = statusToCountMapForLeaf
          .filter { case (status, _) =>
            AMTCheckpointProvider.liveDataEntryStatuses.contains(status) }
          .values.sum
        val mdvDeclaredCardinality = leaf.manifest_info.dv_cardinality.getOrElse(0L)
        val decoded =
          leaf.manifest_info.dv.map(RoaringBitmapArray.readFrom).getOrElse(new RoaringBitmapArray)
        assert(decoded.cardinality == mdvDeclaredCardinality,
          s"Leaf ${leaf.location}: dv_cardinality=$mdvDeclaredCardinality but the decoded MDV " +
            s"has ${decoded.cardinality} bits.")
        assert(mdvDeclaredCardinality <= physicalEntries,
          s"Leaf ${leaf.location}: MDV cardinality ($mdvDeclaredCardinality) exceeds physical " +
            s"entries " +
            s"($physicalEntries).")
        decoded.toArray.foreach { pos =>
          assert(pos >= 0 && pos < physicalEntries,
            s"Leaf ${leaf.location}: MDV position $pos is out of range [0, $physicalEntries).")
        }
        livePhysicalEntries - mdvDeclaredCardinality
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
      numRootEntriesAddedStatus: Int = 0,
      numRootEntriesExistingStatus: Int = 0,
      numRootEntriesModifiedStatus: Int = 0,
      numRootEntriesReplacedStatus: Int = 0,
      numRootEntriesDeletedStatus: Int = 0,
      numLeafMdvBitsAdded: Int = 0,
      numLeafDeleteCDFBitsAdded: Int = 0,
      numLeafReplaceCDFBitsAdded: Int = 0,
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
      numRootEntriesAddedStatus = numRootEntriesAddedStatus,
      numRootEntriesExistingStatus = numRootEntriesExistingStatus,
      numRootEntriesModifiedStatus = numRootEntriesModifiedStatus,
      numRootEntriesReplacedStatus = numRootEntriesReplacedStatus,
      numRootEntriesDeletedStatus = numRootEntriesDeletedStatus,
      numLeafMdvBitsAdded = numLeafMdvBitsAdded,
      numLeafDeleteCDFBitsAdded = numLeafDeleteCDFBitsAdded,
      numLeafReplaceCDFBitsAdded = numLeafReplaceCDFBitsAdded,
      numLeavesAddedStatus = numLeavesAddedStatus,
      numLeavesExistingStatus = numLeavesExistingStatus,
      numLeavesModifiedStatus = numLeavesModifiedStatus,
      numLeavesDeletedStatus = numLeavesDeletedStatus,
      numStaleDeletedLeavesDropped = numStaleDeletedLeavesDropped)
  // scalastyle:on argcount

  /**
   * Bootstraps a tree: the `initialIdRangeInLeaf` files packed into whole leaves via a full
   * checkpoint, then (when `initialIdRangeInRoot` is non-empty) that id range appended
   * root-resident via an incremental checkpoint. The caller must have set AMT_ENTRIES_PER_LEAF to
   * `entriesPerLeaf`. Returns the leaf pointers of the bootstrapped tree.
   */
  private def setup(
      baselineDeltaLog: DeltaLog,
      amtDeltaLog: DeltaLog,
      entriesPerLeaf: Int,
      initialIdRangeInLeaf: Range,
      initialIdRangeInRoot: Range = Range(0, 0)): Seq[DataManifestEntry] = {
    require(initialIdRangeInLeaf.size > entriesPerLeaf,
      s"initialIdRangeInLeaf (${initialIdRangeInLeaf.size}) must exceed entriesPerLeaf " +
        s"($entriesPerLeaf).")
    val numLeaves = math.ceil(initialIdRangeInLeaf.size.toDouble / entriesPerLeaf).toInt
    require(initialIdRangeInRoot.size <= entriesPerLeaf - numLeaves,
      s"initialIdRangeInRoot (${initialIdRangeInRoot.size}) must be <= " +
        s"${entriesPerLeaf - numLeaves} (entriesPerLeaf minus $numLeaves leaf pointers) so the " +
        "root-resident adds do not spill.")
    commitBoth(baselineDeltaLog, amtDeltaLog, initialIdRangeInLeaf.map(fakeAdd))
    commitCheckpoint(amtDeltaLog, incremental = false)
    assert(leafPointers(amtDeltaLog.update()).size == numLeaves,
      s"the full checkpoint must pack $numLeaves leaves.")
    if (initialIdRangeInRoot.nonEmpty) {
      commitBoth(baselineDeltaLog, amtDeltaLog, initialIdRangeInRoot.map(fakeAdd))
      commitCheckpoint(amtDeltaLog, incremental = true)
      assert(leafPointers(amtDeltaLog.update()).size == numLeaves,
        "the root-resident adds must not spill any leaf.")
    }
    leafPointers(amtDeltaLog.update()).values.toSeq
  }

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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numRootEntriesExistingStatus = 1,
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = 3,
            numRootEntriesExistingStatus = 5,
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
        // Append one file. All carried pointers stay untouched. The carried leaf pointers already
        // fill the root to the cap (fixedRootCount counts them, and numLeafCountBefore >= cap = 2),
        // so the appended file has no root capacity and must spill into exactly one new leaf --
        // which is the accounting this test guards.
        val oldAMTLeafToMDV = leafToLeafMDVCardinalityMap(amtDeltaLog)
        commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(7)))
        val actualIncrementalMetrics = commitCheckpoint(amtDeltaLog, incremental = true)
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numRootEntriesExistingStatus = 4))
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numRootEntriesExistingStatus = 8))
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numNewLeaves = 1,
            numRootEntriesExistingStatus = 1,
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numNewLeaves = 3,
            numRootEntriesAddedStatus = 0,
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

  test("C2: MDV masking applies independently across two distinct leaves") {
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

  test("C3: deleting two files from the SAME leaf adds two bits to one leaf") {
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

  test("C4: multiple deferred intermediate commits accumulate all their leaf removes in the MDV") {
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

  test("C5: consecutive incremental AMT deletes accumulate MDV bits across writes") {
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

  test("C8: an untouched sibling leaf keeps an empty MDV while another is masked") {
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
          baselineDeltaLog,
          amtDeltaLog,
          createIncrementalAMTWriteMetrics(
            numOldLeavesUntouched = numLeafCountBefore,
            numLeafMdvBitsAdded = 0,
            numLeavesExistingStatus = numLeafCountBefore))
      }
    }
  }

  test("D3: a leaf file removed then re-added at the same path is masked once") {
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

  test("D4: a leaf file re-added and removed again is masked once, not double-counted") {
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

  test("D5: a re-add at a different path leaves both files live, no double-count") {
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
            numRootEntriesExistingStatus = businessCommits,
            numLeavesExistingStatus = numLeafCountBefore),
          expectedNumIntermediateCommits = Some(businessCommits + 1))
      }
    }
  }

  test("D9: replay re-derives the root across a chain of incremental AMTs") {
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

  test("D10: a long mixed chain of writes folds into ONE incremental AMT") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")

        // The opposite packing of D9: NOTHING is checkpointed until the very end, so instead of
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

  test("D11: re-adding an already-live file leaves it live exactly once, no double-count") {
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

  /////////////////////////////////////////////////////////////
  // Section-G:                                              //
  //     The inline emission path: statuses, tombstones,     //
  //      and DELETE vs REPLACE                              //
  /////////////////////////////////////////////////////////////

  test("G1: an inline write emits an incremental AMT that describes its own commit version") {
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

  test("G2: an inline-appended file is ADDED, then EXISTING once carried by the next commit") {
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

  test("G3: an inline delete of a leaf-resident file masks it via a cumulative MDV") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        val leavesBefore = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30).map(_.location).toSet
        assert(leavesBefore.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leavesBefore.size} leaves.")
        val physicalEntriesBefore = currentLeafDataEntries(amtDeltaLog.update())
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
        assert(currentLeafDataEntries(amtDeltaLog.update()) == physicalEntriesBefore,
          "The carried leaf keeps every physical entry; a delete only sets the MDV.")
      }
    }
  }

  test("G4: an inline delete of a root-resident file writes a root tombstone for CDF") {
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

        // Inline delete of the root-resident file. Deferred D1 drops such a file through replay
        // with NO tombstone; inline instead has the remove in actionsToCommit, so it becomes a
        // tracking=removed root entry for CDF.
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

  test("G5: an inline leaf delete stamps deleted_positions, and resets it on the next commit") {
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


  test("G6: an inline leaf-resident REPLACE stamps replaced_positions and re-adds MODIFIED") {
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

  test("G7: an inline root-resident REPLACE writes a REPLACED root entry, not DELETED") {
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
        // is MODIFIED -- unlike G4, where a pure root delete leaves a DELETED tombstone.
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

  test("G8: a root DATA entry goes ADDED -> EXISTING -> MODIFIED across incremental writes") {
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

  test("G9: a freshly spilled leaf can have ADDED/MODIFIED/EXISTING and its manifest_info " +
    "counts partition its entries") {
    // A cap of 10 entries per leaf: 20 files pack into 2 whole leaves, and the 8 live entries this
    // commit produces spill whole into one new leaf.
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        // Old tree: 5 root-resident DATA entries over 2 leaves (files 1..20 packed into 2 whole
        // leaves, then files 21..25 appended root-resident under the cap of 10).
        val oldLeafLocations = setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 20, initialIdRangeInRoot = 21 to 25).map(_.location).toSet
        assert(rootDataEntryStatusToCount(amtDeltaLog).values.sum == 5,
          "the old root must hold exactly the 5 root-resident DATA entries.")

        // A deferred (window) log commit appends files 26..28; not checkpointed, so they enter the
        // next incremental write as intermediate window commits.
        val windowIds = 26 to 28
        commitBoth(baselineDeltaLog, amtDeltaLog, windowIds.map(fakeAdd))

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
        val netNewAdd = Seq(fakeAdd(29))
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

        // manifest_info collapses ADDED + MODIFIED into added_files_count; EXISTING stands alone,
        // and a live spilled leaf carries no tombstone counts.
        val mi = spilledLeaf.manifest_info
        assert(mi.added_files_count == 7 && mi.existing_files_count == 2 &&
          mi.deleted_files_count == 0 && mi.replaced_files_count == 0,
          s"leaf manifest_info must be added=7 (1 ADDED + 6 MODIFIED), existing=2, no " +
            s"tombstones; got $mi.")
        assert(mi.added_files_count + mi.existing_files_count == spilledLeaf.record_count.toInt,
          s"added + existing must partition the leaf's ${spilledLeaf.record_count} entries.")
      }
    }
  }

  test("G10: an inline commit deleting one leaf file and replacing another on the same leaf") {
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

  test("G11: an overflowing tombstone leaf is born ADDED, decays to DELETED, then is dropped") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = 31 to 37)
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
          tombstoneLeaf.manifest_info.existing_files_count == 0,
          s"the tombstone leaf holds no live entries; got ${tombstoneLeaf.manifest_info}.")
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

  test("G12: a spilled tombstone leaf counts a REPLACED + DELETED mix") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10,
          initialIdRangeInLeaf = 1 to 30, initialIdRangeInRoot = 31 to 37)
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
        assert(mi.added_files_count == 0 && mi.existing_files_count == 0,
          s"the tombstone leaf holds no live entries; got $mi.")
        assertLiveAddFilesEquals(baselineDeltaLog, amtDeltaLog)
      }
    }
  }

  test("G13: one inline write spills live leaves and a tombstone leaf, all born ADDED") {
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

  test("G14: an inline same-key re-add of an already-live file with dataChange=false is allowed") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        setup(baselineDeltaLog, amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        val livePathsBefore = livePathsInLatestAMTCheckpoint(amtDeltaLog)
        // The allowed counterpart to H4 and the happy path of invariant (3): ComputeStats
        // re-commits a currently-live leaf file with recomputed stats and dataChange=false. It
        // stays EXISTING and the file is surfaced exactly once -- one EXISTING root entry, its old
        // leaf slot MDV-masked.
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

  test("G15: a window then inline same-key re-add (dataChange=false) of a leaf and a root file, " +
      "the inline actions carrying back references, keeps both EXISTING") {
    assertWindowThenInlineSameKeyReAddStaysExisting(dropInlineBackReference = false)
  }

  test("G16: a window then inline same-key re-add (dataChange=false) where the inline leaf " +
      "action carries no back reference still keeps both EXISTING (recognized via the live set)") {
    assertWindowThenInlineSameKeyReAddStaysExisting(dropInlineBackReference = true)
  }

  test("G17: a restore round-trip re-adds removed leaf and root files and drops the interim " +
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
  gridTest("G18: a mixed old root plus multi-source deletes across intermediate commits" +
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

  /////////////////////////////////////////////////////////////
  // Section-H:                                              //
  //     Invariant enforcement: rejecting commits that       //
  //      break a write- or commit-shape invariant           //
  /////////////////////////////////////////////////////////////

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
      actionsToCommit = actionsToCommit)

  test("H1: writeIncremental rejects intermediate commits with a hole up to attemptVersion") {
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

  test("H2: an inline no-backref remove with no originating AddFile is rejected") {
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

  test("H3: an inline same-key re-add of an already-live file with dataChange=true is rejected") {
    withSQLConf(leafPackingConfs: _*) {
      withTables() { (baselineDeltaLog, amtDeltaLog) =>
        commitBoth(baselineDeltaLog, amtDeltaLog, (1 to leafPackedFiles).map(fakeAdd))
        commitCheckpoint(amtDeltaLog, incremental = false)
        val leafToAddFileMapping = leafToAddFileMap(amtDeltaLog)
        assert(leafToAddFileMapping.size >= 2,
          s"Need a tree-shaped bootstrap; got ${leafToAddFileMapping.size} leaves.")
        // Re-commit a currently-live leaf file under its SAME (path, dv) key -- carrying the leaf
        // back reference it was reconstructed with -- in one inline commit with dataChange = true.
        // A same-key re-add of an already-live file is a metadata-only refresh (dataChange = false,
        // as in D11); a data-changing re-add is rejected by the incremental writer.
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

  test("H4: (1) a dataChange=true commit rejects a same-key re-add of an already-live file") {
    val ex = intercept[IllegalStateException] {
      processedActions(
        oldRootAdds = Seq(fakeAdd(1)),
        actionsToCommit = Seq(fakeAdd(1, dataChange = true)))
    }
    assert(ex.getMessage.contains("dataChange=true is not allowed"),
      s"expected invariant (1); got: ${ex.getMessage}")
  }

  test("H5: (2) a dataChange=false commit with removes rejects a re-add of an already-live file") {
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

  test("H6: (3) a dataChange=false commit with no removes rejects a new (non-re-add) file") {
    val ex = intercept[IllegalStateException] {
      processedActions(
        oldRootAdds = Seq(fakeAdd(1)),
        actionsToCommit = Seq(fakeAdd(99, dataChange = false)))
    }
    assert(ex.getMessage.contains("must re-add only already-live files"),
      s"expected invariant (3); got: ${ex.getMessage}")
  }

  test("H7: the three legal commit shapes construct and classify their adds correctly") {
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

}
