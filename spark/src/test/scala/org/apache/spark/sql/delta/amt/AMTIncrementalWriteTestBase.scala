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

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

/** Shared helpers for AMTIncrementalWrite* suites. */
abstract class AMTIncrementalWriteTestBase extends AMTCheckpointTestBase {

  import testImplicits._

  /** A deterministic fake data file. Paths are unique per id so live sets are easy to reason on. */
  protected def fakeAdd(fileID: Int): AddFile = fakeAdd(fileID, dataChange = true)

  protected def fakeAdd(fileID: Int, dataChange: Boolean, numRecords: Long = 1L): AddFile =
    AddFile(
      path = f"part-$fileID%05d.parquet",
      partitionValues = Map.empty,
      size = 100L + fileID,
      modificationTime = 1000L + fileID,
      dataChange = dataChange,
      stats = s"""{"numRecords":$numRecords}""")

  /** Creates the non-AMT never-checkpointed baseline table and the AMT-backed subject table. */
  protected def createTables(
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
  protected def writeOperation: DeltaOperations.Operation =
    DeltaOperations.Write(SaveMode.Append)

  /** Commits the identical `actions` as a "WRITE" commit to both tables. */
  protected def commitBoth(
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
  protected def livePathsInLatestSnapshot(deltaLog: DeltaLog): Set[String] =
    liveAddFilesInLatestSnapshot(deltaLog).map(_.path).toSet

  /**
   * The live-file path set the AMT provider reconstructs from its manifest tree (root + leaves,
   * MDV / tracking.status honored).
   */
  protected def livePathsInLatestAMTCheckpoint(deltaLog: DeltaLog): Set[String] = {
    val provider = amtProvider(deltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    provider.loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null").select("add.path").as[String].collect().toSet
  }

  /** DATA-entry tracking statuses that mark a root-resident entry as a CDF tombstone. */
  protected val tombstoneTrackingStatuses = Set(Tracking.Status.Deleted, Tracking.Status.Replaced)

  /**
   * Physical DATA-row counts of a manifest parquet keyed by `tracking.status`.
   */
  protected def trackingStatusToAddFileCountMap(absManifestPath: String): Map[Int, Long] =
    withManifestDataEntries(Seq(absManifestPath)) { entries =>
      entries.groupBy(col("tracking.status").as("status")).count()
        .as[(Int, Long)].collect().toMap
    }

  /**
   * DATA-entry `location`s (each `add.path`, a file's unique id) in a manifest parquet, keyed by
   * `tracking.status`. Lets a test assert *which* file carries each status, not just the counts.
   */
  protected def trackingStatusToLocationsMap(absManifestPath: String): Map[Int, Set[String]] =
    withManifestDataEntries(Seq(absManifestPath)) { entries =>
      entries.select(col("tracking.status").as("status"), col("location"))
        .as[(Int, String)].collect()
        .groupBy(_._1).map { case (status, rows) => status -> rows.map(_._2).toSet }
    }

  /** Root-resident DATA-entry counts keyed by tracking.status (DATA_MANIFEST pointers excluded). */
  protected def rootDataEntryStatusToCount(amtDeltaLog: DeltaLog): Map[Int, Long] = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    trackingStatusToAddFileCountMap(
      provider.checkpointAction.contentRoot.getAbsolutePath(provider.tableRoot).toString)
  }

  /** Root-resident DATA-entry `location`s keyed by tracking.status (pointers excluded). */
  protected def rootDataEntryStatusToLocations(amtDeltaLog: DeltaLog): Map[Int, Set[String]] = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    trackingStatusToLocationsMap(
      provider.checkpointAction.contentRoot.getAbsolutePath(provider.tableRoot).toString)
  }

  /** The live AddFiles of a table's latest snapshot, for building real removes. */
  protected def liveAddFilesInLatestSnapshot(deltaLog: DeltaLog): Seq[AddFile] =
    liveAddFiles(deltaLog.update())

  /**
   * The leaf-resident live files of the AMT table grouped by their leaf's relative location, read
   * from the reconstructed AddFiles' back references. Root-resident live files (no back reference)
   * are excluded. Lets a scenario pick removes against the *actual* leaf assignment.
   */
  protected def leafToAddFileMap(amtDeltaLog: DeltaLog): Map[String, Seq[AddFile]] =
    liveAddFilesInLatestSnapshot(amtDeltaLog)
      .flatMap(add => add.backReference.map(br => br.manifest -> add))
      .groupBy(_._1).map { case (leaf, pairs) => leaf -> pairs.map(_._2) }

  /** Asserts the table's current AMT checkpoint describes exactly `expectedVersion`. */
  protected def assertCheckpointDescribesVersion(
      amtDeltaLog: DeltaLog, expectedVersion: Long): Unit = {
    val provider = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT"))
    assert(provider.checkpointAction.version == expectedVersion,
      s"Checkpoint must describe version $expectedVersion; " +
        s"got ${provider.checkpointAction.version}.")
  }

  /** A real remove for the live file with the given id, carrying its stamped back reference. */
  protected def removeOf(amtDeltaLog: DeltaLog, fileID: Int): RemoveFile = {
    val path = fakeAdd(fileID).path
    val add = liveAddFilesInLatestSnapshot(amtDeltaLog).find(_.path == path)
      .getOrElse(fail(s"fileID=$fileID ($path) is not live in the AMT table."))
    add.remove
  }

  /**
   * A descriptor-only synthetic deletion vector named `name` (a 'p'-type path descriptor pointing
   * at no real bytes).
   */
  protected def syntheticDv(name: String): DeletionVectorDescriptor =
    DeletionVectorDescriptor.onDiskWithAbsolutePath(
      // A `p` DV path must parse as an absolute URI (scheme required); the file is never read.
      path = s"file:/$name", sizeInBytes = 5, cardinality = 5L, offset = Some(1))

  /**
   * Generates a Remove of the current file path (with whatever DV it has) plus an Add of the same
   * file under a given DV. E.g. if the table holds file f1 with DV dv4, this returns a remove of
   * (f1, dv4) and an add of (f1, given_dv).
   */
  protected def removeAndReAddWithDV(
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
  protected def leafToLeafMDVCardinalityMap(amtDeltaLog: DeltaLog): Map[String, Long] = {
    val provider = amtProvider(amtDeltaLog.update())
      .getOrElse(fail("AMT table must be checkpoint-provider-backed."))
    provider.leaves.map(l => l.location -> l.manifest_info.dv_cardinality.getOrElse(0L)).toMap
  }

  /** Root-resident DATA-entry counts (live adds, tombstones) read straight off the new root. */
  protected def liveAddsAndTombstonesCountInRoot(amtDeltaLog: DeltaLog): (Long, Long) = {
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
  protected def leafDeletedPositions(leaf: DataManifestEntry): Set[Long] =
    leaf.tracking.deleted_positions
      .map(RoaringBitmapArray.readFrom(_).toArray.toSet).getOrElse(Set.empty)

  /** The per-commit CDF `tracking.replaced_positions` off a leaf pointer (empty if unset). */
  protected def leafReplacedPositions(leaf: DataManifestEntry): Set[Long] =
    leaf.tracking.replaced_positions
      .map(RoaringBitmapArray.readFrom(_).toArray.toSet).getOrElse(Set.empty)

  /** The current snapshot's leaf pointers, keyed by relative location. */
  protected def leafPointers(snapshot: Snapshot): Map[String, DataManifestEntry] = {
    val provider = amtProvider(snapshot).getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
    provider.leaves.map(l => l.location -> l).toMap
  }

  /** The MDV cardinality numLeafCountBefore on a leaf pointer, 0 when it has none. */
  protected def mdvCardinality(leaf: DataManifestEntry): Long =
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
  protected def createIncrementalAMTAndValidate(
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
        commitIncrementalCheckpointAndReturnMetrics(amtDeltaLog)
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
  protected def assertCarriedLeafCountsImmutable(
      oldLeaves: Map[String, DataManifestEntry], amtDeltaLog: DeltaLog): Unit = {
    def fileAndRowCounts(
        e: DataManifestEntry): (Int, Int, Int, Int, Int, Long, Long, Long, Long, Long) =
      (e.manifest_info.added_files_count, e.manifest_info.existing_files_count,
        e.manifest_info.deleted_files_count, e.manifest_info.replaced_files_count,
        e.manifest_info.modified_files_count,
        e.manifest_info.added_rows_count, e.manifest_info.existing_rows_count,
        e.manifest_info.deleted_rows_count, e.manifest_info.replaced_rows_count,
        e.manifest_info.modified_rows_count)
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
  protected def assertIncrementalAMTWriteMetrics(
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
  protected def assertMetricsMatchTreeDelta(
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
  protected def assertLiveAddFilesEquals(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog): Unit = {
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
  protected def assertTreeInvariants(amtDeltaLog: DeltaLog): Unit = {
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
  protected def withTables(amtTableLocation: Option[String] = None)(
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
  protected def createIncrementalAMTWriteMetrics(
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
  protected def setup(
      baselineDeltaLog: DeltaLog,
      amtDeltaLog: DeltaLog,
      entriesPerLeaf: Int,
      initialIdRangeInLeaf: Range,
      initialIdRangeInRoot: Range = Range(0, 0),
      rowCountPerDataEntry: Long = 1L): Seq[DataManifestEntry] = {
    require(initialIdRangeInLeaf.size > entriesPerLeaf,
      s"initialIdRangeInLeaf (${initialIdRangeInLeaf.size}) must exceed entriesPerLeaf " +
        s"($entriesPerLeaf).")
    val numLeaves = math.ceil(initialIdRangeInLeaf.size.toDouble / entriesPerLeaf).toInt
    require(initialIdRangeInRoot.size <= entriesPerLeaf - numLeaves,
      s"initialIdRangeInRoot (${initialIdRangeInRoot.size}) must be <= " +
        s"${entriesPerLeaf - numLeaves} (entriesPerLeaf minus $numLeaves leaf pointers) so the " +
        "root-resident adds do not spill.")
    def adds(ids: Range): Seq[AddFile] =
      ids.map(id => fakeAdd(id, dataChange = true, numRecords = rowCountPerDataEntry))
    commitBoth(baselineDeltaLog, amtDeltaLog, adds(initialIdRangeInLeaf))
    commitCheckpoint(amtDeltaLog, incremental = false)
    assert(leafPointers(amtDeltaLog.update()).size == numLeaves,
      s"the full checkpoint must pack $numLeaves leaves.")
    if (initialIdRangeInRoot.nonEmpty) {
      commitBoth(baselineDeltaLog, amtDeltaLog, adds(initialIdRangeInRoot))
      commitCheckpoint(amtDeltaLog, incremental = true)
      assert(leafPointers(amtDeltaLog.update()).size == numLeaves,
        "the root-resident adds must not spill any leaf.")
    }
    leafPointers(amtDeltaLog.update()).values.toSeq
  }

  /**
   * Bootstraps a leafless full AMT: a single live file clusters into one manifest, which the full
   * rewrite promotes to the root. Returns after asserting the tree really has no leaf pointers.
   */
  protected def fullCheckpointPromotedToRoot(
      baselineDeltaLog: DeltaLog, amtDeltaLog: DeltaLog): Unit = {
    commitBoth(baselineDeltaLog, amtDeltaLog, Seq(fakeAdd(1)))
    commitCheckpoint(amtDeltaLog, incremental = false)
    val leafCount = amtProvider(amtDeltaLog.update()).getOrElse(fail("expected AMT")).leaves.size
    assert(leafCount == 0,
      s"A single-manifest full rewrite must be promoted to the root; got $leafCount leaves.")
  }
}
