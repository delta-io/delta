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

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{DeltaFileProviderUtils, DeltaLog, SingleCommit}
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, DomainMetadata, FileAction, InMemoryLogReplay, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession

/**
 * Writes an *incremental* AMT on top of an existing AMT without rewriting a single existing leaf.
 *
 * The new root is built from three parts:
 *   [        Part-1        ][             Part-2              ][      Part-3      ]
 *   [       old AMT        ][ -- log commits / minor          ][ this commit's    ]
 *   [       root           ][    compactions after it --      ][ actions          ]
 *
 * Algorithm:
 * - Take [[DataEntry]] from old root (part-1) + logCommits since then until attemptVersion
 *   (part-2) + commitActions that we want to commit (part-3) -- the logReplay of above becomes
 *   [[DataEntry]]s for the new AMT root.
 * - The previous tree's leaves ([[DataManifestEntry]]s) are carried forward by pointer.
 * - The [[AddFile]] / [[RemoveFile]] actions from part-2 / part-3 that have backreferences
 *   contribute towards MDVs for existing leafs.
 * - Tombstones:
 *   - The RemoveFile from the current proposed commit (part-3) contributes for CDF.
 *   - The one with backreferences contributes to deleted_positions / replaced_positions on the
 *     [[DataManifestEntry]] corresponding to existing leafs.
 *   - The one without backreferences contributes to a tombstone [[DataEntry]] in the new root /
 *     spilled leaves.
 * - If the root crosses the maxEntriesPerLeaf threshold, live [[DataEntry]]s / tombstone
 *   [[DataEntry]]s are spilled to a new leaf.
 *
 * [[DataEntry]] tracking.status is set to reflect THIS commit's CDF: only this commit's own actions
 * contribute ADDED (inserts), DELETED (deletes), and REPLACED + MODIFIED (updates -- the prior and
 * new copies of a re-added file). A file carried forward untouched is EXISTING and contributes no
 * CDF. Window (intermediate) commits already emitted their CDF, so their files carry forward as
 * EXISTING, not re-attributed to this commit.
 *
 * [[DataManifestEntry]] tracking.status state transitions (re-derived on every carry-forward to a
 * new AMT, from the masking the leaf gains that commit -- prior live status does not matter):
 * - A new leaf is always born ADDED, whether it holds spilled live [[DataEntry]]s or only spilled
 *   tombstones (a tombstone-only leaf holds no live file, but is still born ADDED).
 * - ADDED ->
 *   - EXISTING -- the leaf still holds a live file and gains no new masking this commit.
 *   - MODIFIED -- this commit masks some, but not all, of its live entries.
 *   - DELETED  -- the leaf holds no live file: this commit masks its last live entry, or it only
 *     ever held tombstones; the pointer becomes a tombstone.
 * - EXISTING ->
 *   - EXISTING / MODIFIED / DELETED -- same rule as ADDED, from this commit's masking.
 * - MODIFIED ->
 *   - MODIFIED / EXISTING / DELETED -- same rule as ADDED, from this commit's masking.
 * - DELETED is terminal: the tombstone pointer is emitted once, then dropped from the next AMT.
 */
class IncrementalAMTWriter(spark: SparkSession, deltaLog: DeltaLog) {

  private def entriesPerLeaf: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF)

  private val hadoopConf = deltaLog.newDeltaHadoopConf()
  private val tableRoot = deltaLog.dataPath
  // Always use object identity for AMT tables here.
  private val useDeletionVectorObjectIdentity = true
  private val fs = tableRoot.getFileSystem(hadoopConf)
  private val metadataDir = FileNames.amtMetadataDirPath(tableRoot)

  /**
   * Materializes the incremental manifest for `attemptVersion` and returns the inline
   * [[Checkpoint]] write result plus its metrics.
   *
   * @param oldAMTVersion             version the previous AMT checkpoint describes.
   * @param oldAMTCheckpointProvider  provider for the previous AMT (root, leaves, inline state).
   * @param intermediateLogCommits    the commit log files written after the old AMT and up to the
   *                                  last committed version (the window between the old AMT and
   *                                  this commit), in commit order.
   * @param attemptVersion            the version this commit targets.
   * @param actionsToCommit           this commit's actions (empty for a deferred OPTIMIZE
   *                                  CHECKPOINT).
   * @param trigger                   trigger name recorded in metrics.
   */
  def writeIncremental(
      oldAMTVersion: Long,
      oldAMTCheckpointProvider: AMTCheckpointProvider,
      intermediateLogCommits: Seq[FileStatus],
      attemptVersion: Long,
      actionsToCommit: Seq[Action],
      trigger: String): (AMTWriteResult, SingleAMTWriteMetrics) = {
    val startNanos = System.nanoTime()
    val oldCheckpoint = oldAMTCheckpointProvider.checkpointAction

    // ---- Step 0: validate the window is contiguous and lines up with our assumptions. ----
    // The intermediate commits after the old AMT must contiguously cover
    // [oldAMTVersion + 1, attemptVersion) with no holes.
    assertWindowCoversRange(intermediateLogCommits, oldAMTVersion, attemptVersion)

    // ---- Step 1: gather the actions of all three parts and process them. ----
    // 1.a: old AMT root -- its live root-resident files, plus its inline non-content state
    // (protocol, metadata, setTxns, domainMetadata). Leaf-resident files are NOT read here.
    val fileActionsFromOldRoot = AMTCheckpointProvider.readLiveRootDataEntries(
      deltaLog, oldCheckpoint)
    val nonContentFromOldCheckpoint: Seq[Action] =
      Seq[Action](oldCheckpoint.protocol, oldCheckpoint.metaData) ++
        oldCheckpoint.txns ++ oldCheckpoint.domainMetadata
    // 1.b: the log commits / minor compactions committed after the old AMT, read in parallel (the
    // MinorCompactionHook primitive). Each segment delta file becomes a SingleCommit keyed by its
    // version (a compacted delta's version is its endVersion, via getFileVersion).
    val windowCommits =
      intermediateLogCommits.map(f => SingleCommit(deltaLog, FileNames.getFileVersion(f), f))
    val actionsFromDeltas =
      DeltaFileProviderUtils.parallelReadAndParseDeltaFilesAsSeq(spark, windowCommits)
    // 1.c: this commit attempt's own actions (actionsToCommit).
    val processedActions = new ProcessedActions(
      oldAMTVersion = oldAMTVersion,
      oldRootAdds = fileActionsFromOldRoot,
      nonContentFromOldCheckpoint = nonContentFromOldCheckpoint,
      windowCommits = windowCommits,
      windowCommitActions = actionsFromDeltas,
      attemptVersion = attemptVersion,
      actionsToCommit = actionsToCommit,
      tableRoot = tableRoot,
      useDeletionVectorObjectIdentity = useDeletionVectorObjectIdentity)

    // ---- Step 2: carry the old leaf pointers forward, patching MDV + CDF positions. ----
    val (carriedLeafPointers, leafPositions) =
      carryForwardLeaves(
        oldAMTCheckpointProvider,
        processedActions.mdvSupersededBackrefs,
        processedActions.cdfDeletedBackrefs,
        processedActions.cdfReplacedBackrefs)

    // ---- Step 3: build the root DATA entries (with per-file status). ----
    // 3.a: live files -- classify each by whether THIS commit changed it:
    // - ADDED if inserted by this commit (not live before it).
    // - MODIFIED if replaced (re-added under a new DV) this commit.
    // - else EXISTING -- carried from the old root, or a window (intermediate) commit, whose own
    //   CDF already fired, so this commit proposes no change to it.
    val liveAddFiles = processedActions.liveAddFiles
    // A re-add of a file already live before this commit (carries a leaf back reference, or its key
    // is in the pre-commit live set; not a replace) is a metadata-only refresh: it stays EXISTING
    // and its old leaf slot is MDV-masked (ProcessedActions asserts the re-add invariants).
    def trackingForLiveAdd(add: AddFile): Tracking = {
      if (processedActions.replacedPaths.contains(add.path)) {
        AMTWriteHelper.modifiedTrackingForDataEntry()
      } else if (add.backReference.isDefined ||
          processedActions.preCommitLiveKeys.contains(
            add.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity))) {
        AMTWriteHelper.existingTrackingForDataEntry()
      } else {
        AMTWriteHelper.addedTrackingForDataEntry()
      }
    }
    val liveEntries =
      liveAddFiles.map(add => DataEntry.fromAddFile(add, trackingForLiveAdd(add), tableRoot))
    // 3.b: root remove entries (for CDF) -- this commit's net-removed no-backref files, built from
    // the matching AddFile (old root + window) so CDF gets full stats.
    val removeEntries = buildRootRemoveEntries(
      processedActions.cdfNoBackrefRemoves, liveAddFiles, processedActions.rootAndWindowAdds,
      processedActions.commitAddedPaths)

    // ---- Step 4: spill entries into new leaves if the root would exceed the per-leaf cap. ----
    val fixedRootCount = carriedLeafPointers.size
    val (rootLiveEntries, rootRemoveEntries, spilledLeafPointers) =
      spillIfNeeded(liveEntries, removeEntries, fixedRootCount,
        processedActions.postCommitMetadata, processedActions.postCommitProtocol)
    val allLeafPointers = carriedLeafPointers ++ spilledLeafPointers

    // ---- Step 5: write the new root. The post-commit metadata and protocol shape the persisted
    // manifest schema (the Iceberg partition struct and the typed per-column content stats), so
    // every manifest write needs them.
    val rootRows =
      allLeafPointers.map(_.wrap) ++
        rootLiveEntries.map(_.wrap) ++
        rootRemoveEntries.map(_.wrap)
    // The version the tree describes: an inline commit describes itself; a deferred OPTIMIZE
    // CHECKPOINT (no user actions) describes the last committed version (attemptVersion - 1).
    val contentStateVersion =
      if (actionsToCommit.isEmpty) attemptVersion - 1 else attemptVersion
    val contentRootBase = AMTWriteHelper.writeRoot(
      spark, fs, hadoopConf, tableRoot, metadataDir, processedActions.postCommitMetadata,
      processedActions.postCommitProtocol, rootRows,
      version = contentStateVersion)

    // ---- Step 6: generate the Checkpoint action. ----
    // An incremental rewrite carries forward the previous tree's last-full-rewrite marker.
    val lastFullRewriteVersion = oldCheckpoint.contentRoot
      .lastManifestCommitWithFullRewrite.getOrElse(contentStateVersion)
    val contentRoot = ContentRoot(
      path = contentRootBase.path,
      sizeInBytes = contentRootBase.sizeInBytes,
      version = contentStateVersion,
      isIncremental = true,
      lastManifestCommitWithFullRewrite = lastFullRewriteVersion,
      numLeaves = allLeafPointers.size.toLong)
    val checkpoint = Checkpoint(
      version = contentStateVersion,
      contentRoot = contentRoot,
      protocol = processedActions.postCommitProtocol,
      metaData = processedActions.postCommitMetadata,
      domainMetadata = processedActions.domainMetadatas,
      txns = processedActions.transactions,
      sidecars = Seq.empty)
    val result = AMTWriteResult(
      contentRootVersion = contentStateVersion,
      checkpoint = checkpoint,
      leaves = allLeafPointers,
      includeActionsInCommitJson = true)
    val numOldLeavesUpdated = carriedLeafPointers.count(p =>
      leafPositions.newMDVPositionsByLeaf.getOrElse(p.location, Set.empty[Long]).nonEmpty)
    // Per-status breakdown over every leaf pointer in the new tree (carried + newly spilled).
    val leavesByStatus = allLeafPointers.groupBy(_.tracking.status).map {
      case (status, ps) => status -> ps.size
    }
    val numStaleDeletedLeavesDropped =
      oldAMTCheckpointProvider.leaves.count(_.tracking.status == Tracking.Status.Deleted)
    def positionCount(byLeaf: Map[String, Set[Long]]): Int = byLeaf.valuesIterator.map(_.size).sum
    // Per-status breakdown over the new tree's root-resident DATA entries (live + remove entries).
    val rootEntriesByStatus = (rootLiveEntries ++ rootRemoveEntries)
      .groupBy(_.tracking.status).map { case (status, es) => status -> es.size }
    val incrementalWriteMetrics = IncrementalAMTWriteMetrics(
      numIntermediateCommits = intermediateLogCommits.size,
      numOldLeavesUpdated = numOldLeavesUpdated,
      numOldLeavesUntouched = carriedLeafPointers.size - numOldLeavesUpdated,
      numNewLeaves = spilledLeafPointers.size,
      numRootEntriesAddedStatus = rootEntriesByStatus.getOrElse(Tracking.Status.Added, 0),
      numRootEntriesExistingStatus = rootEntriesByStatus.getOrElse(Tracking.Status.Existing, 0),
      numRootEntriesModifiedStatus = rootEntriesByStatus.getOrElse(Tracking.Status.Modified, 0),
      numRootEntriesReplacedStatus = rootEntriesByStatus.getOrElse(Tracking.Status.Replaced, 0),
      numRootEntriesDeletedStatus = rootEntriesByStatus.getOrElse(Tracking.Status.Deleted, 0),
      numLeafMdvBitsAdded = positionCount(leafPositions.newMDVPositionsByLeaf),
      numLeafDeleteCDFBitsAdded = positionCount(leafPositions.deleteCDFPositionByLeaf),
      numLeafReplaceCDFBitsAdded = positionCount(leafPositions.replaceCDFPositionByLeaf),
      numLeavesAddedStatus = leavesByStatus.getOrElse(Tracking.Status.Added, 0),
      numLeavesExistingStatus = leavesByStatus.getOrElse(Tracking.Status.Existing, 0),
      numLeavesModifiedStatus = leavesByStatus.getOrElse(Tracking.Status.Modified, 0),
      numLeavesDeletedStatus = leavesByStatus.getOrElse(Tracking.Status.Deleted, 0),
      numStaleDeletedLeavesDropped = numStaleDeletedLeavesDropped)
    val metric = SingleAMTWriteMetrics(
      trigger = trigger,
      // This writer only ever produces an incremental tree.
      incremental = "true",
      materializeDurationMs = NANOSECONDS.toMillis(System.nanoTime() - startNanos),
      incrementalWriteMetrics = Some(incrementalWriteMetrics))
    (result, metric)
  }

  /**
   * Carries the previous tree's leaf pointers forward. Three differently-scoped decisions apply
   * per pointer:
   *   - a pointer the previous AMT already marked DELETED is dropped, not carried forward: it was
   *     kept only to emit that commit's CDF, and re-emitting it here would produce wrong CDF;
   *   - `manifest_info.dv` (MDV, masking) accumulates ALL leaf positions removed since the old AMT
   *     (`mdvRemoves` = window + commit), so the reader hides every entry deleted since the last
   *     full tree; and
   *   - `tracking.{deleted_positions / replaced_positions}` (CDF) is RESET to just THIS commit's
   *     removed positions -- a window remove already emitted its CDF in its own commit, and the
   *     old pointer's stale positions must not carry forward, so this is cleared on every pointer
   *     and re-set only for leaves this commit touched.
   * Leaf parquet files are never re-read or rewritten.
   *
   * Returns the carried-forward pointers and an [[MDVAndCDFPositions]] that has been set per-leaf.
   */
  private def carryForwardLeaves(
      provider: AMTCheckpointProvider,
      mdvSupersededBackrefs: Seq[BackReference],
      cdfDeletedBackrefs: Seq[BackReference],
      cdfReplacedBackrefs: Seq[BackReference])
      : (Seq[DataManifestEntry], MDVAndCDFPositions) = {
    // A (leaf, position) can be superseded multiple times, e.g. a leaf file removed, re-added and
    // removed again. We use a Set to dedupe the positions so the count matches the number of bits
    // gained by the MDV or CDF bitmap.
    def positionsByLeaf(backrefs: Seq[BackReference]): Map[String, Set[Long]] =
      backrefs.map(br => br.manifest -> br.pos)
        .groupBy(_._1).map { case (leaf, pairs) => leaf -> pairs.map(_._2).toSet }
    val newMDVPositionsByLeaf = positionsByLeaf(mdvSupersededBackrefs)
    val deletedPositionsByLeaf = positionsByLeaf(cdfDeletedBackrefs)
    val replacedPositionsByLeaf = positionsByLeaf(cdfReplacedBackrefs)
    val pointers = provider.leaves.flatMap { pointer =>
      if (pointer.tracking.status == Tracking.Status.Deleted) None
      else {
        val newMdvPositions =
          newMDVPositionsByLeaf.getOrElse(pointer.location, Set.empty[Long]).toSeq
        val deletedPositions =
          deletedPositionsByLeaf.getOrElse(pointer.location, Set.empty[Long]).toSeq
        val replacedPositions =
          replacedPositionsByLeaf.getOrElse(pointer.location, Set.empty[Long]).toSeq
        Some(carryForwardOneLeaf(pointer, newMdvPositions, deletedPositions, replacedPositions))
      }
    }
    (pointers,
      MDVAndCDFPositions(newMDVPositionsByLeaf, deletedPositionsByLeaf, replacedPositionsByLeaf))
  }

  // Visible for testing.
  private[amt] def carryForwardOneLeaf(
      pointer: DataManifestEntry,
      newMdvPositions: Seq[Long],
      deletedPositions: Seq[Long],
      replacedPositions: Seq[Long]): DataManifestEntry = {
    val liveFileCount = pointer.manifest_info.liveFilesCount
    val tombstoneFileCount = pointer.manifest_info.tombstoneFilesCount
    if (liveFileCount > 0 && tombstoneFileCount > 0) {
      throw new IllegalStateException(
        "Leaves having mix of live files and tombstones are not supported yet")
    }
    if (newMdvPositions.nonEmpty && liveFileCount == 0) {
      throw new IllegalStateException(
        s"Leaf ${pointer.location} holds no live file but gained new MDV positions.")
    }
    val (tracking, manifestInfo) =
      if (tombstoneFileCount > 0) {
        // A carried leaf with no live file -- a tombstone-only leaf born ADDED last commit --
        // decays to DELETED so the next AMT drops it.
        AMTWriteHelper.deletedTrackingForCarriedLeaf(pointer)
      } else {
        // this case means tombstoneFileCount = 0 and liveFileCount >= 0
        // In this scenario, mark the leaf as DELETED if any of below is true:
        // a) all live files are marked for deletion in this commit (with newMdvPositions) OR
        // b) all the live files were already striked off in previous commit itself but the
        //    leaf was not marked as DELETED then (this is not done by our implementation but
        //    could be done by external engine).
        if (newMdvPositions.nonEmpty) {
          // The MDV grew this commit: MODIFIED, or DELETED once every live entry is masked.
          AMTWriteHelper.modifiedOrDeletedTrackingForLeaf(
            pointer, newMdvPositions, deletedPositions, replacedPositions)
        } else if (pointer.manifest_info.dv_cardinality.getOrElse(0L) == pointer.record_count) {
          AMTWriteHelper.deletedTrackingForCarriedLeaf(pointer)
        } else {
          // Untouched leaf (no new MDVs) that still holds live files: carry it forward EXISTING.
          AMTWriteHelper.existingTrackingForLeaf(pointer)
        }
      }
    pointer.copy(manifest_info = manifestInfo, tracking = tracking)
  }

  /**
   * Builds root-resident entries for THIS commit's net-removed no-backref files, so Change Data
   * Feed can recover them. (Window actions already emitted their CDF in their own commits, so they
   * are not passed here.)
   *   - Each entry is built from the removed file's `AddFile` (found among root + window adds) --
   *     NOT from the sparse `RemoveFile` -- so it carries full stats / DV.
   *   - A `RemoveFile` for a file ALSO re-added this commit under a new DV yields a REPLACED
   *     `DataEntry` (its prior state).
   *   - A `RemoveFile` with no matching re-add for the same path (no DV change -- the full
   *     `(path, dv)` pair is removed) yields a DELETED `DataEntry`.
   */
  private def buildRootRemoveEntries(
      withoutBackrefRemoves: Seq[RemoveFile],
      liveAdds: Seq[AddFile],
      rootAndWindowAdds: Seq[AddFile],
      commitAddedPaths: Set[String]): Seq[DataEntry] = {
    if (withoutBackrefRemoves.isEmpty) return Seq.empty
    val liveKeys: Set[UniqueFileActionTuple] =
      liveAdds.iterator
        .map(_.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity))
        .toSet
    // AddFile lookup from root + window: the removed file's add predates this commit (a replace
    // re-adds the same path under a new DV -- a distinct key -- so this still finds the prior add).
    val addByKey: Map[UniqueFileActionTuple, AddFile] =
      rootAndWindowAdds
        .map(a => a.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity) -> a)
        .toMap
    withoutBackrefRemoves.map { r =>
      val removeKey = r.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity)
      // Two invariants a no-backref remove must satisfy (it targets a root-resident or
      // window-added file, never a leaf); violating either means a corrupt tree:
      //   - it is net-removed, so its (path, dv) is absent from the live set (a still-live key
      //     means a malformed add + remove of the same key in one commit); and
      //   - its originating add is present among root + window adds.
      if (liveKeys.contains(removeKey)) {
        throw new IllegalStateException(
          s"Net-removed no-backref file ${r.path} (key $removeKey) is still in the live set; " +
            "a file cannot be both removed and live in the same commit.")
      }
      val priorAddFile = addByKey.get(removeKey).getOrElse {
        throw new IllegalStateException(
          s"No originating AddFile for net-removed no-backref file ${r.path} (key " +
            s"$removeKey); cannot build its CDF root entry.")
      }
      val tracking =
        if (commitAddedPaths.contains(r.path)) AMTWriteHelper.replacedTrackingForDataEntry()
        else AMTWriteHelper.removedTrackingForDataEntry()
      DataEntry.fromAddFile(priorAddFile, tracking, tableRoot)
    }
  }

  /**
   * If the root would exceed `entriesPerLeaf` (carried pointers + live entries + remove entries),
   * moves whole `entriesPerLeaf`-sized batches into new leaves until the root's row count fits --
   * live entries first, then remove (DELETED / REPLACED) entries. Each spill adds one leaf pointer
   * to the fixed root count. Returns the live entries and remove entries that remain root-resident,
   * plus the pointers for any new leaves.
   */
  private def spillIfNeeded(
      liveEntries: Seq[DataEntry],
      removeEntries: Seq[DataEntry],
      fixedRootCount: Int,
      metadata: Metadata,
      protocol: Protocol): (Seq[DataEntry], Seq[DataEntry], Seq[DataManifestEntry]) = {
    val spilled = ArrayBuffer.empty[DataManifestEntry]
    var remainingLive = liveEntries
    var remainingRemoves = removeEntries
    def rootRowCount: Int =
      fixedRootCount + spilled.size + remainingLive.size + remainingRemoves.size
    while (rootRowCount > entriesPerLeaf && remainingLive.nonEmpty) {
      val (batch, rest) = remainingLive.splitAt(entriesPerLeaf)
      spilled += AMTWriteHelper.writeLeaf(
        spark, fs, hadoopConf, tableRoot, metadataDir, metadata, protocol, batch)
      remainingLive = rest
    }
    while (rootRowCount > entriesPerLeaf && remainingRemoves.nonEmpty) {
      val (batch, rest) = remainingRemoves.splitAt(entriesPerLeaf)
      spilled += AMTWriteHelper.writeLeaf(
        spark, fs, hadoopConf, tableRoot, metadataDir, metadata, protocol, batch)
      remainingRemoves = rest
    }
    (remainingLive, remainingRemoves, spilled.toSeq)
  }

  // Asserts the intermediate commit files cover EXACTLY [oldAMTVersion+1, attemptVersion) -- no
  // holes and no versions outside the window (a minor-compacted delta covers its whole
  // [startV, endV] range).
  private def assertWindowCoversRange(
      intermediateLogCommits: Seq[FileStatus],
      oldAMTVersion: Long,
      attemptVersion: Long): Unit = {
    val covered = intermediateLogCommits.flatMap { f =>
      f.getPath match {
        case FileNames.CompactedDeltaFile(_, startV, endV) => startV to endV
        case _ => Seq(FileNames.deltaVersion(f))
      }
    }.toSet
    val expected = ((oldAMTVersion + 1) until attemptVersion).toSet
    assert(covered == expected,
      s"intermediateLogCommits must cover exactly [${oldAMTVersion + 1}, $attemptVersion); " +
        s"missing ${(expected -- covered).toList.sorted}, " +
        s"unexpected ${(covered -- expected).toList.sorted}.")
  }
}

private class ProcessedActions(
    oldAMTVersion: Long,
    oldRootAdds: Seq[AddFile],
    nonContentFromOldCheckpoint: Seq[Action],
    windowCommits: Seq[SingleCommit],
    windowCommitActions: Seq[Seq[Action]],
    attemptVersion: Long,
    actionsToCommit: Seq[Action],
    tableRoot: Path,
    useDeletionVectorObjectIdentity: Boolean) {
  private val replay = new InMemoryLogReplay(
    minFileRetentionTimestamp = None,
    minSetTransactionRetentionTimestamp = None,
    tableRoot = tableRoot,
    useDeletionVectorObjectIdentity = useDeletionVectorObjectIdentity)
  private val rootAndWindowAddsBuf = ArrayBuffer.empty[AddFile]
  private val mdvSupersededBuf = ArrayBuffer.empty[BackReference]
  // This commit's own file actions, split out for CDF and re-add classification.
  private val commitAddsBuf = ArrayBuffer.empty[AddFile]
  private val commitRemovesBuf = ArrayBuffer.empty[RemoveFile]

  // Log replay of part-1/2/3.
  replay.append(oldAMTVersion, oldRootAdds.iterator ++ nonContentFromOldCheckpoint.iterator)
  windowCommits.zip(windowCommitActions).foreach { case (commit, actions) =>
    replay.append(commit.version, actions.iterator)
  }
  // Keys of files live in the {old root + window}'s replay BEFORE this commit's actions; a
  // live add already here (or carrying a leaf back reference) is EXISTING, not ADDED.
  val preCommitLiveKeys: Set[UniqueFileActionTuple] =
    replay.allFiles.iterator
      .map(_.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity))
      .toSet
  replay.append(attemptVersion, actionsToCommit.iterator)

  // One more pass over part-1/2/3 to initialize the auxiliary buffers above.
  rootAndWindowAddsBuf ++= oldRootAdds
  windowCommits.zip(windowCommitActions).foreach { case (commit, actions) =>
    actions.foreach {
      case a: AddFile =>
        rootAndWindowAddsBuf += a
        a.backReference.foreach(mdvSupersededBuf += _)
      case r: RemoveFile => r.backReference.foreach(mdvSupersededBuf += _)
      case _ =>
    }
  }
  actionsToCommit.foreach {
    case a: AddFile =>
      commitAddsBuf += a
      a.backReference.foreach(mdvSupersededBuf += _)
    case r: RemoveFile =>
      commitRemovesBuf += r
      r.backReference.foreach(mdvSupersededBuf += _)
    case _ =>
  }

  /** The net-live files held directly in the new root. */
  val liveAddFiles: Seq[AddFile] = replay.allFiles
  /** Post-commit non-content metadata. */
  val postCommitProtocol: Protocol = replay.getProtocol.getOrElse(
    throw new IllegalStateException("Replay produced no protocol for the incremental AMT."))
  val postCommitMetadata: Metadata = replay.getMetadata.getOrElse(
    throw new IllegalStateException("Replay produced no metadata for the incremental AMT."))
  val domainMetadatas: Seq[DomainMetadata] = replay.getDomainMetadatas.toSeq
  val transactions: Seq[SetTransaction] = replay.getTransactions.toSeq

  /**
   * AddFiles resident in the old root + window: used to generate the tombstone DataEntries for the
   * RemoveFile actions in actionsToCommit (Part-3). The prior AddFile entries carry richer
   * information than the RemoveFile, so use them to construct the tombstone.
   */
  val rootAndWindowAdds: Seq[AddFile] = rootAndWindowAddsBuf.toSeq

  /**
   * Leaf positions superseded since the old AMT (window + commit), by a RemoveFile (deleted) or a
   * back-referenced AddFile (re-added -- the fresh copy lands in the root, so its original leaf
   * slot is stale). Masking both keeps a stale slot from surfacing.
   */
  val mdvSupersededBackrefs: Seq[BackReference] = mdvSupersededBuf.toSeq

  /** Paths this commit adds; a removed path also present here is a same-commit replace. */
  val commitAddedPaths: Set[String] = commitAddsBuf.iterator.map(_.path).toSet
  val replacedPaths: Set[String] =
    commitRemovesBuf.iterator.map(_.path).toSet.intersect(commitAddedPaths)

  /**
   * The commit's dataChange flag: the first FileAction's (Delta enforces one value per commit),
   * defaulting to true when the commit carries no file actions.
   */
  val dataChange: Boolean =
    actionsToCommit.collectFirst { case f: FileAction => f.dataChange }.getOrElse(true)

  // CDF is driven ONLY by this commit's removes; a backref remove is REPLACED when its path is
  // re-added here (under a new DV), else DELETED; a no-backref remove drives a root entry likewise.
  private def reAddedHere(r: RemoveFile): Boolean = commitAddedPaths.contains(r.path)
  val cdfReplacedBackrefs: Seq[BackReference] =
    commitRemovesBuf.iterator.filter(r => r.backReference.isDefined && reAddedHere(r))
      .map(_.backReference.get).toSeq
  val cdfDeletedBackrefs: Seq[BackReference] =
    commitRemovesBuf.iterator.filter(r => r.backReference.isDefined && !reAddedHere(r))
      .map(_.backReference.get).toSeq
  val cdfNoBackrefRemoves: Seq[RemoveFile] =
    commitRemovesBuf.iterator.filter(_.backReference.isEmpty).toSeq

  // ---- Commit-shape invariants, checked once at construction. ----
  // True if `add` re-commits a file already live before this commit (its key is in the pre-commit
  // live set, or it carries a back reference to an existing leaf slot) and is not a replace (a
  // replace changes the DV, a distinct key). Such a re-add is a metadata-only refresh.
  private def isReCommittedLiveAdd(add: AddFile): Boolean = {
    val preCommitLiveKeysContainAddFile = preCommitLiveKeys.contains(
      add.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity))
    !replacedPaths.contains(add.path) &&
      (add.backReference.isDefined || preCommitLiveKeysContainAddFile)
  }
  val reCommittedLiveAdd: Option[AddFile] = commitAddsBuf.find(isReCommittedLiveAdd)
  if (dataChange) {
    // (1) A data-changing commit must not re-add a file already live before it: a same-key re-add
    // is a metadata-only refresh, so it must carry dataChange=false.
    reCommittedLiveAdd.foreach { add =>
      throw new IllegalStateException(
        s"Re-adding already-live file ${add.path} with dataChange=true is not allowed; a " +
          "same-key re-add must be a metadata-only change (dataChange=false).")
    }
  } else if (commitRemovesBuf.nonEmpty) {
    // (2) A metadata-only commit that also removes files is a compaction (OPTIMIZE/REORG): its adds
    // are freshly written files, so none may re-add a file already live before it.
    reCommittedLiveAdd.foreach { add =>
      throw new IllegalStateException(
        s"A metadata-only commit that removes files must not re-add an already-live file, but " +
          s"${add.path} is re-added.")
    }
  } else {
    // (3) A metadata-only commit with no removes is a metadata refresh (e.g. stats/tags update):
    // every add must re-commit a file already live before it, under the same (path, dv) key.
    commitAddsBuf.find(a => !isReCommittedLiveAdd(a)).foreach { add =>
      throw new IllegalStateException(
        s"A metadata-only commit with no removes must re-add only already-live files, but " +
          s"${add.path} is a new file.")
    }
  }
}

private case class MDVAndCDFPositions(
    newMDVPositionsByLeaf: Map[String, Set[Long]],
    deleteCDFPositionByLeaf: Map[String, Set[Long]],
    replaceCDFPositionByLeaf: Map[String, Set[Long]])
