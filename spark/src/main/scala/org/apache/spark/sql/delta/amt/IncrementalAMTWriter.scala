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
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, ContentRoot, InMemoryLogReplay, RemoveFile}
import org.apache.spark.sql.delta.actions.InMemoryLogReplay.UniqueFileActionTuple
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.FileStatus

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
 * - The [[RemoveFile]] actions from part-2 / part-3 that have backreferences contribute towards
 *   MDVs for existing leafs.
 * - Tombstones:
 *   - The RemoveFile from the current proposed commit (part-3) contributes for CDF.
 *   - The one with backreferences contributes to deleted_positions on the [[DataManifestEntry]]
 *     corresponding to existing leafs.
 *   - The one without backreferences contributes to a tombstone [[DataEntry]] in the new root.
 * - If the root crosses the maxEntriesPerLeaf threshold, live [[DataEntry]]s are spilled to a new
 *   leaf.
 */
class IncrementalAMTWriter(spark: SparkSession, deltaLog: DeltaLog) {

  private def entriesPerLeaf: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF)

  private val hadoopConf = deltaLog.newDeltaHadoopConf()
  private val tableRoot = deltaLog.dataPath
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

    // ---- Step 1: gather the actions of all three parts. ----
    // 1.a: old AMT root -- its live root-resident files, plus its inline non-content state
    // (protocol, metadata, setTxns, domainMetadata). Leaf-resident files are NOT read here.
    val fileActionsFromOldRoot = AMTCheckpointProvider.readLiveRootDataEntries(
      spark, deltaLog, oldCheckpoint)
    val nonContentFromOldRoot: Iterator[Action] =
      Iterator(oldCheckpoint.protocol, oldCheckpoint.metaData) ++
        oldCheckpoint.txns.iterator ++ oldCheckpoint.domainMetadata.iterator
    // 1.b: the log commits / minor compactions committed after the old AMT, read in parallel (the
    // MinorCompactionHook primitive). Each segment delta file becomes a SingleCommit keyed by its
    // version (a compacted delta's version is its endVersion, via getFileVersion).
    val windowCommits =
      intermediateLogCommits.map(f => SingleCommit(deltaLog, FileNames.getFileVersion(f), f))
    val actionsFromDeltas =
      DeltaFileProviderUtils.parallelReadAndParseDeltaFilesAsSeq(spark, deltaLog, windowCommits)
    // 1.c: this commit attempt's own actions (actionsToCommit).

    // ---- Step 2: log-replay all three parts, keyed by their real commit versions. ----
    // The version handed to `append` feeds InMemoryLogReplay's monotonic-progress assert: the old
    // AMT root describes oldAMTVersion, each window delta carries its own commit version
    // (FileNames.getFileVersion; for a minor compaction that is a good proxy), and this commit
    // lands at attemptVersion. Because 1.a seeds the old inline non-content state and 1.b/1.c
    // override it (last-writer-wins), the replay's protocol/metadata/setTxns/domainMetadata equal
    // the post-commit values.
    val replay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None, minSetTransactionRetentionTimestamp = None)
    replay.append(
      oldAMTVersion,
      fileActionsFromOldRoot.iterator ++ nonContentFromOldRoot)
    windowCommits.zip(actionsFromDeltas).foreach { case (commit, actions) =>
      replay.append(commit.version, actions.iterator)
    }
    replay.append(attemptVersion, actionsToCommit.iterator)

    // ---- Step 3: gather the removes that drive MDV masking and CDF, from different scopes. ----
    val windowActions = actionsFromDeltas.flatten
    // 3.a: MDV masking -- ALL leaf-resident removes since the old AMT (window + commit). The MDV
    // must hide every entry deleted since the last full tree, so it accumulates the window's
    // removes too, regardless of which commit deleted them.
    // NOTE: this can NOT come from `replay.getActiveRemoveFiles`. Leaf files are not seeded into
    // the replay, and a leaf file removed then re-added within the window (e.g. RESTORE) has its
    // remove cancelled by the re-add there, so getActiveRemoveFiles would omit it -- yet its old
    // leaf entry still needs masking (the re-added copy lives in the root). We must mask by every
    // backref position deleted since the old AMT, which is exactly this scan.
    val mdvBackrefRemoves = (windowActions ++ actionsToCommit)
      .collect { case r: RemoveFile if r.backReference.isDefined => r }
    // 3.b: CDF -- ONLY this commit's removes. A window remove already emitted its CDF (deleted DV /
    // root tombstone) in its own commit; re-emitting here would double-count. So this commit's
    // with-backref removes drive tracking.deleted_positions on the touched leaf pointers, and its
    // without-backref removes drive the root tombstones.
    val commitRemoves = actionsToCommit.collect { case r: RemoveFile => r }
    val (cdfBackrefRemoves, cdfNoBackrefRemoves) =
      commitRemoves.partition(_.backReference.isDefined)

    // ---- Step 4: carry the old leaf pointers forward, patching MDV + deleted_positions. ----
    // Also returns the MDV positions this write added per leaf (keyed by relative location), reused
    // for the shape metrics below rather than recomputed.
    val (carriedLeafPointers, mdvPositionsByLeaf) =
      carryForwardLeaves(oldAMTCheckpointProvider, mdvBackrefRemoves, cdfBackrefRemoves)

    // ---- Step 5: build the root DATA entries. ----
    // 5.a: live files (added/existing) -- the net-new files, held directly in the root.
    val liveAdds = replay.allFiles
    // 5.b: removed tombstones (for CDF) -- net-removed no-backref files removed by THIS commit,
    // built from the matching AddFile (root + window) so CDF gets full stats, never from the sparse
    // RemoveFile.
    val rootAndWindowAdds = fileActionsFromOldRoot ++ windowActions.collect { case a: AddFile => a }
    val tombstoneEntries = buildTombstones(cdfNoBackrefRemoves, liveAdds, rootAndWindowAdds)

    // ---- Step 6: spill live adds into new leaves if the root would exceed the per-leaf cap. ----
    val fixedRootCount = carriedLeafPointers.size + tombstoneEntries.size
    val (rootAdds, spilledLeafPointers) =
      spillIfNeeded(liveAdds, fixedRootCount)

    // ---- Step 7: write the new root. ----
    val addedTracking = AMTWriteHelper.addedTracking
    val rootRows =
      (carriedLeafPointers ++ spilledLeafPointers).map(_.wrap) ++
        rootAdds.map(add => DataEntry.fromAddFile(add, addedTracking, tableRoot).wrap) ++
        tombstoneEntries.map(_.wrap)
    val contentRootBase =
      AMTWriteHelper.writeRoot(spark, fs, hadoopConf, tableRoot, metadataDir, rootRows)

    // ---- Step 8: generate the Checkpoint action. ----
    // The version the tree describes: an inline commit describes itself; a deferred OPTIMIZE
    // CHECKPOINT (no user actions) describes the last committed version (attemptVersion - 1).
    val contentStateVersion =
      if (actionsToCommit.isEmpty) attemptVersion - 1 else attemptVersion
    // An incremental rewrite carries forward the previous tree's last-full-rewrite marker.
    val lastFullRewriteVersion = oldCheckpoint.contentRoot
      .lastManifestCommitWithFullRewrite.getOrElse(contentStateVersion)
    val contentRoot = ContentRoot(
      path = contentRootBase.path,
      sizeInBytes = contentRootBase.sizeInBytes,
      isIncremental = true,
      lastManifestCommitWithFullRewrite = lastFullRewriteVersion)
    val postCommitProtocol = replay.getProtocol.getOrElse(
      throw new IllegalStateException("Replay produced no protocol for the incremental AMT."))
    val postCommitMetadata = replay.getMetadata.getOrElse(
      throw new IllegalStateException("Replay produced no metadata for the incremental AMT."))
    val checkpoint = Checkpoint(
      version = contentStateVersion,
      contentRoot = contentRoot,
      protocol = postCommitProtocol,
      metaData = postCommitMetadata,
      domainMetadata = replay.getDomainMetadatas.toSeq,
      txns = replay.getTransactions.toSeq,
      sidecars = Seq.empty)
    val allLeafPointers = carriedLeafPointers ++ spilledLeafPointers
    val result = AMTWriteResult(
      contentRootVersion = contentStateVersion,
      checkpoint = checkpoint,
      leaves = allLeafPointers,
      includeActionsInCommitJson = true)
    val numExistingLeavesUpdated = carriedLeafPointers.count(p =>
      mdvPositionsByLeaf.getOrElse(p.location, Set.empty[Long]).nonEmpty)
    val incrementalWriteMetrics = IncrementalAMTWriteMetrics(
      numIntermediateCommits = intermediateLogCommits.size,
      numExistingLeavesUpdated = numExistingLeavesUpdated,
      numExistingLeavesUntouched = carriedLeafPointers.size - numExistingLeavesUpdated,
      numNewLeaves = spilledLeafPointers.size,
      numRootLiveAdds = rootAdds.size,
      numRootTombstones = tombstoneEntries.size,
      numLeafMdvBitsAdded = mdvPositionsByLeaf.valuesIterator.map(_.size).sum)
    val metric = SingleAMTWriteMetrics(
      trigger = trigger,
      // This writer only ever produces an incremental tree.
      incremental = "true",
      materializeDurationMs = NANOSECONDS.toMillis(System.nanoTime() - startNanos),
      incrementalWriteMetrics = Some(incrementalWriteMetrics))
    (result, metric)
  }

  /**
   * Carries the previous tree's leaf pointers forward. Two independent, differently-scoped updates
   * are applied to each carried pointer:
   *   - `manifest_info.dv` (MDV, masking) accumulates ALL leaf positions removed since the old AMT
   *     (`mdvRemoves` = window + commit), so the reader hides every entry deleted since the last
   *     full tree; and
   *   - `tracking.deleted_positions` (CDF) is RESET to just THIS commit's removed positions
   *     (`cdfRemoves`) -- a window remove already emitted its CDF in its own commit, and the old
   *     pointer's stale positions must not carry forward, so this is cleared on every pointer and
   *     re-set only for leaves this commit touched.
   * Leaf parquet files are never re-read or rewritten.
   *
   * Returns the carried-forward pointers and the MDV positions this write added per leaf (keyed by
   * relative location), so the caller can derive the shape metrics without recomputing it.
   */
  private def carryForwardLeaves(
      provider: AMTCheckpointProvider,
      mdvRemoves: Seq[RemoveFile],
      cdfRemoves: Seq[RemoveFile]): (Seq[DataManifestEntry], Map[String, Set[Long]]) = {
    // A position is a SET member: the MDV and deleted_positions are both bitmaps, so removing
    // the same (leaf, position) twice -- e.g. a leaf file removed, re-added and removed again --
    // masks it once. Deduplicating here keeps numLeafMdvBitsAdded equal to the bits the MDV
    // actually gained.
    def positionsByLeaf(removes: Seq[RemoveFile]): Map[String, Set[Long]] =
      removes.flatMap(r => r.backReference.map(br => br.manifest -> br.pos))
        .groupBy(_._1).map { case (leaf, pairs) => leaf -> pairs.map(_._2).toSet }
    val mdvPositionsByLeaf = positionsByLeaf(mdvRemoves)
    val cdfPositionsByLeaf = positionsByLeaf(cdfRemoves)
    val pointers = provider.leaves
      .map { pointer =>
        val leafKey = pointer.location
        val newMdvPositions = mdvPositionsByLeaf.getOrElse(leafKey, Set.empty[Long])
        val cdfPositions = cdfPositionsByLeaf.getOrElse(leafKey, Set.empty[Long])
        // Cumulative MDV = old dv + every position removed from this leaf since the old AMT.
        val manifestInfo =
          if (newMdvPositions.isEmpty) {
            pointer.manifest_info
          } else {
            val cumulativeDV = pointer.manifest_info.dv
              .map(AMTUtils.deserializeMdv).getOrElse(new RoaringBitmapArray)
            newMdvPositions.foreach(cumulativeDV.add)
            AMTWriteHelper.withUpdatedMdv(pointer.manifest_info, cumulativeDV)
          }
        // A carried leaf pointer stays live (ADDED); only deleted_positions conveys CDF. It is
        // per-commit: reset to THIS commit's removed positions (empty when this commit did not
        // delete from this leaf), never the old pointer's stale value.
        val deletedPositions =
          if (cdfPositions.isEmpty) None
          else Some(AMTUtils.serializeMdv(RoaringBitmapArray(cdfPositions.toSeq: _*)))
        val tracking = AMTWriteHelper.addedTracking.copy(deleted_positions = deletedPositions)
        pointer.copy(manifest_info = manifestInfo, tracking = tracking)
      }
    (pointers, mdvPositionsByLeaf)
  }

  /**
   * Builds root-resident `tracking=removed` tombstones for THIS commit's net-removed no-backref
   * files, so Change Data Feed can recover them. (Window removes already emitted their CDF in their
   * own commits, so they are not passed here.) Each tombstone is built from the removed file's
   * `AddFile` (found among root + window adds, matched by the `(path, dvId)` key) -- NOT from
   * the sparse `RemoveFile` -- so it carries full stats/DV. A remove is a tombstone only when its
   * file is net-removed (not present in the replay's live set) and its `AddFile` is found.
   */
  private def buildTombstones(
      withoutBackrefRemoves: Seq[RemoveFile],
      liveAdds: Seq[AddFile],
      rootAndWindowAdds: Seq[AddFile]): Seq[DataEntry] = {
    if (withoutBackrefRemoves.isEmpty) return Seq.empty
    // Implicit `toUniqueFileActionTuple` on AddFile / RemoveFile.
    import InMemoryLogReplay.{UniqueAddFileTuple, UniqueRemoveFileTuple}
    val liveKeys: Set[UniqueFileActionTuple] =
      liveAdds.iterator.map(_.toUniqueFileActionTuple).toSet
    // AddFile lookup from root + window (NOT this commit -- Delta disallows add+remove in one
    // commit, so a removed file's add always predates the commit).
    val addByKey: Map[UniqueFileActionTuple, AddFile] =
      rootAndWindowAdds.map(a => a.toUniqueFileActionTuple -> a).toMap
    withoutBackrefRemoves.flatMap { r =>
      val removeKey = r.toUniqueFileActionTuple
      // Only tombstone a genuinely removed file whose originating add we can find.
      if (liveKeys.contains(removeKey)) None
      else addByKey.get(removeKey).map(add =>
        DataEntry.fromAddFile(add, AMTWriteHelper.removedTracking(), tableRoot))
    }
  }

  /**
   * If the root would exceed `entriesPerLeaf` (carried pointers + tombstones + live adds), moves
   * whole `entriesPerLeaf`-sized batches of live adds into new leaves until the root's row count
   * fits. Returns the live adds that remain root-resident and the pointers for any new leaves.
   */
  private def spillIfNeeded(
      liveAdds: Seq[AddFile],
      fixedRootCount: Int): (Seq[AddFile], Seq[DataManifestEntry]) = {
    var remaining = liveAdds
    val spilled = ArrayBuffer.empty[DataManifestEntry]
    // Each spill adds one leaf pointer to the fixed root count; keep spilling while the root would
    // still overflow.
    while (fixedRootCount + spilled.size + remaining.size > entriesPerLeaf && remaining.nonEmpty) {
      val (batch, rest) = remaining.splitAt(entriesPerLeaf)
      spilled += AMTWriteHelper.writeLeaf(
        spark, fs, hadoopConf, tableRoot, metadataDir, batch)
      remaining = rest
    }
    (remaining, spilled.toSeq)
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
