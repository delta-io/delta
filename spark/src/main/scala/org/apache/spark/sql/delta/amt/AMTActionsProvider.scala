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

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, DomainMetadata, Metadata, Protocol, SetTransaction}

/**
 * The previous-AMT state that an incremental write builds the next manifest tree on top of.
 *
 * This is everything [[IncrementalAMTWriter]] needs from the "old" tree.
 *
 * @param metadata                          the old AMT's inline table metadata.
 * @param protocol                          the old AMT's inline table protocol.
 * @param setTransactions                   the old AMT's inline `SetTransaction` actions.
 * @param domainMetadatas                   the old AMT's inline `DomainMetadata` actions.
 * @param fileActionsFromRoot               the live root-resident file actions (leaf-resident files
 *                                          are carried forward by pointer, not materialized here).
 * @param allLeafs                          the root's `DATA_MANIFEST` leaf pointers, one per leaf.
 * @param version                           the table version the old AMT describes.
 * @param lastManifestCommitWithFullRewrite the last-full-rewrite marker carried forward into the
 *                                          new tree's `ContentRoot`.
 */
case class BaseAMTActionsResult(
    metadata: Metadata,
    protocol: Protocol,
    setTransactions: Seq[SetTransaction],
    domainMetadatas: Seq[DomainMetadata],
    fileActionsFromRoot: Seq[AddFile],
    allLeafs: Seq[DataManifestEntry],
    version: Long,
    lastManifestCommitWithFullRewrite: Option[Long])

/**
 * Loads the previous-AMT state that [[IncrementalAMTWriter]] extends. Abstracting the source lets
 * the writer build on top of any old-AMT representation, not just a resolved checkpoint provider.
 */
trait BaseAMTActionsProvider {
  def load(): BaseAMTActionsResult
}

/**
 * A [[BaseAMTActionsProvider]] backed by a Delta snapshot with no previous AMT.
 *
 * Currently supports only empty snapshots. Supporting nonempty snapshots requires preserving
 * file tracking semantics and loading live files without collecting an unbounded snapshot on the
 * driver.
 */
class BaseSnapshotActionsProvider(
    snapshot: Snapshot) extends BaseAMTActionsProvider {
  require(!snapshot.checkpointProvider.isInstanceOf[AMTCheckpointProvider],
    "A snapshot-backed AMT base must not already have an AMT checkpoint.")
  require(snapshot.numOfFiles == 0,
    "The snapshot-backed AMT actions provider currently requires an empty snapshot.")

  override def load(): BaseAMTActionsResult = {
    BaseAMTActionsResult(
      metadata = snapshot.metadata,
      protocol = snapshot.protocol,
      setTransactions = snapshot.setTransactions,
      domainMetadatas = snapshot.domainMetadata,
      // Safe because of the empty-snapshot invariant above. A nonempty implementation must supply
      // every live base file here without eagerly collecting an unbounded snapshot to the driver.
      fileActionsFromRoot = Seq.empty,
      allLeafs = Seq.empty,
      version = snapshot.version,
      lastManifestCommitWithFullRewrite = None)
  }
}

/** A [[BaseAMTActionsProvider]] backed by an already-resolved [[AMTCheckpointProvider]]. */
class BaseAMTCheckpointActionsProvider(
    deltaLog: DeltaLog,
    checkpointProvider: AMTCheckpointProvider) extends BaseAMTActionsProvider {
  override def load(): BaseAMTActionsResult = {
    val checkpoint = checkpointProvider.checkpointAction
    BaseAMTActionsResult(
      metadata = checkpoint.metaData,
      protocol = checkpoint.protocol,
      setTransactions = checkpoint.txns,
      domainMetadatas = checkpoint.domainMetadata,
      fileActionsFromRoot = AMTCheckpointProvider.readLiveRootDataEntries(deltaLog, checkpoint),
      allLeafs = checkpointProvider.leaves,
      version = checkpoint.contentRoot.version,
      lastManifestCommitWithFullRewrite = checkpoint.contentRoot.lastManifestCommitWithFullRewrite)
  }
}
