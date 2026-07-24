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

package org.apache.spark.sql.delta

import scala.collection.mutable

import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo}
import org.apache.spark.sql.delta.amt.AMTTriggerMode
import org.apache.spark.sql.delta.hooks.PostCommitHook

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Describes the log-maintenance work a committed transaction has decided is needed after it
 * commits. Populated during the commit and carried on [[CommittedTransaction]] for the
 * post-commit hooks to act on.
 *
 * For a non-AMT table this is derived from the checkpoint/minor-compaction decision
 * (`CheckpointTrigger`). For an AMT table it is set by `AMTWriterManager`, which also picks the
 * trigger mode driving the follow-up manifest commit.
 *
 * @param shouldCheckpoint  whether a checkpoint should be written after the commit. For an AMT
 *                          table this means a follow-up `OPTIMIZE CHECKPOINT` commit rewrites the
 *                          manifest tree; for a non-AMT table it means a standalone V2 checkpoint.
 * @param amtTriggerModeOpt for an AMT checkpoint, the [[AMTTriggerMode]] that scheduled it (it
 *                          carries whether the rewrite is incremental and the name recorded in the
 *                          AMT write metrics). `None` when `shouldCheckpoint` is false or the table
 *                          is not AMT.
 */
case class MaintenanceOperation(
    shouldCheckpoint: Boolean = false,
    amtTriggerModeOpt: Option[AMTTriggerMode] = None) {
  // A trigger mode only makes sense when a checkpoint is scheduled.
  require(amtTriggerModeOpt.isEmpty || shouldCheckpoint,
    "amtTriggerModeOpt must be empty when shouldCheckpoint is false.")
}

/**
 * Represents a successfully committed transaction.
 *
 * This class encapsulates all relevant information about a transaction that has been successfully
 * committed. The main usage of this class is in running the post-commit hooks.
 *
 * @param txnId                 the unique identifier of the committed transaction.
 * @param deltaLog              the [[DeltaLog]] instance for the table the transaction
 *                              committed on.
 * @param catalogTable          the catalog table exposed to post-commit hooks.
 *                              Normal commits use the transaction's construction-time table.
 *                              UC retry and migration paths may pass the refreshed table.
 *                              That table comes from the successful commit attempt.
 * @param readSnapshot          the snapshot of the table at the time of the transaction's read.
 * @param committedVersion      the version of the table after the txn committed.
 * @param committedActions      the actions that were committed in this transaction.
 * @param postCommitSnapshot    the snapshot of the table after the txn successfully committed.
 *                              NOTE: This may not match the committedVersion, if racing
 *                              commits were written while the snapshot was computed.
 * @param postCommitHooks       the list of post-commit hooks to run after the commit.
 * @param txnExecutionTimeMs    the time taken to execute the transaction.
 * @param maintenanceOperation  the log-maintenance work (checkpoint etc.) this
 *                              commit decided is needed, for the post-commit hooks to act on.
 * @param partitionsAddedToOpt  the partitions that this txn added new files to.
 * @param isBlindAppend         whether this transaction was a blind append.
 */
case class CommittedTransaction(
    txnId: String,
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    readSnapshot: Snapshot,
    committedVersion: Long,
    committedActions: Seq[Action],
    postCommitSnapshot: Snapshot,
    postCommitHooks: Seq[PostCommitHook],
    txnExecutionTimeMs: Long,
    maintenanceOperation: MaintenanceOperation,
    partitionsAddedToOpt: Option[mutable.HashSet[Map[String, String]]],
    isBlindAppend: Boolean
)
