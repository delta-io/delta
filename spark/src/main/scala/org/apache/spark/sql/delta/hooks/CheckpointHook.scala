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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CommittedTransaction, DeltaOperations}

import org.apache.spark.sql.SparkSession

/** Write a new checkpoint at the version committed by the txn if required. */
object CheckpointHook extends PostCommitHook {
  override val name: String = "Post commit checkpoint trigger"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!txn.maintenanceOperation.shouldCheckpoint) return

    // AMT tables checkpoint by rewriting their manifest tree via a follow-up OPTIMIZE CHECKPOINT
    // commit rather than a standalone checkpoint file.
    if (txn.postCommitSnapshot.protocol.isFeatureSupported(AdaptiveMetadataTableFeature)) {
      val triggerMode = txn.maintenanceOperation.amtTriggerModeOpt.getOrElse {
        throw new IllegalStateException(
          "An AMT table scheduled a checkpoint but carries no AMTTriggerMode.")
      }
      writeAMTCheckpoint(
        txn, incremental = triggerMode.isIncremental, triggerName = triggerMode.name)
      return
    }

    // Since the postCommitSnapshot isn't guaranteed to match committedVersion, we have to
    // explicitly checkpoint the snapshot at the committedVersion.
    val cp = txn.postCommitSnapshot.checkpointProvider
    val snapshotToCheckpoint = txn.deltaLog.getSnapshotAt(
      txn.committedVersion,
      lastCheckpointHint = None,
      lastCheckpointProvider = Some(cp),
      catalogTableOpt = txn.catalogTable,
      enforceTimeTravelWithinDeletedFileRetention = false)
    txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
  }

  /**
   * Attempts a Manifest Commit on the table.
   */
  private def writeAMTCheckpoint(
      txn: CommittedTransaction, incremental: Boolean, triggerName: String): Unit = {
    val checkpointTxn =
      txn.deltaLog.startTransaction(txn.catalogTable, Some(txn.postCommitSnapshot))
    checkpointTxn.commit(Seq.empty, DeltaOperations.OptimizeCheckpoint(incremental, triggerName))
  }
}
