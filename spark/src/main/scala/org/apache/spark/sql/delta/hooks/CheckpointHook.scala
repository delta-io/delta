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

import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.amt.AMTUtils
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/** Write a new checkpoint at the version committed by the txn if required. */
object CheckpointHook extends PostCommitHook with DeltaLogging {
  override val name: String = "Post commit checkpoint trigger"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!txn.maintenanceOperation.shouldCheckpoint) return

    val snapshotToCheckpoint =
      if (AMTUtils.amtEnabled(txn.postCommitSnapshot)) {
        txn.postCommitSnapshot
      } else {
        // Two txns writing multi-part checkpoints at the same version has the risk of corruption.
        // So make sure the txn write the checkpoint only for the version it created.
        val cp = txn.postCommitSnapshot.checkpointProvider
        txn.deltaLog.getSnapshotAt(
          txn.committedVersion,
          lastCheckpointHint = None,
          lastCheckpointProvider = Some(cp),
          catalogTableOpt = txn.catalogTable,
          enforceTimeTravelWithinDeletedFileRetention = false)
      }
    if (!AMTUtils.amtEnabled(snapshotToCheckpoint) &&
        spark.conf.get(DeltaSQLConf.DELTA_CHECKPOINT_SKIP_IF_IN_PROGRESS)) {
      // Skip the checkpoint if this process is already writing one for this table, for example
      // after a concurrent commit to the same table. A later commit writes the next one.
      val checkpointed =
        txn.deltaLog.checkpointUnlessInProgress(snapshotToCheckpoint, txn.catalogTable)
      if (!checkpointed) {
        recordDeltaEvent(
          txn.deltaLog,
          opType = "delta.checkpoint.skippedInProgress",
          data = Map("version" -> txn.committedVersion))
      }
      return
    }
    txn.deltaLog.checkpoint(
      snapshotToCheckpoint,
      txn.catalogTable,
      amtTriggerModeOpt = txn.maintenanceOperation.amtTriggerModeOpt)
  }
}
