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

import org.apache.spark.sql.delta.{CheckpointInstance, OptimisticTransactionImpl, Snapshot}
import org.apache.spark.sql.delta.actions.Action

import org.apache.spark.sql.SparkSession

/** Write a new checkpoint at the version committed by the txn if required. */
object CheckpointHook extends PostCommitHook {
  override val name: String = "Post commit checkpoint trigger"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {
    if (!txn.needsCheckpoint) return

    txn.deltaLog.ensureLogDirectoryExist()

    // Since the postCommitSnapshot isn't guaranteed to match committedVersion, we have to
    // explicitly checkpoint the snapshot at the committedVersion.
    val cp = postCommitSnapshot.checkpointProvider
    txn.deltaLog.checkpoint(txn.deltaLog.getSnapshotAt(committedVersion, cp)
    )
  }
}
