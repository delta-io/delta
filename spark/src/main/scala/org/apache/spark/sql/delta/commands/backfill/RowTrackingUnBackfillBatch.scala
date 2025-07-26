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

package org.apache.spark.sql.delta.commands.backfill

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.SparkSession

case class RowTrackingUnBackfillBatch(filesInBatch: Seq[AddFile]) extends BackfillBatch {
  override val backfillBatchStatsOpType = "delta.rowTracking.unbackfill.batch.stats"

  /** Remove relevant metadata from addFiles. */
  override protected def prepareFilesAndCommit(
      spark: SparkSession,
      txn: OptimisticTransaction,
      batchId: Int): Unit = {
    val metadata = txn.snapshot.metadata
    val isEnabled = DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)
    val suspendIdGeneration = DeltaConfigs.ROW_TRACKING_SUSPENDED.fromMetaData(metadata)
    if (isEnabled || !suspendIdGeneration) {
      throw new IllegalStateException(
        "Cannot run unbackfill when row tracking is enabled or not suspended.")
    }

    val filesToCommit = filesInBatch.map(_.copy(
      baseRowId = None,
      defaultRowCommitVersion = None,
      dataChange = false))
    txn.commit(filesToCommit, DeltaOperations.RowTrackingUnBackfill(batchId))
  }
}
