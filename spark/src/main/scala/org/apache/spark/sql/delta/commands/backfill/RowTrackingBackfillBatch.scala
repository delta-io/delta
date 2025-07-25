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

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaOperations, OptimisticTransaction, RowTrackingFeature}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

import org.apache.spark.sql.SparkSession

case class RowTrackingBackfillBatch(filesInBatch: Seq[AddFile]) extends BackfillBatch {

  override val backfillBatchStatsOpType = "delta.rowTracking.backfill.batch.stats"

  /** Mark all files as dataChange = false and commit. */
  override protected def prepareFilesAndCommit(
      spark: SparkSession,
      txn: OptimisticTransaction,
      batchId: Int): Unit = {
    val protocol = txn.snapshot.protocol
    val metadata = txn.snapshot.metadata
    val isRowTrackingSupported = protocol.isFeatureSupported(RowTrackingFeature)
    val ignoreProperty = DeltaSQLConf.DELTA_ROW_TRACKING_IGNORE_SUSPENSION.key
    val ignoreSuspension = DeltaUtils.isTesting && spark.conf.get(ignoreProperty).toBoolean
    val suspendRowTracking =
      DeltaConfigs.ROW_TRACKING_SUSPENDED.fromMetaData(metadata) && !ignoreSuspension
    if (!isRowTrackingSupported || suspendRowTracking) {
      throw new IllegalStateException(
        """
          |Cannot run backfill command if row tracking is not supported or
          |row ID generation is suspended.""".stripMargin)
    }

    val filesToCommit = filesInBatch.map(_.copy(dataChange = false))
    // Base Row IDs are added as part of the OptimisticTransaction.prepareCommit(), so we don't
    // need to do anything here other than recommit the files.
    // Note: A backfill commit can be empty ,i.e. have no file actions, at commit time due to
    // files being removed by concurrent conflict resolution.
    txn.commit(filesToCommit, DeltaOperations.RowTrackingBackfill(batchId))
  }
}
