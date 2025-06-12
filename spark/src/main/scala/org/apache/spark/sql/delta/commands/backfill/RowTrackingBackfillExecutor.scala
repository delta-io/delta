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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Dataset, SparkSession}

class RowTrackingBackfillExecutor(
    override val spark: SparkSession,
    override val origTxn: OptimisticTransaction,
    override val tracker: FileMetadataMaterializationTracker,
    override val maxBatchesInParallel: Int,
    override val backfillStats: BackfillCommandStats) extends BackfillExecutor {
  override val backFillBatchOpType = "delta.rowTracking.backfill.batch"
}

private[delta] object RowTrackingBackfillExecutor extends DeltaLogging {
  /**
   * Returns the dataset with the list of candidate files to backfill.
   */
  def getCandidateFilesToBackfill(snapshot: Snapshot): Dataset[AddFile] = {
    // Note: We can't use txn.filterFiles() because it drops the file statistics.
    snapshot
      .allFiles
      .filter(_.baseRowId.isEmpty)
  }
}
