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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession

trait BackfillExecutor extends DeltaLogging {
  def spark: SparkSession
  def origTxn: OptimisticTransaction
  def tracker: FileMetadataMaterializationTracker
  def backfillStats: BackfillCommandStats

  def backFillBatchOpType: String

  /** Execute the command by consuming an iterator of [[BackfillBatch]]. */
  def run(batches: Iterator[BackfillBatch]): Unit = {
    if (batches.isEmpty) {
      logInfo(log"Backfill command did not need to update any files")
      return
    }
    executeBackfillBatches(batches)
  }

  /**
   * Execute all [[BackfillBatch]] objects inside the [[Iterator]].
   *
   * @param batches The iterator of [[BackfillBatch]] to be executed.
   */
  private def executeBackfillBatches(
      batches: Iterator[BackfillBatch]): Unit = {
    val observer = BackfillExecutionObserver.getObserver
    val numSuccessfulBatch = new AtomicInteger(0)
    val numFailedBatch = new AtomicInteger(0)
    try {
      batches.zipWithIndex.foreach { case (batch, batchId) =>
        try {
          observer.executeBatch {
            recordDeltaOperation(origTxn.deltaLog, backFillBatchOpType) {
              batch.execute(origTxn, batchId, numSuccessfulBatch, numFailedBatch)
            }
          }
        } finally {
          // release all the permits for the files materialized by the batch
          if (tracker != null) {
            tracker.releasePermits(batch.filesInBatch.length)
          }
        }
      }
    } finally {
      backfillStats.numSuccessfulBatches = numSuccessfulBatch.get()
      backfillStats.numFailedBatches = numFailedBatch.get()
    }
  }
}
