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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession

trait BackfillBatch extends DeltaLogging {
  /** The files in this batch. */
  def filesInBatch: Seq[AddFile]
  def backfillBatchStatsOpType: String

  protected def prepareFilesAndCommit(
      spark: SparkSession,
      txn: OptimisticTransaction,
      batchId: Int): Long

  /**
   * The main method of this trait. This method commits the backfill batch, records metrics and
   * updates the two atomic counters passed in.
   *
   * @param spark The Spark session.
   * @param backfillTxnId the transaction id associated with the parent command.
   * @param batchId an integer identifier of the batch within a parent [[BackfillCommand]].
   * @param txn transaction used to construct the current batch.
   * @param numSuccessfulBatch an AtomicInteger which serves as a counter for the total number of
   *                           batches that were successful.
   * @param numFailedBatch an AtomicInteger which serves as a counter for the total number of
   *                       batches that failed.
   */
  def execute(
      spark: SparkSession,
      backfillTxnId: String,
      batchId: Int,
      txn: OptimisticTransaction,
      numSuccessfulBatch: AtomicInteger,
      numFailedBatch: AtomicInteger): Long = {
    val startTimeNs = System.nanoTime()

    def recordBackfillBatchStats(txnId: String, wasSuccessful: Boolean): Unit = {
      if (wasSuccessful) {
        numSuccessfulBatch.incrementAndGet()
      } else {
        numFailedBatch.incrementAndGet()
      }
      val totalExecutionTimeInMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
      val batchStats = BackfillBatchStats(
        backfillTxnId, txnId, batchId, filesInBatch.size, totalExecutionTimeInMs, wasSuccessful)
      recordDeltaEvent(
        txn.deltaLog,
        opType = backfillBatchStatsOpType,
        data = batchStats
      )
    }

    logInfo(log"Batch ${MDC(DeltaLogKeys.BATCH_ID, batchId.toLong)} starting, committing " +
      log"${MDC(DeltaLogKeys.NUM_FILES, filesInBatch.size.toLong)} candidate files")
    val txnId = txn.txnId
    try {
      val commitVersion = prepareFilesAndCommit(spark, txn, batchId)
      recordBackfillBatchStats(txnId, wasSuccessful = true)
      commitVersion
    } catch {
      case t: Throwable =>
        recordBackfillBatchStats(txnId, wasSuccessful = false)
        throw t
    }
  }
}
