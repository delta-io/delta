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

import java.util.concurrent.{Semaphore, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

trait BackfillExecutor extends DeltaLogging {
  def spark: SparkSession
  def origTxn: OptimisticTransaction
  def tracker: FileMetadataMaterializationTracker
  def maxBatchesInParallel: Int
  def backfillStats: BackfillCommandStats

  def backFillBatchOpType: String

  /** Execute the command by consuming an iterator of [[BackfillBatch]]. */
  def run(batches: Iterator[BackfillBatch]): Unit = {
    if (batches.isEmpty) {
      logInfo(log"Backfill command did not need to update any files")
      return
    }
    executeBackfillBatches(
      batches,
      activeThreadSemaphore = new Semaphore(maxBatchesInParallel))
  }

  /**
   * Execute all [[BackfillBatch]] objects inside the [[Iterator]] by launching a set
   * of background threads. Each thread handles and commits a single
   * [[BackfillBatch]] object.
   *
   * @param batches               The iterator of [[BackfillBatch]] to be executed.
   * @param activeThreadSemaphore The semaphore to control the number of concurrent
   *                              active threads.
   */
  private def executeBackfillBatches(
      batches: Iterator[BackfillBatch],
      activeThreadSemaphore: Semaphore): Unit = {
    // Get the thread pool to handle each batch.
    val globalThreadPool = BackfillExecutor.getOrCreateThreadPool()
    // The futures to wait for concurrent active batches to complete.
    val futures = new ArrayBuffer[java.util.concurrent.Future[Unit]]
    val numSuccessfulBatch = new AtomicInteger(0)
    val numFailedBatch = new AtomicInteger(0)
    try {
      batches.zipWithIndex.foreach { case (batch, batchId) =>
        activeThreadSemaphore.acquire()
        futures += globalThreadPool.submit(() => try {
          // Make sure each commit has the same active spark session
          SparkSession.setActiveSession(spark)
          recordDeltaOperation(origTxn.deltaLog, backFillBatchOpType) {
            batch.execute(origTxn, batchId, numSuccessfulBatch, numFailedBatch)
          }
        } finally {
          activeThreadSemaphore.release()
          // release all the permits for the files materialized by the batch
          if (tracker != null) {
            tracker.releasePermits(batch.filesInBatch.length)
          }
        })
      }
      futures.foreach(_.get())
    } catch {
      case t: Throwable =>
        // In case of exception we want to cancel futures that haven't started running yet. We don't
        // want to interrupt futures that are already running because it may lead to incorrect
        // metrics. We don't record the metric BackfillBatchStats atomically with the
        // txn.commit call, so if we cancel the future, we may not correctly record the success.
        val mayInterruptIfRunning = false
        futures.foreach(_.cancel(mayInterruptIfRunning))
        throw t
    } finally {
      backfillStats.numSuccessfulBatches = numSuccessfulBatch.get()
      backfillStats.numFailedBatches = numFailedBatch.get()
    }
  }
}

private[delta] object BackfillExecutor extends DeltaLogging {

  /** Global thread pool for all Backfill queries */
  private var threadPool: ThreadPoolExecutor = _

  /**
   * Get existing global thread pool or create a new one if it doesn't exist.
   */
  private[backfill] def getOrCreateThreadPool(): ThreadPoolExecutor = synchronized {
    if (threadPool == null) {
      threadPool = ThreadUtils.newDaemonCachedThreadPool(
        "deltaBackfill",
        maxThreadNumber = 64)
    }
    threadPool
  }
}
