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
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable

trait BackfillExecutor extends DeltaLogging {
  def spark: SparkSession
  def deltaLog: DeltaLog
  def catalogTableOpt: Option[CatalogTable]
  def backfillTxnId: String
  def backfillStats: BackfillCommandStats

  def backFillBatchOpType: String
  def filesToBackfill(snapshot: Snapshot): Dataset[AddFile]
  def constructBatch(files: Seq[AddFile]): BackfillBatch

  /** Execute the command by consuming an iterator of [[BackfillBatch]]. */
  def run(maxNumFilesPerCommit: Int): Unit = {
    executeBackfillBatches(maxNumFilesPerCommit)
  }

  /**
   * Execute all [[BackfillBatch]] objects inside the [[Iterator]].
   */
  private def executeBackfillBatches(maxNumFilesPerCommit: Int): Unit = {
    val observer = BackfillExecutionObserver.getObserver
    val numSuccessfulBatch = new AtomicInteger(0)
    val numFailedBatch = new AtomicInteger(0)
    var batchId = 0
    try {
      while (true) {
        val snapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
        val filesInBatch = filesToBackfill(snapshot).limit(maxNumFilesPerCommit).collect()
        if (filesInBatch.isEmpty) {
          return
        }

        val batch = constructBatch(filesInBatch)
        observer.executeBatch {
          val txn = deltaLog.startTransaction(catalogTableOpt, Some(snapshot))
          txn.trackFilesRead(filesInBatch)
          recordDeltaOperation(deltaLog, backFillBatchOpType) {
            batch.execute(backfillTxnId, batchId, txn, numSuccessfulBatch, numFailedBatch)
          }
        }

        batchId += 1
      }
    } finally {
      backfillStats.numSuccessfulBatches = numSuccessfulBatch.get()
      backfillStats.numFailedBatches = numFailedBatch.get()
    }
  }
}
