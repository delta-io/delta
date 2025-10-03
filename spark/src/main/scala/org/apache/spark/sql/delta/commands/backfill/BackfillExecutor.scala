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
import org.apache.spark.sql.delta.sources.DeltaSQLConf

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

  /**
   * Execute the command by consuming a sequence of [[BackfillBatch]].
   * Returns an option with the last commit version when available. Otherwise, it returns None.
   */
  def run(maxNumFilesPerCommit: Int): Option[Long] = {
    executeBackfillBatches(maxNumFilesPerCommit)
  }

  /**
   * Execute all available [[BackfillBatch]].
   * Returns an option with the last commit version when available. Otherwise, it returns None.
   *
   * Note, in the case of competing concurrent transactions, this method will exit after processing
   * a maximum of `backfill.maxNumFilesFactor` times the total number of files in the table.
   */
  private def executeBackfillBatches(maxNumFilesPerCommit: Int): Option[Long] = {
    val observer = BackfillExecutionObserver.getObserver
    val numSuccessfulBatch = new AtomicInteger(0)
    val numFailedBatch = new AtomicInteger(0)
    val totalFileCount = deltaLog.update(catalogTableOpt = catalogTableOpt).numOfFiles
    val factor = spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_FACTOR)
    val maxFilesToProcess = totalFileCount * factor

    var batchId = 0
    var totalFilesProcessed = 0L
    var filesInBatch = Seq.empty[AddFile]
    var lastCommitOpt: Option[Long] = None

    // If the last batch contained fewer files than the maxNumFilesPerCommit we exit.
    // This protects against live-locking with fast concurrent txns that only commit
    // a few files.
    // Having excluded this option the backfill can only live-lock with an equally fast
    // concurrent txn, i.e a competing un-backfill that only commits logs files.
    // To protect against this we set a maximum backfill limit equal to a factor of the
    // total table file count.
    def moreFilesToProcess(): Boolean = {
      filesInBatch.length == maxNumFilesPerCommit && totalFilesProcessed < maxFilesToProcess
    }

    try {
      do {
        val snapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
        filesInBatch = filesToBackfill(snapshot).limit(maxNumFilesPerCommit).collect()
        if (filesInBatch.isEmpty) {
          return lastCommitOpt
        }

        val batch = constructBatch(filesInBatch)
        observer.executeBatch {
          val txn = deltaLog.startTransaction(catalogTableOpt, Some(snapshot))
          txn.trackFilesRead(filesInBatch)
          recordDeltaOperation(deltaLog, backFillBatchOpType) {
            lastCommitOpt = Some(batch.execute(
              spark, backfillTxnId, batchId, txn, numSuccessfulBatch, numFailedBatch))
          }
          batchId += 1
          totalFilesProcessed += filesInBatch.length
        }
      } while (moreFilesToProcess())

      if (totalFilesProcessed >= maxFilesToProcess) {
        recordDeltaEvent(
          deltaLog,
          opType = "delta.backfillExceededMaxFilesToProcess",
          data = Map("maxFilesProcessed" -> maxFilesToProcess))
      }
      lastCommitOpt
    } finally {
      backfillStats.numSuccessfulBatches = numSuccessfulBatch.get()
      backfillStats.numFailedBatches = numFailedBatch.get()
    }
  }
}
