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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 * This command will lazily materialize AllFiles and split them into multiple backfill commits
 * if the number of files exceeds the threshold set by
 * [[DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT]].
 */
trait BackfillCommand extends LeafRunnableCommand with DeltaCommand {
  def deltaLog: DeltaLog
  def nameOfTriggeringOperation: String
  def catalogTable: Option[CatalogTable]

  def getBackfillExecutor(
    spark: SparkSession,
    txn: OptimisticTransaction,
    fileMaterializationTracker: FileMetadataMaterializationTracker,
    maxNumBatchesInParallel: Int,
    backfillStats: BackfillCommandStats): BackfillExecutor

  def filesToBackfill(txn: OptimisticTransaction): Dataset[AddFile]

  def opType: String

  def constructBatch(files: Seq[AddFile]): BackfillBatch

  override def run(spark: SparkSession): Seq[Row] = {
    val maxNumBatchesInParallel =
      spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_BATCHES_IN_PARALLEL)
    recordDeltaOperation(deltaLog, opType) {
      val txn = deltaLog.startTransaction(catalogTable)
      // This txn object is not used for commit. We need to do manual state transitions to make the
      // concurrency testing framework happy.
      txn.executionObserver.preparingCommit()
      txn.executionObserver.beginDoCommit()
      txn.executionObserver.beginBackfill()
      val maxNumFilesPerCommit =
        spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT)
      val metricsOpType = "delta.backfill.materialization.trackerMetrics"
      FileMetadataMaterializationTracker.withTracker(txn, spark, metricsOpType) {
        fileMaterializationTracker =>
          val startTimeNs = System.nanoTime()
          val backfillStats = BackfillCommandStats(
            transactionId = txn.txnId,
            nameOfTriggeringOperation,
            maxNumBatchesInParallel = maxNumBatchesInParallel
          )
          try {
            val backfillExecutor = getBackfillExecutor(
              spark, txn, fileMaterializationTracker, maxNumBatchesInParallel, backfillStats)

            val batches = new BackfillBatchIterator(
              filesToBackfill(txn),
              fileMaterializationTracker,
              maxNumFilesPerCommit,
              constructBatch)

            try {
              backfillExecutor.run(batches)
            } finally {
              batches.close()
            }

            backfillStats.wasSuccessful = true
          } finally {
            val totalExecutionTimeMs =
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
            backfillStats.totalExecutionTimeMs = totalExecutionTimeMs

            recordDeltaEvent(
              txn.deltaLog,
              opType = opType + ".stats",
              data = backfillStats
            )
            txn.executionObserver.transactionCommitted()
          }
      }
    }

    Seq.empty[Row]
  }
}
