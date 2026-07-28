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

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Row, SparkSession}
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
  def catalogTableOpt: Option[CatalogTable]

  def getBackfillExecutor(
    spark: SparkSession,
    deltaLog: DeltaLog,
    catalogTableOpt: Option[CatalogTable],
    backfillId: String,
    backfillStats: BackfillCommandStats): BackfillExecutor

  def opType: String

  override def run(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, opType) {
      val backfillId = UUID.randomUUID().toString
      val maxNumFilesPerCommit =
        spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT)
      val startTimeNs = System.nanoTime()
      val backfillStats = BackfillCommandStats(
        transactionId = backfillId,
        nameOfTriggeringOperation
      )
      try {
        val backfillExecutor = getBackfillExecutor(
          spark, deltaLog, catalogTableOpt, backfillId, backfillStats)
        val lastCommitOpt = backfillExecutor.run(maxNumFilesPerCommit)
        backfillStats.wasSuccessful = true
        Array.empty[Row] ++ lastCommitOpt.map(Row(_))
      } finally {
        val totalExecutionTimeMs =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        backfillStats.totalExecutionTimeMs = totalExecutionTimeMs

        recordDeltaEvent(
          deltaLog,
          opType = opType + ".stats",
          data = backfillStats
        )
      }
    }
  }
}
