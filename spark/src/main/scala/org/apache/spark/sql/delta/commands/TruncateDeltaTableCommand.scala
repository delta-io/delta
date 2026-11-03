/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands

import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
// scalastyle:on import.ordering.noEmptyLine

case class TruncateDeltaTableCommand(child: LogicalPlan)
    extends RunnableCommand
    with UnaryNode
    with DeltaCommand
{


  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numRemovedFiles" -> createMetric(sc, "number of files removed"),
    "numDeletedRows" -> createMetric(sc, "number of rows removed"),
    "executionTimeMs" -> createMetric(sc, "time taken to execute the entire operation"),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed")
  )

  override val output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): TruncateDeltaTableCommand =
    copy(child = newChild)

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    val startTime = System.nanoTime()
    val deltaTable = getDeltaTable(child, "TRUNCATE")


    recordDeltaOperation(deltaTable, "delta.dml.truncate") {
      val txn = deltaTable.startTransaction()
      DeltaLog.assertRemovable(txn.snapshot)
      val deleteActions: Seq[RemoveFile] = {
        val reportRowLevelMetrics = conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)
        val allFiles =
          txn.filterFiles(Seq.empty, keepNumRecords = reportRowLevelMetrics)
        val operationTimestamp = System.currentTimeMillis()
        val numRemovedBytes = allFiles.map(_.getFileSize).sum
        metrics("numRemovedBytes").set(numRemovedBytes)
        allFiles.map(_.removeWithTimestamp(operationTimestamp))
      }

      if (deleteActions.nonEmpty) {
        metrics("numRemovedFiles").set(deleteActions.size)
        val executionTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
        metrics("executionTimeMs").set(executionTimeMs)

        if (conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)) {
          def numDeletedRows: Option[Long] = {
            var count: Long = 0L
            for (file <- deleteActions) {
              if (file.numLogicalRecords.isEmpty) {
                return None
              }
              count += file.numLogicalRecords.get
            }
            Some(count)
          }

          if (numDeletedRows.nonEmpty) {
            metrics("numDeletedRows").set(numDeletedRows.get)
          }
        }

        txn.registerSQLMetrics(sparkSession, metrics)
        try {
          txn.commit(deleteActions, DeltaOperations.Truncate())
        } finally {
          sendDriverMetrics(sparkSession, metrics)
        }
      }
    }

    // Uncache the relation
    try {
      val tableDf = deltaTable.tableIdentifier match {
        case Some(table) => sparkSession.read.table(table)
        case _ => sparkSession.read.format("delta").load(deltaTable.path.toString)
      }
      sparkSession.sharedState.cacheManager.uncacheQuery(tableDf, cascade = true)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache Delta table ${deltaTable.name}", e)
    }

    Seq.empty[Row]
  }
}
