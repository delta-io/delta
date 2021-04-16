/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.BooleanType

/**
 * Performs an Update using `updateExpression` on the rows that match `condition`
 *
 * Algorithm:
 *   1) Identify the affected files, i.e., the files that may have the rows to be updated.
 *   2) Scan affected files, apply the updates, and generate a new DF with updated rows.
 *   3) Use the Delta protocol to atomically write the new DF as new files and remove
 *      the affected files that are identified in step 1.
 */
case class UpdateCommand(
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
  extends RunnableCommand with DeltaCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numUpdatedRows" -> createMetric(sc, "number of rows updated."),
    "executionTimeMs" -> createMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" -> createMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" -> createMetric(sc, "time taken to rewrite the matched files")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      deltaLog.assertRemovable()
      deltaLog.withNewTransaction { txn =>
        performUpdate(sparkSession, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }
    Seq.empty[Row]
  }

  private def performUpdate(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction): Unit = {
    import sparkSession.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = deltaLog.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)
    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

    val actions: Seq[Action] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      numTouchedFiles = candidateFiles.length

      val filesToRewrite = candidateFiles.map(_.path)
      val operationTimestamp = System.currentTimeMillis()
      val deleteActions = candidateFiles.map(_.removeWithTimestamp(operationTimestamp))

      val rewrittenFiles =
        withStatusCode(
          "DELTA", s"Rewriting ${filesToRewrite.size} files for UPDATE operation (metadata)") {
          rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
            filesToRewrite, nameToAddFile, updateCondition)
        }

      numRewrittenFiles = rewrittenFiles.size
      rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

      deleteActions ++ rewrittenFiles
    } else {
      // Case 3: Find all the affected files using the user-specified condition
      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)
      // Keep everything from the resolved target except a new TahoeFileIndex
      // that only involves the affected files instead of all files.
      val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
      val data = Dataset.ofRows(sparkSession, newTarget)
      val updatedRowCount = metrics("numUpdatedRows")
      val updatedRowUdf = udf { () =>
        updatedRowCount += 1
        true
      }.asNondeterministic()
      val filesToRewrite =
        withStatusCode("DELTA", s"Finding files to rewrite for UPDATE operation") {
          data.filter(new Column(updateCondition))
            .filter(updatedRowUdf())
            .select(input_file_name())
            .distinct().as[String].collect()
        }

      scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
      numTouchedFiles = filesToRewrite.length

      if (filesToRewrite.isEmpty) {
        // Case 3.1: Do nothing if no row qualifies the UPDATE condition
        Nil
      } else {
        // Case 3.2: Delete the old files and generate the new files containing the updated
        // values
        val operationTimestamp = System.currentTimeMillis()
        val deleteActions =
          removeFilesFromPaths(deltaLog, nameToAddFile, filesToRewrite, operationTimestamp)
        val rewrittenFiles =
          withStatusCode("DELTA", s"Rewriting ${filesToRewrite.size} files for UPDATE operation") {
            rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
              filesToRewrite, nameToAddFile, updateCondition)
          }

        numRewrittenFiles = rewrittenFiles.size
        rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

        deleteActions ++ rewrittenFiles
      }
    }

    if (actions.nonEmpty) {
      metrics("numAddedFiles").set(numRewrittenFiles)
      metrics("numRemovedFiles").set(numTouchedFiles)
      metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
      metrics("scanTimeMs").set(scanTimeMs)
      metrics("rewriteTimeMs").set(rewriteTimeMs)
      // In the case where the numUpdatedRows is not captured, we can siphon out the metrics from
      // the BasicWriteStatsTracker. This is for the case where the entire partition is re-written.
      val outputRows = txn.getMetric("numOutputRows").map(_.value).getOrElse(-1L)
      if (metrics("numUpdatedRows").value == 0 && outputRows != 0) {
        metrics("numUpdatedRows").set(outputRows)
      }
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(actions, DeltaOperations.Update(condition.map(_.toString)))
      // This is needed to make the SQL metrics visible in the Spark UI
      val executionId = sparkSession.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkSession.sparkContext, executionId, metrics.values.toSeq)
    }

    recordDeltaEvent(
      deltaLog,
      "delta.dml.update.stats",
      data = UpdateMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        numAddedChangeFiles = 0,
        changeFileBytes = 0,
        scanTimeMs,
        rewriteTimeMs)
    )
  }

  /**
   * Scan all the affected files and write out the updated files
   */
  private def rewriteFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      rootPath: Path,
      inputLeafFiles: Seq[String],
      nameToAddFileMap: Map[String, AddFile],
      condition: Expression): Seq[FileAction] = {
    // Containing the map from the relative file path to AddFile
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles, nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val targetDf = Dataset.ofRows(spark, newTarget)
    val updatedDataFrame = {
      val updatedColumns = buildUpdatedColumns(condition)
      targetDf.select(updatedColumns: _*)
    }

    txn.writeFiles(updatedDataFrame)
  }

  /**
   * Build the new columns. If the condition matches, generate the new value using
   * the corresponding UPDATE EXPRESSION; otherwise, keep the original column value
   */
  private def buildUpdatedColumns(condition: Expression): Seq[Column] = {
    updateExpressions.zip(target.output).map { case (update, original) =>
      val updated = If(condition, update, original)
      new Column(Alias(updated, original.name)())
    }
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method

}

object UpdateCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
}

/**
 * Used to report details about update.
 *
 * @param condition: what was the update condition
 * @param numFilesTotal: how big is the table
 * @param numTouchedFiles: how many files did we touch
 * @param numRewrittenFiles: how many files had to be rewritten
 * @param numAddedChangeFiles: how many change files were generated
 * @param changeFileBytes: total size of change files generated
 * @param scanTimeMs: how long did finding take
 * @param rewriteTimeMs: how long did rewriting take
 *
 * @note All the time units are milliseconds.
 */
case class UpdateMetric(
    condition: String,
    numFilesTotal: Long,
    numTouchedFiles: Long,
    numRewrittenFiles: Long,
    numAddedChangeFiles: Long,
    changeFileBytes: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long)
