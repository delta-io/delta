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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_TYPE_COLUMN_NAME, CDC_TYPE_NOT_CDC, CDC_TYPE_UPDATE_POSTIMAGE, CDC_TYPE_UPDATE_PREIMAGE}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{array, col, explode, input_file_name, lit, struct, typedLit, udf}

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
  extends LeafRunnableCommand with DeltaCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numUpdatedRows" -> createMetric(sc, "number of rows updated."),
    "numCopiedRows" -> createMetric(sc, "number of rows copied."),
    "executionTimeMs" -> createMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" -> createMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" -> createMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated")
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
    var numAddedChangeFiles: Long = 0
    var changeFileBytes: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)
    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

    val filesToRewrite: Seq[AddFile] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      candidateFiles
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
      val pathsToRewrite =
        withStatusCode("DELTA", UpdateCommand.FINDING_TOUCHED_FILES_MSG) {
          data.filter(new Column(updateCondition))
            .filter(updatedRowUdf())
            .select(input_file_name())
            .distinct()
            .as[String]
            .collect()
        }

      scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

      pathsToRewrite.map(getTouchedFile(deltaLog.dataPath, _, nameToAddFile)).toSeq
    }

    numTouchedFiles = filesToRewrite.length

    val newActions = if (filesToRewrite.isEmpty) {
      // Do nothing if no row qualifies the UPDATE condition
      Nil
    } else {
      // Generate the new files containing the updated values
      withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(filesToRewrite.size)) {
        rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
          filesToRewrite.map(_.path), nameToAddFile, updateCondition)
      }
    }

    rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

    val (changeActions, addActions) = newActions.partition(_.isInstanceOf[AddCDCFile])
    numRewrittenFiles = addActions.size
    numAddedChangeFiles = changeActions.size
    changeFileBytes = changeActions.collect { case f: AddCDCFile => f.size }.sum

    val totalActions = if (filesToRewrite.isEmpty) {
      // Do nothing if no row qualifies the UPDATE condition
      Nil
    } else {
      // Delete the old files and return those delete actions along with the new AddFile actions for
      // files containing the updated values
      val operationTimestamp = System.currentTimeMillis()
      val deleteActions = filesToRewrite.map(_.removeWithTimestamp(operationTimestamp))

      deleteActions ++ newActions
    }

    if (totalActions.nonEmpty) {
      metrics("numAddedFiles").set(numRewrittenFiles)
      metrics("numAddedChangeFiles").set(numAddedChangeFiles)
      metrics("changeFileBytes").set(changeFileBytes)
      metrics("numRemovedFiles").set(numTouchedFiles)
      metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
      metrics("scanTimeMs").set(scanTimeMs)
      metrics("rewriteTimeMs").set(rewriteTimeMs)
      // In the case where the numUpdatedRows is not captured, we can siphon out the metrics from
      // the BasicWriteStatsTracker. This is for case #2 where the entire partition is re-written.
      val outputRows = txn.getMetric("numOutputRows").map(_.value).getOrElse(-1L)
      if (metrics("numUpdatedRows").value == 0 && outputRows != 0) {
        metrics("numUpdatedRows").set(outputRows)
      }
      metrics("numCopiedRows").set(outputRows - metrics("numUpdatedRows").value)
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(totalActions, DeltaOperations.Update(condition.map(_.toString)))
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
        numAddedChangeFiles,
        changeFileBytes,
        scanTimeMs,
        rewriteTimeMs)
    )
  }

  /**
   * Scan all the affected files and write out the updated files.
   *
   * When CDF is enabled, includes the generation of CDC preimage and postimage columns for
   * changed rows.
   *
   * @return the list of [[AddFile]]s and [[AddCDCFile]]s that have been written.
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

    val updatedDataFrame = UpdateCommand.withUpdatedColumns(
      target,
      updateExpressions,
      condition,
      targetDf.withColumn(UpdateCommand.CONDITION_COLUMN_NAME, new Column(condition)),
      UpdateCommand.shouldOutputCdc(txn))

    txn.writeFiles(updatedDataFrame)
  }
}

object UpdateCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
  val CONDITION_COLUMN_NAME = "__condition__"
  val FINDING_TOUCHED_FILES_MSG: String = "Finding files to rewrite for UPDATE operation"

  def rewritingFilesMsg(numFilesToRewrite: Long): String =
    s"Rewriting $numFilesToRewrite files for UPDATE operation"

  /**
   * Whether or not CDC is enabled on this table and, thus, if we should output CDC data during this
   * UPDATE operation.
   */
  def shouldOutputCdc(txn: OptimisticTransaction): Boolean = {
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(txn.metadata)
  }

  /**
   * Build the new columns. If the condition matches, generate the new value using
   * the corresponding UPDATE EXPRESSION; otherwise, keep the original column value.
   *
   * When CDC is enabled, includes the generation of CDC pre-image and post-image columns for
   * changed rows.
   *
   * @param target target we are updating into
   * @param updateExpressions the update transformation to perform on the input DataFrame
   * @param dfWithEvaluatedCondition source DataFrame on which we will apply the update expressions
   *                                 with an additional column CONDITION_COLUMN_NAME which is the
   *                                 true/false value of if the update condition is satisfied
   * @param condition update condition
   * @param shouldOutputCdc if we should output CDC data during this UPDATE operation.
   * @return the updated DataFrame, with extra CDC columns if CDC is enabled
   */
  def withUpdatedColumns(
      target: LogicalPlan,
      updateExpressions: Seq[Expression],
      condition: Expression,
      dfWithEvaluatedCondition: DataFrame,
      shouldOutputCdc: Boolean): DataFrame = {
    val resultDf = if (shouldOutputCdc) {
      val namedUpdateCols = updateExpressions.zip(target.output).map {
        case (expr, targetCol) => new Column(expr).as(targetCol.name)
      }

      // Build an array of output rows to be unpacked later. If the condition is matched, we
      // generate CDC pre and postimages in addition to the final output row; if the condition
      // isn't matched, we just generate a rewritten no-op row without any CDC events.
      val preimageCols = target.output.map(new Column(_)) :+
        lit(CDC_TYPE_UPDATE_PREIMAGE).as(CDC_TYPE_COLUMN_NAME)
      val postimageCols = namedUpdateCols :+
        lit(CDC_TYPE_UPDATE_POSTIMAGE).as(CDC_TYPE_COLUMN_NAME)
      val updatedDataCols = namedUpdateCols :+
        typedLit[String](CDC_TYPE_NOT_CDC).as(CDC_TYPE_COLUMN_NAME)
      val noopRewriteCols = target.output.map(new Column(_)) :+
        typedLit[String](CDC_TYPE_NOT_CDC).as(CDC_TYPE_COLUMN_NAME)
      val packedUpdates = array(
        struct(preimageCols: _*),
        struct(postimageCols: _*),
        struct(updatedDataCols: _*)
      ).expr

      val packedData = if (condition == Literal.TrueLiteral) {
        packedUpdates
      } else {
        If(
          UnresolvedAttribute(CONDITION_COLUMN_NAME),
          packedUpdates, // if it should be updated, then use `packagedUpdates`
          array(struct(noopRewriteCols: _*)).expr) // else, this is a noop rewrite
      }

      // Explode the packed array, and project back out the final data columns.
      val finalColNames = target.output.map(_.name) :+ CDC_TYPE_COLUMN_NAME
      dfWithEvaluatedCondition
        .select(explode(new Column(packedData)).as("packedData"))
        .select(finalColNames.map { n => col(s"packedData.`$n`").as(s"$n") }: _*)
    } else {
      val finalCols = updateExpressions.zip(target.output).map { case (update, original) =>
        val updated = if (condition == Literal.TrueLiteral) {
          update
        } else {
          If(UnresolvedAttribute(CONDITION_COLUMN_NAME), update, original)
        }
        new Column(Alias(updated, original.name)())
      }

      dfWithEvaluatedCondition.select(finalCols: _*)
    }

    resultDf.drop(CONDITION_COLUMN_NAME)
  }
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
    rewriteTimeMs: Long
)
