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

package org.apache.spark.sql.delta.commands.merge

import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Trait with optimized execution for merges that only inserts new data.
 * There are two cases for inserts only: when there are no matched clauses for the merge command
 * and when there is nothing matched for the merge command even if there are matched clauses.
 */
trait InsertOnlyMergeExecutor extends MergeOutputGeneration {
  self: MergeIntoCommandBase =>
  import MergeIntoCommandBase._

  /**
   * Optimization to write new files by inserting only new data.
   *
   * When there are no matched clauses for the merge command, data is skipped
   * based on the merge condition and left anti join is performed on the source
   * data to find the rows to be inserted.
   *
   * When there is nothing matched for the merge command even if there are matched clauses,
   * the source table is used to perform inserting.
   *
   * @param spark The spark session.
   * @param deltaTxn The existing transaction.
   * @param filterMatchedRows Whether to filter away matched data or not.
   * @param numSourceRowsMetric The name of the metric in which to record the number of source rows
   */
  protected def writeOnlyInserts(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      filterMatchedRows: Boolean,
      numSourceRowsMetric: String): Seq[FileAction] = {
    val extraOpType = if (filterMatchedRows) {
      "writeInsertsOnlyWhenNoMatchedClauses"
    } else "writeInsertsOnlyWhenNoMatches"
    recordMergeOperation(
      extraOpType = extraOpType,
      status = "MERGE operation - writing new files for only inserts",
      sqlMetricName = "rewriteTimeMs") {

      // If nothing to do when not matched, then nothing to insert, that is, no new files to write
      if (!includesInserts && !filterMatchedRows) {
        performedSecondSourceScan = false
        return Seq.empty
      }

      // source DataFrame
      val mergeSource = getMergeSource
      // Expression to update metrics.
      val incrSourceRowCountExpr = incrementMetricAndReturnBool(numSourceRowsMetric, true)
      val sourceDF = filterSource(mergeSource.df.filter(Column(incrSourceRowCountExpr)))

      var dataSkippedFiles: Option[Seq[AddFile]] = None
      val preparedSourceDF = if (filterMatchedRows) {
        // This is an optimization of the case when there is no update clause for the merge.
        // We perform an left anti join on the source data to find the rows to be inserted.

        // Skip data based on the merge condition
        val conjunctivePredicates = splitConjunctivePredicates(condition)
        val targetOnlyPredicates =
          conjunctivePredicates.filter(_.references.subsetOf(target.outputSet))
        dataSkippedFiles = Some(deltaTxn.filterFiles(targetOnlyPredicates))

        val targetPlan = buildTargetPlanWithFiles(
          spark,
          deltaTxn,
          dataSkippedFiles.get,
          columnsToDrop = Nil)
        val targetDF = Dataset.ofRows(spark, targetPlan)
        sourceDF.join(targetDF, Column(condition), "leftanti")
      } else {
        sourceDF
      }

      val outputDF = generateInsertsOnlyOutputDF(spark, preparedSourceDF, deltaTxn)
      logDebug(s"$extraOpType: output plan:\n" + outputDF.queryExecution)

      val newFiles = writeFiles(spark, deltaTxn, outputDF)

      // Update metrics
      if (filterMatchedRows) {
        metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
        metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
        if (dataSkippedFiles.nonEmpty) {
          val (afterSkippingBytes, afterSkippingPartitions) =
            totalBytesAndDistinctPartitionValues(dataSkippedFiles.get)
          metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.get.size
          metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
          metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
        }
        metrics("numTargetFilesRemoved") += 0
        metrics("numTargetBytesRemoved") += 0
        metrics("numTargetPartitionsRemovedFrom") += 0
      }
      metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
      val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
      metrics("numTargetBytesAdded") += addedBytes
      metrics("numTargetPartitionsAddedTo") += addedPartitions
      newFiles
    }
  }

  private def filterSource(source: DataFrame): DataFrame = {
    // If there is only one insert clause, then filter out the source rows that do not
    // satisfy the clause condition because those rows will not be written out.
    if (notMatchedClauses.size == 1 && notMatchedClauses.head.condition.isDefined) {
      source.filter(Column(notMatchedClauses.head.condition.get))
    } else {
      source
    }
  }

  /**
   * Generate the DataFrame to write out for merges that contains only inserts - either, insert-only
   * clauses or inserts when no matches were found.
   *
   * Specifically, it handles insert clauses in two cases: when there is only one insert clause,
   * and when there are multiple insert clauses.
   */
  private def generateInsertsOnlyOutputDF(
      spark: SparkSession,
      preparedSourceDF: DataFrame,
      deltaTxn: OptimisticTransaction): DataFrame = {

    val targetWriteColNames = deltaTxn.metadata.schema.map(_.name)

    // When there is only one insert clause, there is no need for ROW_DROPPED_COL and
    // output df can be generated without CaseWhen.
    if (notMatchedClauses.size == 1) {
      val outputCols = generateOneInsertOutputCols(targetWriteColNames)
      return preparedSourceDF.select(outputCols: _*)
    }

    // Precompute conditions in insert clauses and generate source data frame with precomputed
    // boolean columns and insert clauses with rewritten conditions.
    val (sourceWithPrecompConditions, insertClausesWithPrecompConditions) =
      generatePrecomputedConditionsAndDF(preparedSourceDF, notMatchedClauses)

    // Generate output cols.
    val outputCols = generateInsertsOnlyOutputCols(
      targetWriteColNames,
      insertClausesWithPrecompConditions
        .collect { case c: DeltaMergeIntoNotMatchedInsertClause => c })

    sourceWithPrecompConditions
      .select(outputCols: _*)
      .filter(s"$ROW_DROPPED_COL = false")
      .drop(ROW_DROPPED_COL)
  }

  /**
   * Generate output columns when there is only one insert clause.
   *
   * It assumes that the caller has already filtered out the source rows (`preparedSourceDF`)
   * that do not satisfy the insert clause condition (if any).
   * Then it simply applies the insertion action expression to generate
   * the output target table rows.
   */
  private def generateOneInsertOutputCols(
      targetWriteColNames: Seq[String]
    ): Seq[Column] = {

    val outputExprs = notMatchedClauses.head.resolvedActions.map(_.expr)
    assert(outputExprs.nonEmpty)
    // generate the outputDF without `CaseWhen` expressions.
    outputExprs.zip(targetWriteColNames).zipWithIndex.map { case ((expr, name), i) =>
      val exprAfterPassthru = if (i == 0) {
        IncrementMetric(expr, metrics("numTargetRowsInserted"))
      } else {
        expr
      }
      new Column(Alias(exprAfterPassthru, name)())
    }
  }

  /**
   * Generate the output columns for inserts only when there are multiple insert clauses.
   *
   * It combines all the conditions and corresponding actions expressions
   * into complicated CaseWhen expressions - one CaseWhen expression for
   * each column in the target row. If a source row does not match any of the clause conditions,
   * then the row will be dropped. These CaseWhen expressions basically look like this.
   *
   *    For the i-th output column,
   *    CASE
   *        WHEN [insert condition 1] THEN [execute i-th expression of insert action 1]
   *        WHEN [insert condition 2] THEN [execute i-th expression of insert action 2]
   *        ELSE [mark the source row to be dropped]
   */
  private def generateInsertsOnlyOutputCols(
      targetWriteColNames: Seq[String],
      insertClausesWithPrecompConditions: Seq[DeltaMergeIntoNotMatchedClause]
    ): Seq[Column] = {
    // ==== Generate the expressions to generate the target rows from the source rows ====
    // If there are N columns in the target table, there will be N + 1 columns generated
    // - N columns for target table
    // - ROW_DROPPED_COL to define whether the generated row should be dropped or written out
    // To generate these N + 1 columns, we will generate N + 1 expressions

    val outputColNames = targetWriteColNames :+ ROW_DROPPED_COL
    val numOutputCols = outputColNames.size

    // Generate the sequence of N + 1 expressions from the sequence of INSERT clauses
    val allInsertExprs: Seq[Seq[Expression]] =
      insertClausesWithPrecompConditions.map { clause =>
        clause.resolvedActions.map(_.expr) :+ incrementMetricAndReturnBool(
          "numTargetRowsInserted", false)
      }

    // Expressions to drop the source row when it does not match any of the insert clause
    // conditions. Note that it sets the N+1-th column ROW_DROPPED_COL to true.
    val dropSourceRowExprs =
      targetWriteColNames.map { _ => Literal(null)} :+ Literal.TrueLiteral

    // Generate the final N + 1 expressions to generate the final target output rows.
    // There are multiple not match clauses. Use `CaseWhen` to conditionally evaluate the right
    // action expressions to output columns.
    val outputExprs: Seq[Expression] = {
      val allInsertConditions =
        insertClausesWithPrecompConditions.map(_.condition.getOrElse(Literal.TrueLiteral))

      (0 until numOutputCols).map { i =>
        // For the i-th output column, generate
        // CASE
        //     WHEN <not match condition 1> THEN <execute i-th expression of action 1>
        //     WHEN <not match condition 2> THEN <execute i-th expression of action 2>
        //     ...
        //
        val conditionalBranches = allInsertConditions.zip(allInsertExprs).map {
          case (notMatchCond, notMatchActionExprs) => notMatchCond -> notMatchActionExprs(i)
        }
        CaseWhen(conditionalBranches, dropSourceRowExprs(i))
      }
    }

    assert(outputExprs.size == numOutputCols,
      s"incorrect # not matched expressions:\n\t" + seqToString(outputExprs))
    logDebug("prepareInsertsOnlyOutputDF: not matched expressions\n\t" +
      seqToString(outputExprs))

    outputExprs.zip(outputColNames).map { case (expr, name) =>
      new Column(Alias(expr, name)())
    }
  }
}
