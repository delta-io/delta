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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.{DeletionVectorBitmapGenerator, DMLWithDeletionVectorsHelper, MergeIntoCommandBase}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_TYPE_COLUMN_NAME, CDC_TYPE_NOT_CDC}
import org.apache.spark.sql.delta.commands.merge.MergeOutputGeneration.{SOURCE_ROW_INDEX_COL, TARGET_ROW_INDEX_COL}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.util.SetAccumulator

import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoClause
import org.apache.spark.sql.functions.{coalesce, col, count, input_file_name, lit, monotonically_increasing_id, sum}

/**
 * Trait with merge execution in two phases:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition (see [[findTouchedFiles]]).
 *    In the special case that there is no update clause we write all the non-matching
 *    source data as new files and skip phase 2.
 *    Issues an error message when the ON search_condition of the MERGE statement can match
 *    a single row from the target table with multiple rows of the source table-reference.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *    If there are updates, then use an outer join using the given condition to write the
 *    updates and inserts (see [[writeAllChanges()]]). If there are no matches for updates,
 *    only inserts, then write them directly (see [[writeInsertsOnlyWhenNoMatches()]]).
 *
 *    Note, when deletion vectors are enabled, phase 2 is split into two parts:
 *    2.a. Read the touched files again and only write modified and new
 *         rows (see [[writeAllChanges()]]).
 *    2.b. Read the touched files and generate deletion vectors for the modified
 *         rows (see [[writeDVs()]]).
 *
 * If there are no matches for updates, only inserts, then write them directly
 * (see [[writeInsertsOnlyWhenNoMatches()]]). This remains the same when DVs are enabled since there
 * are no modified rows. Furthermore, eee [[InsertOnlyMergeExecutor]] for the optimized executor
 * used in case there are only inserts.
 */
trait ClassicMergeExecutor extends MergeOutputGeneration {
  self: MergeIntoCommandBase =>
  import MergeIntoCommandBase._

  /**
   * Find the target table files that contain the rows that satisfy the merge condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the merge condition.
   */
  protected def findTouchedFiles(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction
    ): (Seq[AddFile], DeduplicateCDFDeletes) = recordMergeOperation(
      extraOpType = "findTouchedFiles",
      status = "MERGE operation - scanning files for matches",
      sqlMetricName = "scanTimeMs") {

    val columnComparator = spark.sessionState.analyzer.resolver

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

    // Prune non-matching files if we don't need to collect them for NOT MATCHED BY SOURCE clauses.
    val dataSkippedFiles =
      if (notMatchedBySourceClauses.isEmpty) {
        deltaTxn.filterFiles(getTargetOnlyPredicates(spark), keepNumRecords = true)
      } else {
        deltaTxn.filterFiles(filters = Seq(Literal.TrueLiteral), keepNumRecords = true)
      }

    // Join the source and target table using the merge condition to find touched files. An inner
    // join collects all candidate files for MATCHED clauses, a right outer join also includes
    // candidates for NOT MATCHED BY SOURCE clauses.
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val joinType = if (notMatchedBySourceClauses.isEmpty) "inner" else "right_outer"

    // When they are only MATCHED clauses, after the join we prune files that have no rows that
    // satisfy any of the clause conditions.
    val matchedPredicate =
      if (isMatchedOnly) {
        matchedClauses
          // An undefined condition (None) is implicitly true
          .map(_.condition.getOrElse(Literal.TrueLiteral))
          .reduce((a, b) => Or(a, b))
      } else Literal.TrueLiteral

    // Compute the columns needed for the inner join.
    val targetColsNeeded = {
      condition.references.map(_.name) ++ deltaTxn.snapshot.metadata.partitionColumns ++
        matchedPredicate.references.map(_.name)
    }

    val columnsToDrop = deltaTxn.snapshot.metadata.schema.map(_.name)
      .filterNot { field =>
        targetColsNeeded.exists { name => columnComparator(name, field) }
      }
    val incrSourceRowCountExpr = incrementMetricAndReturnBool("numSourceRows", valueToReturn = true)
    // We can't use filter() directly on the expression because that will prevent
    // column pruning. We don't need the SOURCE_ROW_PRESENT_COL so we immediately drop it.
    val sourceDF = getMergeSource.df
      .withColumn(SOURCE_ROW_PRESENT_COL, Column(incrSourceRowCountExpr))
      .filter(SOURCE_ROW_PRESENT_COL)
      .drop(SOURCE_ROW_PRESENT_COL)
    val targetPlan =
      buildTargetPlanWithFiles(
        spark,
        deltaTxn,
        dataSkippedFiles,
        columnsToDrop)
    val targetDF = Dataset.ofRows(spark, targetPlan)
      .withColumn(ROW_ID_COL, monotonically_increasing_id())
      .withColumn(FILE_NAME_COL, input_file_name())

    val joinToFindTouchedFiles =
      sourceDF.join(targetDF, Column(condition), joinType)

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName =
      DeltaUDF.intFromStringBoolean { (fileName, shouldRecord) =>
        if (shouldRecord) {
          touchedFilesAccum.add(fileName)
        }
        1
      }.asNondeterministic()

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL),
        recordTouchedFileName(col(FILE_NAME_COL), Column(matchedPredicate)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))

    // Get multiple matches and simultaneously collect (using touchedFilesAccum) the file names
    import org.apache.spark.sql.delta.implicits._
    val (multipleMatchCount, multipleMatchSum) = matchedRowCounts
      .filter("count > 1")
      .select(coalesce(count(Column("*")), lit(0)), coalesce(sum("count"), lit(0)))
      .as[(Long, Long)]
      .collect()
      .head

    val hasMultipleMatches = multipleMatchCount > 0
    throwErrorOnMultipleMatches(hasMultipleMatches, spark)
    if (hasMultipleMatches) {
      // This is only allowed for delete-only queries.
      // This query will count the duplicates for numTargetRowsDeleted in Job 2,
      // because we count matches after the join and not just the target rows.
      // We have to compensate for this by subtracting the duplicates later,
      // so we need to record them here.
      val duplicateCount = multipleMatchSum - multipleMatchCount
      multipleMatchDeleteOnlyOvercount = Some(duplicateCount)
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.map(
      getTouchedFile(targetDeltaLog.dataPath, _, nameToAddFileMap))

    if (metrics("numSourceRows").value == 0 && (dataSkippedFiles.isEmpty ||
      dataSkippedFiles.forall(_.numLogicalRecords.getOrElse(0) == 0))) {
      // The target table is empty, and the optimizer optimized away the join entirely OR the
      // source table is truly empty. In that case, scanning the source table once is the only
      // way to get the correct metric.
      val numSourceRows = sourceDF.count()
      metrics("numSourceRows").set(numSourceRows)
    }

    metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
    val (afterSkippingBytes, afterSkippingPartitions) =
      totalBytesAndDistinctPartitionValues(dataSkippedFiles)
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
    metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
    val (removedBytes, removedPartitions) = totalBytesAndDistinctPartitionValues(touchedAddFiles)
    metrics("numTargetFilesRemoved") += touchedAddFiles.size
    metrics("numTargetBytesRemoved") += removedBytes
    metrics("numTargetPartitionsRemovedFrom") += removedPartitions
    val dedupe = DeduplicateCDFDeletes(
      hasMultipleMatches && isCdcEnabled(deltaTxn),
      includesInserts)
    (touchedAddFiles, dedupe)
  }

  /**
   * Helper function that produces an expression by combining a sequence of clauses with OR.
   * Requires the sequence to be non-empty.
   */
  protected def clauseDisjunction(clauses: Seq[DeltaMergeIntoClause]): Expression = {
    require(clauses.nonEmpty)
    clauses
      .map(_.condition.getOrElse(Literal.TrueLiteral))
      .reduceLeft(Or)
  }

  /**
   * Returns the expression that can be used for selecting the modified rows generated
   * by the merge operation. The expression is to designed to work irrespectively
   * of the join type used between the source and target tables.
   *
   * The expression consists of two parts, one for each of the action clause types that produce
   * row modifications: MATCHED, NOT MATCHED BY SOURCE. All actions of the same clause type form
   * a disjunctive clause. The result is then conjucted to an expression that filters the rows
   * of the particular action clause type. For example:
   *
   * MERGE INTO t
   * USING s
   * ON s.id = t.id
   * WHEN MATCHED AND id < 5 THEN ...
   * WHEN MATCHED AND id > 10 THEN ...
   * WHEN NOT MATCHED BY SOURCE AND id > 20 THEN ...
   *
   * Produces the following expression:
   *
   * ((as.id = t.id) AND (id < 5 OR id > 10))
   * OR
   * ((SOURCE TABLE IS NULL) AND (id > 20))
   */
  protected def generateFilterForModifiedRows(): Expression = {
    val matchedExpression = if (matchedClauses.nonEmpty) {
      And(Column(condition).expr, clauseDisjunction(matchedClauses))
    } else {
      Literal.FalseLiteral
    }

    val notMatchedBySourceExpression = if (notMatchedBySourceClauses.nonEmpty) {
      val combinedClauses = clauseDisjunction(notMatchedBySourceClauses)
      And(col(SOURCE_ROW_PRESENT_COL).isNull.expr, combinedClauses)
    } else {
      Literal.FalseLiteral
    }

    Or(matchedExpression, notMatchedBySourceExpression)
  }

  /**
   * Returns the expression that can be used for selecting the new rows generated
   * by the merge operation.
   */
  protected def generateFilterForNewRows(): Expression = {
    if (notMatchedClauses.nonEmpty) {
      val combinedClauses = clauseDisjunction(notMatchedClauses)
      And(col(TARGET_ROW_PRESENT_COL).isNull.expr, combinedClauses)
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full-outer-join using the merge condition.
   *
   * Note that unlike the insert-only code paths with just one control column ROW_DROPPED_COL, this
   * method has a second control column CDC_TYPE_COL_NAME used for handling CDC when enabled.
   */
  protected def writeAllChanges(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      filesToRewrite: Seq[AddFile],
      deduplicateCDFDeletes: DeduplicateCDFDeletes,
      writeUnmodifiedRows: Boolean): Seq[FileAction] = recordMergeOperation(
        extraOpType = if (!writeUnmodifiedRows) {
            "writeModifiedRowsOnly"
          } else if (shouldOptimizeMatchedOnlyMerge(spark)) {
            "writeAllUpdatesAndDeletes"
          } else {
            "writeAllChanges"
          },
        status = s"MERGE operation - Rewriting ${filesToRewrite.size} files",
        sqlMetricName = "rewriteTimeMs") {

      val cdcEnabled = isCdcEnabled(deltaTxn)

      require(
        !deduplicateCDFDeletes.enabled || cdcEnabled,
        "CDF delete duplication is enabled but overall the CDF generation is disabled")

    // Generate a new target dataframe that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val targetPlan = buildTargetPlanWithFiles(
      spark,
      deltaTxn,
      filesToRewrite,
      columnsToDrop = Nil)
    val baseTargetDF = RowTracking.preserveRowTrackingColumns(
      dfWithoutRowTrackingColumns = Dataset.ofRows(spark, targetPlan),
      snapshot = deltaTxn.snapshot)

    val joinType = if (writeUnmodifiedRows) {
      if (shouldOptimizeMatchedOnlyMerge(spark)) {
        "rightOuter"
      } else {
        "fullOuter"
      }
    } else {
      // Since we do not need to write unmodified rows, we can perform stricter joins.
      if (isMatchedOnly) {
        "inner"
      } else if (notMatchedBySourceClauses.isEmpty) {
        "leftOuter"
      } else if (notMatchedClauses.isEmpty) {
        "rightOuter"
      } else {
        "fullOuter"
      }
    }

    logDebug(s"""writeAllChanges using $joinType join:
       |  source.output: ${source.outputSet}
       |  target.output: ${target.outputSet}
       |  condition: $condition
       |  newTarget.output: ${baseTargetDF.queryExecution.logical.outputSet}
       """.stripMargin)

    // Expressions to update metrics
    val incrSourceRowCountExpr = incrementMetricAndReturnBool(
      "numSourceRowsInSecondScan", valueToReturn = true)
    val incrNoopCountExpr = incrementMetricAndReturnBool(
      "numTargetRowsCopied", valueToReturn = false)

    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the outer join, will allow us to identify whether the joined row was a
    // matched inner result or an unmatched result with null on one side.
    val joinedBaseDF = {
      var sourceDF = getMergeSource.df
      if (deduplicateCDFDeletes.enabled && deduplicateCDFDeletes.includesInserts) {
        // Add row index for the source rows to identify inserted rows during the cdf deleted rows
        // deduplication. See [[deduplicateCDFDeletes()]]
        sourceDF = sourceDF.withColumn(SOURCE_ROW_INDEX_COL, monotonically_increasing_id())
      }
      val left = sourceDF
          .withColumn(SOURCE_ROW_PRESENT_COL, Column(incrSourceRowCountExpr))
          // In some cases, the optimizer (incorrectly) decides to omit the metrics column.
          // This causes issues in the source determinism validation. We work around the issue by
          // adding a redundant dummy filter to make sure the column is not pruned.
          .filter(SOURCE_ROW_PRESENT_COL)

      val targetDF = baseTargetDF
        .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
      val right = if (deduplicateCDFDeletes.enabled) {
        targetDF.withColumn(TARGET_ROW_INDEX_COL, monotonically_increasing_id())
      } else {
        targetDF
      }
      left.join(right, Column(condition), joinType)
    }

    val joinedDF =
      if (writeUnmodifiedRows) {
        joinedBaseDF
      } else {
        val filter = Or(generateFilterForModifiedRows(), generateFilterForNewRows())
        joinedBaseDF.filter(Column(filter))
      }

    // Precompute conditions in matched and not matched clauses and generate
    // the joinedDF with precomputed columns and clauses with rewritten conditions.
    val (joinedAndPrecomputedConditionsDF, clausesWithPrecompConditions) =
        generatePrecomputedConditionsAndDF(
          joinedDF,
          clauses = matchedClauses ++ notMatchedClauses ++ notMatchedBySourceClauses)

    // In case Row IDs are preserved, get the attribute expression of the Row ID column.
    val rowIdColumnExpressionOpt =
      MaterializedRowId.getAttribute(deltaTxn.snapshot, joinedAndPrecomputedConditionsDF)

    val rowCommitVersionColumnExpressionOpt =
      MaterializedRowCommitVersion.getAttribute(deltaTxn.snapshot, joinedAndPrecomputedConditionsDF)

    // The target output columns need to be marked as nullable here, as they are going to be used
    // to reference the output of an outer join.
    val targetWriteCols = postEvolutionTargetExpressions(makeNullable = true)

    // If there are N columns in the target table, the full outer join output will have:
    // - N columns for target table
    // - Two optional Row ID / Row commit version preservation columns with their physical name.
    // - ROW_DROPPED_COL to define whether the generated row should be dropped or written
    // - if CDC is enabled, also CDC_TYPE_COLUMN_NAME with the type of change being performed
    //   in a particular row
    // (N+1 or N+2 columns depending on CDC disabled / enabled)
    val outputColNames =
      targetWriteCols.map(_.name) ++
        rowIdColumnExpressionOpt.map(_.name) ++
        rowCommitVersionColumnExpressionOpt.map(_.name) ++
        Seq(ROW_DROPPED_COL) ++
        (if (cdcEnabled) Some(CDC_TYPE_COLUMN_NAME) else None)

    // Copy expressions to copy the existing target row and not drop it (ROW_DROPPED_COL=false),
    // and in case CDC is enabled, set it to CDC_TYPE_NOT_CDC.
    // (N+1 or N+2 or N+3 columns depending on CDC disabled / enabled and if Row IDs are preserved)
    val noopCopyExprs =
      targetWriteCols ++
        rowIdColumnExpressionOpt ++
        rowCommitVersionColumnExpressionOpt ++
        Seq(incrNoopCountExpr) ++
        (if (cdcEnabled) Seq(CDC_TYPE_NOT_CDC) else Seq())

    // Generate output columns.
    val outputCols = generateWriteAllChangesOutputCols(
      targetWriteCols,
      rowIdColumnExpressionOpt,
      rowCommitVersionColumnExpressionOpt,
      outputColNames,
      noopCopyExprs,
      clausesWithPrecompConditions,
      cdcEnabled
    )

    val preOutputDF = if (cdcEnabled) {
      generateCdcAndOutputRows(
          joinedAndPrecomputedConditionsDF,
          outputCols,
          outputColNames,
          noopCopyExprs,
          rowIdColumnExpressionOpt.map(_.name),
          rowCommitVersionColumnExpressionOpt.map(_.name),
          deduplicateCDFDeletes)
    } else {
      // change data capture is off, just output the normal data
      joinedAndPrecomputedConditionsDF
        .select(outputCols: _*)
    }
    // The filter ensures we only consider rows that are not dropped.
    // The drop ensures that the dropped flag does not leak out to the output.
    val outputDF = preOutputDF
      .filter(s"$ROW_DROPPED_COL = false")
      .drop(ROW_DROPPED_COL)

    logDebug("writeAllChanges: join output plan:\n" + outputDF.queryExecution)

    // Write to Delta
    val newFiles = writeFiles(spark, deltaTxn, outputDF)

    // Update metrics
    val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
    metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
    metrics("numTargetChangeFilesAdded") += newFiles.count(_.isInstanceOf[AddCDCFile])
    metrics("numTargetChangeFileBytes") += newFiles.collect{ case f: AddCDCFile => f.size }.sum
    metrics("numTargetBytesAdded") += addedBytes
    metrics("numTargetPartitionsAddedTo") += addedPartitions
    if (multipleMatchDeleteOnlyOvercount.isDefined) {
      // Compensate for counting duplicates during the query.
      val actualRowsDeleted =
        metrics("numTargetRowsDeleted").value - multipleMatchDeleteOnlyOvercount.get
      assert(actualRowsDeleted >= 0)
      metrics("numTargetRowsDeleted").set(actualRowsDeleted)
      val actualRowsMatchedDeleted =
        metrics("numTargetRowsMatchedDeleted").value - multipleMatchDeleteOnlyOvercount.get
      assert(actualRowsMatchedDeleted >= 0)
      metrics("numTargetRowsMatchedDeleted").set(actualRowsMatchedDeleted)
    }

    newFiles
  }

  /**
   * Writes Deletion Vectors for rows modified by the merge operation.
   */
  protected def writeDVs(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    filesToRewrite: Seq[AddFile]): Seq[FileAction] = recordMergeOperation(
      extraOpType = "writeDeletionVectors",
      status = s"MERGE operation - Rewriting Deletion Vectors to ${filesToRewrite.size} files",
      sqlMetricName = "rewriteTimeMs") {

    val fileIndex = new TahoeBatchFileIndex(
      spark,
      actionType = "merge",
      addFiles = filesToRewrite,
      deltaLog = deltaTxn.deltaLog,
      path = deltaTxn.deltaLog.dataPath,
      snapshot = deltaTxn.snapshot)

    val targetDF = DMLWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
      spark,
      target,
      fileIndex)

    // For writing DVs we are only interested in the target table. When there are no
    // notMatchedBySource clauses an inner join is sufficient. Otherwise, we need an rightOuter
    // join to include target rows that are not matched.
    val joinType = if (notMatchedBySourceClauses.isEmpty) {
      "inner"
    } else {
      "rightOuter"
    }

    val joinedDF = getMergeSource.df
      .withColumn(SOURCE_ROW_PRESENT_COL, lit(true))
      .join(targetDF, Column(condition), joinType)

    val modifiedRowsFilter = generateFilterForModifiedRows()
    val matchedDVResult =
      DeletionVectorBitmapGenerator.buildRowIndexSetsForFilesMatchingCondition(
        spark,
        deltaTxn,
        tableHasDVs = true,
        targetDf = joinedDF,
        candidateFiles = filesToRewrite,
        condition = modifiedRowsFilter
      )

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, filesToRewrite)

    val touchedFilesWithDVs = DMLWithDeletionVectorsHelper
      .findFilesWithMatchingRows(deltaTxn, nameToAddFileMap, matchedDVResult)

    val (dvActions, metricsMap) = DMLWithDeletionVectorsHelper.processUnmodifiedData(
      spark,
      touchedFilesWithDVs,
      deltaTxn.snapshot)

    metrics("numTargetDeletionVectorsAdded")
      .set(metricsMap.getOrElse("numDeletionVectorsAdded", 0L))
    metrics("numTargetDeletionVectorsRemoved")
      .set(metricsMap.getOrElse("numDeletionVectorsRemoved", 0L))
    metrics("numTargetDeletionVectorsUpdated")
      .set(metricsMap.getOrElse("numDeletionVectorsUpdated", 0L))

    // When DVs are enabled we override metrics related to removed files.
    metrics("numTargetFilesRemoved").set(metricsMap.getOrElse("numRemovedFiles", 0L))

    val fullyRemovedFiles = touchedFilesWithDVs.filter(_.isFullyReplaced()).map(_.fileLogEntry)
    val (removedBytes, removedPartitions) = totalBytesAndDistinctPartitionValues(fullyRemovedFiles)
    metrics("numTargetBytesRemoved").set(removedBytes)
    metrics("numTargetPartitionsRemovedFrom").set(removedPartitions)

    dvActions
  }
}
