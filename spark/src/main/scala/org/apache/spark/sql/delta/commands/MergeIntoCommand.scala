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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase.ROW_DROPPED_COL
import org.apache.spark.sql.delta.commands.merge.InsertOnlyMergeExecutor
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType, StructType}

/**
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 *    for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source                     Source data to merge from
 * @param target                     Target table to merge into
 * @param targetFileIndex            TahoeFileIndex of the target table
 * @param condition                  Condition for a source row to match with a target row
 * @param matchedClauses             All info related to matched clauses.
 * @param notMatchedClauses          All info related to not matched clauses.
 * @param notMatchedBySourceClauses  All info related to not matched by source clauses.
 * @param migratedSchema             The final schema of the target - may be changed by schema
 *                                   evolution.
 */
case class MergeIntoCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
    notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
    migratedSchema: Option[StructType]) extends MergeIntoCommandBase
  with AnalysisHelper
  with InsertOnlyMergeExecutor
  with ImplicitMetadataOperation {

  import MergeIntoCommand._
  import MergeIntoCommandBase.totalBytesAndDistinctPartitionValues

  import org.apache.spark.sql.delta.commands.cdc.CDCReader._

  override val canMergeSchema: Boolean = conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  // We over-count numTargetRowsDeleted when there are multiple matches;
  // this is the amount of the overcount, so we can subtract it to get a correct final metric.
  private var multipleMatchDeleteOnlyOvercount: Option[Long] = None
  override def run(spark: SparkSession): Seq[Row] = {
    metrics("executionTimeMs").set(0)
    metrics("scanTimeMs").set(0)
    metrics("rewriteTimeMs").set(0)

    if (migratedSchema.isDefined) {
      // Block writes of void columns in the Delta log. Currently void columns are not properly
      // supported and are dropped on read, but this is not enough for merge command that is also
      // reading the schema from the Delta log. Until proper support we prefer to fail merge
      // queries that add void columns.
      val newNullColumn = SchemaUtils.findNullTypeColumn(migratedSchema.get)
      if (newNullColumn.isDefined) {
        throw new AnalysisException(
          s"""Cannot add column '${newNullColumn.get}' with type 'void'. Please explicitly specify a
              |non-void type.""".stripMargin.replaceAll("\n", " ")
        )
      }
    }
    val (materializeSource, _) = shouldMaterializeSource(spark, source, isInsertOnly)
    if (!materializeSource) {
      runMerge(spark)
    } else {
      // If it is determined that source should be materialized, wrap the execution with retries,
      // in case the data of the materialized source is lost.
      runWithMaterializedSourceLostRetries(
        spark, targetFileIndex.deltaLog, metrics, runMerge)
    }
  }

  protected def runMerge(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
      val startTime = System.nanoTime()
      targetDeltaLog.withNewTransaction { deltaTxn =>
        if (hasBeenExecuted(deltaTxn, spark)) {
          sendDriverMetrics(spark, metrics)
          return Seq.empty
        }
        if (target.schema.size != deltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = deltaTxn.metadata.schema)
        }

        if (canMergeSchema) {
          updateMetadata(
            spark, deltaTxn, migratedSchema.getOrElse(target.schema),
            deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }

        // If materialized, prepare the DF reading the materialize source
        // Otherwise, prepare a regular DF from source plan.
        val materializeSourceReason = prepareSourceDFAndReturnMaterializeReason(
          spark,
          source,
          condition,
          matchedClauses,
          notMatchedClauses,
          isInsertOnly)

        val deltaActions = {
          if (isInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            // This is a single-job execution so there is no WriteChanges.
            metrics("numSourceRowsInSecondScan").set(-1)
            writeOnlyInserts(
              spark, deltaTxn, filterMatchedRows = true, numSourceRowsMetric = "numSourceRows")
          } else {
            val filesToRewrite = findTouchedFiles(spark, deltaTxn)
            if (filesToRewrite.nonEmpty) {
              val newWrittenFiles = withStatusCode("DELTA", "Writing merged data") {
                writeAllChanges(spark, deltaTxn, filesToRewrite)
              }
              filesToRewrite.map(_.remove) ++ newWrittenFiles
            } else {
              // Run an insert-only job instead of WriteChanges
              writeOnlyInserts(
                spark,
                deltaTxn,
                filterMatchedRows = false,
                numSourceRowsMetric = "numSourceRowsInSecondScan")
            }
          }
        }
        val stats = collectMergeStats(
          spark,
          deltaTxn,
          startTime,
          deltaActions,
          materializeSourceReason)

        recordDeltaEvent(targetFileIndex.deltaLog, "delta.dml.merge.stats", data = stats)

      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
    }
    sendDriverMetrics(spark, metrics)
    Seq(Row(metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value +
            metrics("numTargetRowsInserted").value, metrics("numTargetRowsUpdated").value,
            metrics("numTargetRowsDeleted").value, metrics("numTargetRowsInserted").value))
  }

  /**
   * Find the target table files that contain the rows that satisfy the merge condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the merge condition.
   */
  private def findTouchedFiles(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction)
    : Seq[AddFile] = recordMergeOperation(
      extraOpType = "findTouchedFiles",
      status = "MERGE operation - scanning files for matches",
      sqlMetricName = "scanTimeMs") {

    val columnComparator = spark.sessionState.analyzer.resolver

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName =
      DeltaUDF.intFromStringBoolean { (fileName, shouldRecord) => {
        if (shouldRecord) {
          touchedFilesAccum.add(fileName)
        }
        1
      }}.asNondeterministic()

    // Prune non-matching files if we don't need to collect them for NOT MATCHED BY SOURCE clauses.
    val dataSkippedFiles =
      if (notMatchedBySourceClauses.isEmpty) {
        deltaTxn.filterFiles(getTargetOnlyPredicates(spark))
      } else {
        deltaTxn.filterFiles()
      }

    val incrSourceRowCountExpr = incrementMetricAndReturnBool("numSourceRows", valueToReturn = true)

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
          .map(clause => clause.condition.getOrElse(Literal(true)))
          .reduce((a, b) => Or(a, b))
      } else Literal(true)

    // Compute the columns needed for the inner join.
    val targetColsNeeded = {
      condition.references.map(_.name) ++ deltaTxn.snapshot.metadata.partitionColumns ++
        matchedPredicate.references.map(_.name)
    }

    val columnsToDrop = deltaTxn.snapshot.metadata.schema.map(_.name)
      .filterNot { field =>
        targetColsNeeded.exists { name => columnComparator(name, field) }
      }

    // We can't use filter() directly on the expression because that will prevent
    // column pruning. We don't need the SOURCE_ROW_PRESENT_COL so we immediately drop it.
    val sourceDF = getSourceDF()
      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
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

    val joinToFindTouchedFiles = sourceDF.join(targetDF, new Column(condition), joinType)

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL),
        recordTouchedFileName(col(FILE_NAME_COL), new Column(matchedPredicate)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))

    // Get multiple matches and simultaneously collect (using touchedFilesAccum) the file names
    // multipleMatchCount = # of target rows with more than 1 matching source row (duplicate match)
    // multipleMatchSum = total # of duplicate matched rows
    import org.apache.spark.sql.delta.implicits._
    val (multipleMatchCount, multipleMatchSum) = matchedRowCounts
      .filter("count > 1")
      .select(coalesce(count(new Column("*")), lit(0)), coalesce(sum("count"), lit(0)))
      .as[(Long, Long)]
      .collect()
      .head

    val hasMultipleMatches = multipleMatchCount > 0

    // Throw error if multiple matches are ambiguous or cannot be computed correctly.
    val canBeComputedUnambiguously = {
      // Multiple matches are not ambiguous when there is only one unconditional delete as
      // all the matched row pairs in the 2nd join in `writeAllChanges` will get deleted.
      val isUnconditionalDelete = matchedClauses.headOption match {
        case Some(DeltaMergeIntoMatchedDeleteClause(None)) => true
        case _ => false
      }
      matchedClauses.size == 1 && isUnconditionalDelete
    }

    if (hasMultipleMatches && !canBeComputedUnambiguously) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
    }

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
    val touchedAddFiles = touchedFileNames.map(f =>
      getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    // When the target table is empty, and the optimizer optimized away the join entirely
    // numSourceRows will be incorrectly 0. We need to scan the source table once to get the correct
    // metric here.
    if (metrics("numSourceRows").value == 0 &&
      (dataSkippedFiles.isEmpty || targetDF.take(1).isEmpty)) {
      val numSourceRows = sourceDF.count()
      metrics("numSourceRows").set(numSourceRows)
    }

    // Update metrics
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
    touchedAddFiles
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full|right-outer-join using the merge condition.
   *
   * Note that unlike the insert-only code paths with just one control column INCR_ROW_COUNT_COL,
   * this method has two additional control columns ROW_DROPPED_COL for dropping deleted rows and
   * CDC_TYPE_COL_NAME used for handling CDC when enabled.
   */
  private def writeAllChanges(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    filesToRewrite: Seq[AddFile])
    : Seq[FileAction] = recordMergeOperation(
      extraOpType =
        if (shouldOptimizeMatchedOnlyMerge(spark)) "writeAllUpdatesAndDeletes"
        else "writeAllChanges",
      status = s"MERGE operation - Rewriting ${filesToRewrite.size} files",
      sqlMetricName = "rewriteTimeMs") {
    import org.apache.spark.sql.catalyst.expressions.Literal.{TrueLiteral, FalseLiteral}

    val cdcEnabled = DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(deltaTxn.metadata)

    var targetOutputCols = getTargetOutputCols(deltaTxn, makeNullable = true)
    var outputRowSchema = deltaTxn.metadata.schema

    // When we have duplicate matches (only allowed when the whenMatchedCondition is a delete with
    // no match condition) we will incorrectly generate duplicate CDC rows.
    // Duplicate matches can be due to:
    // - Duplicate rows in the source w.r.t. the merge condition
    // - A target-only or source-only merge condition, which essentially turns our join into a cross
    //   join with the target/source satisfiying the merge condition.
    // These duplicate matches are dropped from the main data output since this is a delete
    // operation, but the duplicate CDC rows are not removed by default.
    // See https://github.com/delta-io/delta/issues/1274

    // We address this specific scenario by adding row ids to the target before performing our join.
    // There should only be one CDC delete row per target row so we can use these row ids to dedupe
    // the duplicate CDC delete rows.

    // We also need to address the scenario when there are duplicate matches with delete and we
    // insert duplicate rows. Here we need to additionally add row ids to the source before the
    // join to avoid dropping these valid duplicate inserted rows and their corresponding cdc rows.

    // When there is an insert clause, we set SOURCE_ROW_ID_COL=null for all delete rows because we
    // need to drop the duplicate matches.
    val isDeleteWithDuplicateMatchesAndCdc = multipleMatchDeleteOnlyOvercount.nonEmpty && cdcEnabled

    // Generate a new target dataframe that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val targetPlan = buildTargetPlanWithFiles(
      spark,
      deltaTxn,
      filesToRewrite,
      columnsToDrop = Nil)
    val baseTargetDF = Dataset.ofRows(spark, targetPlan)
    val joinType = if (shouldOptimizeMatchedOnlyMerge(spark)) {
      "rightOuter"
    } else {
      "fullOuter"
    }

    logDebug(s"""writeAllChanges using $joinType join:
                |  source.output: ${source.outputSet}
                |  target.output: ${target.outputSet}
                |  condition: $condition
                |  newTarget.output: ${baseTargetDF.queryExecution.logical.outputSet}
       """.stripMargin)

    // Expressions to update metrics
    val incrSourceRowCountExpr =
      incrementMetricAndReturnBool("numSourceRowsInSecondScan", valueToReturn = true)
    val incrUpdatedCountExpr =
      incrementMetricAndReturnBool("numTargetRowsUpdated", valueToReturn = true)
    val incrUpdatedMatchedCountExpr =
      incrementMetricAndReturnBool("numTargetRowsMatchedUpdated", valueToReturn = true)
    val incrUpdatedNotMatchedBySourceCountExpr =
      incrementMetricAndReturnBool("numTargetRowsNotMatchedBySourceUpdated", valueToReturn = true)
    val incrInsertedCountExpr =
      incrementMetricAndReturnBool("numTargetRowsInserted", valueToReturn = true)
    val incrNoopCountExpr =
      incrementMetricAndReturnBool("numTargetRowsCopied", valueToReturn = true)
    val incrDeletedCountExpr =
      incrementMetricAndReturnBool("numTargetRowsDeleted", valueToReturn = true)
    val incrDeletedMatchedCountExpr =
      incrementMetricAndReturnBool("numTargetRowsMatchedDeleted", valueToReturn = true)
    val incrDeletedNotMatchedBySourceCountExpr =
      incrementMetricAndReturnBool("numTargetRowsNotMatchedBySourceDeleted", valueToReturn = true)

    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the outer join, will allow us to identify whether the resultant joined row was a
    // matched inner result or an unmatched result with null on one side.
    // We add row IDs to the targetDF if we have a delete-when-matched clause with duplicate
    // matches and CDC is enabled, and additionally add row IDs to the source if we also have an
    // insert clause. See above at isDeleteWithDuplicateMatchesAndCdc definition for more details.
    var sourceDF = getSourceDF()
      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
    var targetDF = baseTargetDF
      .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
    if (isDeleteWithDuplicateMatchesAndCdc) {
      targetDF = targetDF.withColumn(TARGET_ROW_ID_COL, monotonically_increasing_id())
      if (notMatchedClauses.nonEmpty) { // insert clause
        sourceDF = sourceDF.withColumn(SOURCE_ROW_ID_COL, monotonically_increasing_id())
      }
    }
    val joinedDF = sourceDF.join(targetDF, new Column(condition), joinType)
    val joinedPlan = joinedDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      tryResolveReferencesForExpressions(spark, exprs, joinedPlan)
    }

    // ==== Generate the expressions to process full-outer join output and generate target rows ====
    // If there are N columns in the target table, there will be N + 3 columns after processing
    // - N columns for target table
    // - ROW_DROPPED_COL to define whether the generated row should dropped or written
    // - INCR_ROW_COUNT_COL containing an expression to update the output row row counter
    // - CDC_TYPE_COLUMN_NAME containing the type of change being performed in a particular row

    // To generate these N + 3 columns, we will generate N + 3 expressions and apply them to the
    // rows in the joinedDF. The CDC column will be either used for CDC generation or dropped before
    // performing the final write, and the other two will always be dropped after executing the
    // metrics expressions and filtering on ROW_DROPPED_COL.

    // We produce rows for both the main table data (with CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC),
    // and rows for the CDC data which will be output to CDCReader.CDC_LOCATION.
    // See [[CDCReader]] for general details on how partitioning on the CDC type column works.

    // In the following functions `updateOutput`, `deleteOutput` and `insertOutput`, we
    // produce a Seq[Expression] for each intended output row.
    // Depending on the clause and whether CDC is enabled, we output between 0 and 3 rows, as a
    // Seq[Seq[Expression]]

    // There is one corner case outlined above at isDeleteWithDuplicateMatchesAndCdc definition.
    // When we have a delete-ONLY merge with duplicate matches we have N + 4 columns:
    // N target cols, TARGET_ROW_ID_COL, ROW_DROPPED_COL, INCR_ROW_COUNT_COL, CDC_TYPE_COLUMN_NAME
    // When we have a delete-when-matched merge with duplicate matches + an insert clause, we have
    // N + 5 columns:
    // N target cols, TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL, ROW_DROPPED_COL, INCR_ROW_COUNT_COL,
    // CDC_TYPE_COLUMN_NAME
    // These ROW_ID_COL will always be dropped before the final write.

    if (isDeleteWithDuplicateMatchesAndCdc) {
      targetOutputCols = targetOutputCols :+ UnresolvedAttribute(TARGET_ROW_ID_COL)
      outputRowSchema = outputRowSchema.add(TARGET_ROW_ID_COL, DataTypes.LongType)
      if (notMatchedClauses.nonEmpty) { // there is an insert clause, make SRC_ROW_ID_COL=null
        targetOutputCols = targetOutputCols :+ Alias(Literal(null), SOURCE_ROW_ID_COL)()
        outputRowSchema = outputRowSchema.add(SOURCE_ROW_ID_COL, DataTypes.LongType)
      }
    }

    if (cdcEnabled) {
      outputRowSchema = outputRowSchema
        .add(ROW_DROPPED_COL, DataTypes.BooleanType)
        .add(INCR_ROW_COUNT_COL, DataTypes.BooleanType)
        .add(CDC_TYPE_COLUMN_NAME, DataTypes.StringType)
    }

    def updateOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
      : Seq[Seq[Expression]] = {
      val updateExprs = {
        // Generate update expressions and set ROW_DELETED_COL = false and
        // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC
        val mainDataOutput = resolvedActions.map(_.expr) :+ FalseLiteral :+
          incrMetricExpr :+ CDC_TYPE_NOT_CDC
        if (cdcEnabled) {
          // For update preimage, we have do a no-op copy with ROW_DELETED_COL = false and
          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_PREIMAGE and INCR_ROW_COUNT_COL as a no-op
          // (because the metric will be incremented in `mainDataOutput`)
          val preImageOutput = targetOutputCols :+ FalseLiteral :+ TrueLiteral :+
            Literal(CDC_TYPE_UPDATE_PREIMAGE)
          // For update postimage, we have the same expressions as for mainDataOutput but with
          // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
          // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_POSTIMAGE
          val postImageOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+
            Literal(CDC_TYPE_UPDATE_POSTIMAGE)
          Seq(mainDataOutput, preImageOutput, postImageOutput)
        } else {
          Seq(mainDataOutput)
        }
      }
      updateExprs.map(resolveOnJoinedPlan)
    }

    def deleteOutput(incrMetricExpr: Expression): Seq[Seq[Expression]] = {
      val deleteExprs = {
        // Generate expressions to set the ROW_DELETED_COL = true and CDC_TYPE_COLUMN_NAME =
        // CDC_TYPE_NOT_CDC
        val mainDataOutput = targetOutputCols :+ TrueLiteral :+ incrMetricExpr :+
          CDC_TYPE_NOT_CDC
        if (cdcEnabled) {
          // For delete we do a no-op copy with ROW_DELETED_COL = false, INCR_ROW_COUNT_COL as a
          // no-op (because the metric will be incremented in `mainDataOutput`) and
          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_DELETE
          val deleteCdcOutput = targetOutputCols :+ FalseLiteral :+ TrueLiteral :+ CDC_TYPE_DELETE
          Seq(mainDataOutput, deleteCdcOutput)
        } else {
          Seq(mainDataOutput)
        }
      }
      deleteExprs.map(resolveOnJoinedPlan)
    }

    def insertOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
      : Seq[Seq[Expression]] = {
      // Generate insert expressions and set ROW_DELETED_COL = false and
      // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC
      val insertExprs = resolvedActions.map(_.expr)
      val mainDataOutput = resolveOnJoinedPlan(
        if (isDeleteWithDuplicateMatchesAndCdc) {
          // Must be delete-when-matched merge with duplicate matches + insert clause
          // Therefore we must keep the target row id and source row id. Since this is a not-matched
          // clause we know the target row-id will be null. See above at
          // isDeleteWithDuplicateMatchesAndCdc definition for more details.
          insertExprs :+
            Alias(Literal(null), TARGET_ROW_ID_COL)() :+ UnresolvedAttribute(SOURCE_ROW_ID_COL) :+
            FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC
        } else {
          insertExprs :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC
        }
      )
      if (cdcEnabled) {
        // For insert we have the same expressions as for mainDataOutput, but with
        // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
        // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_INSERT
        val insertCdcOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+ Literal(CDC_TYPE_INSERT)
        Seq(mainDataOutput, insertCdcOutput)
      } else {
        Seq(mainDataOutput)
      }
    }

    def clauseOutput(clause: DeltaMergeIntoClause): Seq[Seq[Expression]] = clause match {
      case u: DeltaMergeIntoMatchedUpdateClause =>
        updateOutput(u.resolvedActions, And(incrUpdatedCountExpr, incrUpdatedMatchedCountExpr))
      case _: DeltaMergeIntoMatchedDeleteClause =>
        deleteOutput(And(incrDeletedCountExpr, incrDeletedMatchedCountExpr))
      case i: DeltaMergeIntoNotMatchedInsertClause =>
        insertOutput(i.resolvedActions, incrInsertedCountExpr)
      case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
        updateOutput(
          u.resolvedActions,
          And(incrUpdatedCountExpr, incrUpdatedNotMatchedBySourceCountExpr))
      case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
        deleteOutput(And(incrDeletedCountExpr, incrDeletedNotMatchedBySourceCountExpr))
    }

    def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
      // if condition is None, then expression always evaluates to true
      val condExpr = clause.condition.getOrElse(TrueLiteral)
      resolveOnJoinedPlan(Seq(condExpr)).head
    }

    val joinedRowEncoder = RowEncoder(joinedPlan.schema)
    val outputRowEncoder = RowEncoder(outputRowSchema).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedConditions = matchedClauses.map(clauseCondition),
      matchedOutputs = matchedClauses.map(clauseOutput),
      notMatchedConditions = notMatchedClauses.map(clauseCondition),
      notMatchedOutputs = notMatchedClauses.map(clauseOutput),
      notMatchedBySourceConditions = notMatchedBySourceClauses.map(clauseCondition),
      notMatchedBySourceOutputs = notMatchedBySourceClauses.map(clauseOutput),
      noopCopyOutput =
        resolveOnJoinedPlan(targetOutputCols :+ FalseLiteral :+ incrNoopCountExpr :+
          CDC_TYPE_NOT_CDC),
      deleteRowOutput =
        resolveOnJoinedPlan(targetOutputCols :+ TrueLiteral :+ TrueLiteral :+ CDC_TYPE_NOT_CDC),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    var outputDF =
      Dataset.ofRows(spark, joinedPlan).mapPartitions(processor.processPartition)(outputRowEncoder)

    if (isDeleteWithDuplicateMatchesAndCdc) {
      // When we have a delete when matched clause with duplicate matches we have to remove
      // duplicate CDC rows. This scenario is further explained at
      // isDeleteWithDuplicateMatchesAndCdc definition.

      // To remove duplicate CDC rows generated by the duplicate matches we dedupe by
      // TARGET_ROW_ID_COL since there should only be one CDC delete row per target row.
      // When there is an insert clause in addition to the delete clause we additionally dedupe by
      // SOURCE_ROW_ID_COL and CDC_TYPE_COLUMN_NAME to avoid dropping valid duplicate inserted rows
      // and their corresponding CDC rows.
      val columnsToDedupeBy = if (notMatchedClauses.nonEmpty) { // insert clause
        Seq(TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL, CDC_TYPE_COLUMN_NAME)
      } else {
        Seq(TARGET_ROW_ID_COL)
      }
      outputDF = outputDF
        .dropDuplicates(columnsToDedupeBy)
        .drop(ROW_DROPPED_COL, INCR_ROW_COUNT_COL, TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL)
    } else {
      outputDF = outputDF.drop(ROW_DROPPED_COL, INCR_ROW_COUNT_COL)
    }

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
}

object MergeIntoCommand {
  /**
   * Spark UI will track all normal accumulators along with Spark tasks to show them on Web UI.
   * However, the accumulator used by `MergeIntoCommand` can store a very large value since it
   * tracks all files that need to be rewritten. We should ask Spark UI to not remember it,
   * otherwise, the UI data may consume lots of memory. Hence, we use the prefix `internal.metrics.`
   * to make this accumulator become an internal accumulator, so that it will not be tracked by
   * Spark UI.
   */
  val TOUCHED_FILES_ACCUM_NAME = "internal.metrics.MergeIntoDelta.touchedFiles"

  val ROW_ID_COL = "_row_id_"
  val TARGET_ROW_ID_COL = "_target_row_id_"
  val SOURCE_ROW_ID_COL = "_source_row_id_"
  val FILE_NAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"
  val INCR_ROW_COUNT_COL = "_incr_row_count_"

  /**
   * @param targetRowHasNoMatch   whether a joined row is a target row with no match in the source
   *                              table
   * @param sourceRowHasNoMatch   whether a joined row is a source row with no match in the target
   *                              table
   * @param matchedConditions     condition for each match clause
   * @param matchedOutputs        corresponding output for each match clause. for each clause, we
   *                              have 1-3 output rows, each of which is a sequence of expressions
   *                              to apply to the joined row
   * @param notMatchedConditions  condition for each not-matched clause
   * @param notMatchedOutputs     corresponding output for each not-matched clause. for each clause,
   *                              we have 1-2 output rows, each of which is a sequence of
   *                              expressions to apply to the joined row
   * @param notMatchedBySourceConditions  condition for each not-matched-by-source clause
   * @param notMatchedBySourceOutputs     corresponding output for each not-matched-by-source
   *                                      clause. for each clause, we have 1-3 output rows, each of
   *                                      which is a sequence of expressions to apply to the joined
   *                                      row
   * @param noopCopyOutput        no-op expression to copy a target row to the output
   * @param deleteRowOutput       expression to drop a row from the final output. this is used for
   *                              source rows that don't match any not-matched clauses
   * @param joinedAttributes      schema of our outer-joined dataframe
   * @param joinedRowEncoder      joinedDF row encoder
   * @param outputRowEncoder      final output row encoder
   */
  class JoinedRowProcessor(
      targetRowHasNoMatch: Expression,
      sourceRowHasNoMatch: Expression,
      matchedConditions: Seq[Expression],
      matchedOutputs: Seq[Seq[Seq[Expression]]],
      notMatchedConditions: Seq[Expression],
      notMatchedOutputs: Seq[Seq[Seq[Expression]]],
      notMatchedBySourceConditions: Seq[Expression],
      notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
      noopCopyOutput: Seq[Expression],
      deleteRowOutput: Seq[Expression],
      joinedAttributes: Seq[Attribute],
      joinedRowEncoder: ExpressionEncoder[Row],
      outputRowEncoder: ExpressionEncoder[Row]) extends Serializable {

    private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
      UnsafeProjection.create(exprs, joinedAttributes)
    }

    private def generatePredicate(expr: Expression): BasePredicate = {
      GeneratePredicate.generate(expr, joinedAttributes)
    }

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {

      val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
      val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
      val matchedPreds = matchedConditions.map(generatePredicate)
      val matchedProjs = matchedOutputs.map(_.map(generateProjection))
      val notMatchedPreds = notMatchedConditions.map(generatePredicate)
      val notMatchedProjs = notMatchedOutputs.map(_.map(generateProjection))
      val notMatchedBySourcePreds = notMatchedBySourceConditions.map(generatePredicate)
      val notMatchedBySourceProjs = notMatchedBySourceOutputs.map(_.map(generateProjection))
      val noopCopyProj = generateProjection(noopCopyOutput)
      val deleteRowProj = generateProjection(deleteRowOutput)
      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      // this is accessing ROW_DROPPED_COL. If ROW_DROPPED_COL is not in outputRowEncoder.schema
      // then CDC must be disabled and it's the column after our output cols
      def shouldDeleteRow(row: InternalRow): Boolean = {
        row.getBoolean(
          outputRowEncoder.schema.getFieldIndex(ROW_DROPPED_COL)
            .getOrElse(outputRowEncoder.schema.fields.size)
        )
      }

      def processRow(inputRow: InternalRow): Iterator[InternalRow] = {
        // Identify which set of clauses to execute: matched, not-matched or not-matched-by-source
        val (predicates, projections, noopAction) = if (targetRowHasNoMatchPred.eval(inputRow)) {
          // Target row did not match any source row, so update the target row.
          (notMatchedBySourcePreds, notMatchedBySourceProjs, noopCopyProj)
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
          // Source row did not match with any target row, so insert the new source row
          (notMatchedPreds, notMatchedProjs, deleteRowProj)
        } else {
          // Source row matched with target row, so update the target row
          (matchedPreds, matchedProjs, noopCopyProj)
        }

        // find (predicate, projection) pair whose predicate satisfies inputRow
        val pair = (predicates zip projections).find {
          case (predicate, _) => predicate.eval(inputRow)
        }

        pair match {
          case Some((_, projections)) =>
            projections.map(_.apply(inputRow)).iterator
          case None => Iterator(noopAction.apply(inputRow))
        }
      }

      val toRow = joinedRowEncoder.createSerializer()
      val fromRow = outputRowEncoder.createDeserializer()
      rowIterator
        .map(toRow)
        .flatMap(processRow)
        .filter(!shouldDeleteRow(_))
        .map { notDeletedInternalRow =>
          fromRow(outputProj(notDeletedInternalRow))
        }
    }
  }
}
