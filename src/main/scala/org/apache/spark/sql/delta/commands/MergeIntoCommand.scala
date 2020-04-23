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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, Literal, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

case class MergeDataRows(rows: Long)
case class MergeDataFiles(files: Long)

/** State for a merge operation */
case class MergeStats(
    // Expressions used in MERGE
    conditionExpr: String,
    updateConditionExpr: String,
    updateExprs: Array[String],
    insertConditionExpr: String,
    insertExprs: Array[String],
    deleteConditionExpr: String,

    // Data sizes of source and target at different stages of processing
    source: MergeDataRows,
    targetBeforeSkipping: MergeDataFiles,
    targetAfterSkipping: MergeDataFiles,

    // Data change sizes
    targetFilesRemoved: Long,
    targetFilesAdded: Long,
    targetRowsCopied: Long,
    targetRowsUpdated: Long,
    targetRowsInserted: Long,
    targetRowsDeleted: Long)

/**
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Read the target files and perform a full join with the source dataset. This
 *    allows identification of not matched by source, not matched by target and matching records.
 *
 * Phase 2: Apply the actions specified in the DeltaMergeBuilder to determine outcome of each
 *    record and collect the files impacted by the delete and update operations.
 *
 * Phase 3: The distinct impacted files are then collected and joined with the dataset
 *    to determine which files need to be rewritten and which can remain unchanged. The records to
 *    be written are then filtered from that result set.
 *
 * Phase 4: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source            Source data to merge from
 * @param target            Target table to merge into
 * @param targetFileIndex   TahoeFileIndex of the target table
 * @param condition         Condition for a source row to match with a target row
 * @param matchedClauses    All info related to matched clauses.
 * @param notMatchedByTargetClause  All info related to not matched clause.
 * @param migratedSchema    The final schema of the target - may be changed by schema evolution.
 */
case class MergeIntoCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedByTargetClause: Option[DeltaMergeIntoInsertClause],
    notMatchedBySourceClause: Option[DeltaMergeIntoNotMatchedDeleteClause],
    migratedSchema: Option[StructType]) extends RunnableCommand
  with DeltaCommand with PredicateHelper with AnalysisHelper with ImplicitMetadataOperation {

  import SQLMetrics._
  import MergeIntoCommand._

  override val canMergeSchema: Boolean = conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  /** Whether this merge statement only inserts new data. */
  private def isInsertOnly: Boolean =
    matchedClauses.isEmpty && notMatchedBySourceClause.isEmpty && notMatchedByTargetClause.isDefined
  private def isMatchedOnly: Boolean =
    notMatchedByTargetClause.isEmpty && notMatchedBySourceClause.isEmpty && matchedClauses.nonEmpty

  lazy val updateClause: Option[DeltaMergeIntoUpdateClause] =
    matchedClauses.collectFirst { case u: DeltaMergeIntoUpdateClause => u }
  lazy val deleteMatchedClause: Option[DeltaMergeIntoMatchedDeleteClause] =
    matchedClauses.collectFirst { case d: DeltaMergeIntoMatchedDeleteClause => d }

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsSkipped" -> createMetric(sc, "number of skipped rows"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"))

  override def run(
    spark: SparkSession): Seq[Row] = recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
    targetDeltaLog.withNewTransaction { deltaTxn =>
      if (canMergeSchema) {
        updateMetadata(
          spark, deltaTxn, migratedSchema.getOrElse(target.schema),
          deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
          isOverwriteMode = false, rearrangeOnly = false)
      }

      val deltaActions = {
       if (isInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
         writeInsertsOnlyWhenNoMatchedClauses(spark, deltaTxn)
       } else {
        val (filesToRewrite, newWrittenFiles) =
          recordDeltaOperation(targetDeltaLog, "delta.dml.merge.findTouchedFiles") {
            writeAllChanges(spark, deltaTxn)
          }
          filesToRewrite.map(_.remove) ++ newWrittenFiles
        }
      }
      deltaTxn.registerSQLMetrics(spark, metrics)
      deltaTxn.commit(
        deltaActions,
        DeltaOperations.Merge(
          Option(condition.sql),
          updateClause.flatMap(_.condition).map(_.sql),
          deleteMatchedClause.flatMap(_.condition).map(_.sql),
          notMatchedBySourceClause.flatMap(_.condition).map(_.sql),
          notMatchedByTargetClause.flatMap(_.condition).map(_.sql)))

      // Record metrics
      val stats = MergeStats(
        // Expressions
        conditionExpr = condition.sql,
        updateConditionExpr = updateClause.flatMap(_.condition).map(_.sql).orNull,
        updateExprs = updateClause.map(_.actions.map(_.sql).toArray).orNull,
        insertConditionExpr = notMatchedByTargetClause.flatMap(_.condition).map(_.sql).orNull,
        insertExprs = notMatchedByTargetClause.map(_.actions.map(_.sql).toArray).orNull,
        deleteConditionExpr = deleteMatchedClause.flatMap(_.condition).map(_.sql).orNull,

        // Data sizes of source and target at different stages of processing
        MergeDataRows(metrics("numSourceRows").value),
        targetBeforeSkipping =
          MergeDataFiles(metrics("numTargetFilesBeforeSkipping").value),
        targetAfterSkipping =
          MergeDataFiles(metrics("numTargetFilesAfterSkipping").value),

        // Data change sizes
        targetFilesAdded = metrics("numTargetFilesAdded").value,
        targetFilesRemoved = metrics("numTargetFilesRemoved").value,
        targetRowsCopied = metrics("numTargetRowsCopied").value,
        targetRowsUpdated = metrics("numTargetRowsUpdated").value,
        targetRowsInserted = metrics("numTargetRowsInserted").value,
        targetRowsDeleted = metrics("numTargetRowsDeleted").value)
      recordDeltaEvent(targetFileIndex.deltaLog, "delta.dml.merge.stats", data = stats)

      // This is needed to make the SQL metrics visible in the Spark UI
      val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
    }
    spark.sharedState.cacheManager.recacheByPlan(spark, target)
    Seq.empty
  }

  /**
   * This is an optimization of the case when there is no update clause for the merge.
   * We perform an left anti join on the source data to find the rows to be inserted.
   */
  private def writeInsertsOnlyWhenNoMatchedClauses(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction
    ): Seq[AddFile] = withStatusCode("DELTA", s"Writing new files " +
    s"for insert-only MERGE operation") {

    // UDFs to update metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val incrInsertedCountExpr = makeMetricUpdateUDF("numTargetRowsInserted")

    val outputColNames = getTargetOutputCols(deltaTxn).map(_.name)
    val outputExprs =
      notMatchedByTargetClause.get.resolvedActions.map(_.expr) :+ incrInsertedCountExpr
    val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
      new Column(Alias(expr, name)())
    }

    // source DataFrame
    val sourceDF = Dataset.ofRows(spark, source)
      .filter(new Column(incrSourceRowCountExpr))
      .filter(new Column(notMatchedByTargetClause.get.condition.getOrElse(Literal(true))))

    // Skip data based on the merge condition
    val conjunctivePredicates = splitConjunctivePredicates(condition)
    val targetOnlyPredicates =
      conjunctivePredicates.filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // target DataFrame
    val targetDF = Dataset.ofRows(
      spark, buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles))

    val insertDf = sourceDF.join(targetDF, new Column(condition), "leftanti")
      .select(outputCols: _*)

    val newFiles = deltaTxn
      .writeFiles(repartitionIfNeeded(spark, insertDf, deltaTxn.metadata.partitionColumns))
    metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetFilesRemoved") += 0
    metrics("numTargetFilesAdded") += newFiles.size
    newFiles
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full|right-outer-join using the merge condition.
   */
  private def writeAllChanges(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction
  ): (Seq[AddFile], Seq[AddFile]) = {
    import spark.implicits._

    val targetOutputCols = getTargetOutputCols(deltaTxn)

    // Skip data based on the merge condition
    val targetOnlyPredicates =
      splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // Generate a new logical plan that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val joinType = if (isMatchedOnly &&
      spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
      "rightOuter"
    } else {
      "full"
    }

    logDebug(s"""writeAllChanges using $joinType join:
                |  source.output: ${source.outputSet}
                |  target.output: ${target.outputSet}
                |  condition: $condition}
       """.stripMargin)

    // UDFs to update metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val incrUpdatedCountExpr = makeMetricUpdateUDF("numTargetRowsUpdated")
    val incrInsertedCountExpr = makeMetricUpdateUDF("numTargetRowsInserted")
    val incrNoopCountExpr = makeMetricUpdateUDF("numTargetRowsCopied")
    val incrDeletedCountExpr = makeMetricUpdateUDF("numTargetRowsDeleted")
    val incrSkippedCountExpr = makeMetricUpdateUDF("numTargetRowsSkipped")

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the outer join, will allow us to identify whether the resultant joined row was a
    // matched inner result or an unmatched result with null on one side.
    val joinedDF = {
      val sourceDF = Dataset.ofRows(spark, source)
        .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
      val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles))
        .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
        .withColumn(FILE_NAME_COL, input_file_name())
        .withColumn(ROW_ID_COL, monotonically_increasing_id())
      sourceDF.join(targetDF, new Column(condition), joinType)
    }

    val joinedPlan = joinedDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      exprs.map { expr => tryResolveReferences(spark)(expr, joinedPlan) }
    }

    // expressions to include the first output for filtering
    val filenameCol = joinedPlan.output.filter { output => output.name == FILE_NAME_COL }.head
    val rowIdCol = joinedPlan.output.filter { output => output.name == ROW_ID_COL }.head

    def matchedClauseOutput(clause: DeltaMergeIntoMatchedClause) = {
      val exprs = clause match {
        case u: DeltaMergeIntoUpdateClause =>
          // Generate update expressions and set ROW_DELETED_COL = false
          u.resolvedActions.map(_.expr) :+
            Literal(TARGET_ROW_UPDATE) :+
            filenameCol :+
            rowIdCol :+
            incrUpdatedCountExpr
        case _: DeltaMergeIntoMatchedDeleteClause =>
          // Generate expressions to set the ROW_DELETED_COL = true
          targetOutputCols :+
            Literal(TARGET_ROW_DELETE) :+
            filenameCol :+
            rowIdCol :+
            incrDeletedCountExpr
      }
      resolveOnJoinedPlan(exprs)
    }

    def notMatchedByTargetClauseOutput(clause: DeltaMergeIntoInsertClause) = {
      val exprs = clause.resolvedActions.map(_.expr) :+
        Literal(TARGET_ROW_INSERT) :+
        filenameCol :+
        rowIdCol :+
        incrInsertedCountExpr
      resolveOnJoinedPlan(exprs)
    }

    def notMatchedBySourceClauseOutput(clause: DeltaMergeIntoNotMatchedDeleteClause) = {
      resolveOnJoinedPlan(
        targetOutputCols :+
        Literal(TARGET_ROW_DELETE) :+
        filenameCol :+
        rowIdCol :+
        incrDeletedCountExpr
      )
    }

    def clauseCondition(clause: Option[DeltaMergeIntoClause]) = {
      val condExprOption = clause.map(_.condition.getOrElse(Literal(true)))
      resolveOnJoinedPlan(condExprOption.toSeq).headOption
    }

    val matchedClause1 = matchedClauses.headOption
    val matchedClause2 = matchedClauses.drop(1).headOption
    val joinedRowEncoder = RowEncoder(joinedPlan.schema)

    // this schema includes the data required to identify which files to rewrite
    val outputRowEncoder = RowEncoder(
      StructType(deltaTxn.metadata.schema.fields.toList :::
      StructField(TARGET_ROW_OUTCOME, IntegerType, true) ::
      StructField(FILE_NAME_COL, StringType, true) ::
      StructField(ROW_ID_COL, LongType, true) :: Nil)
    ).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedCondition1 = clauseCondition(matchedClause1),
      matchedOutput1 = matchedClause1.map(matchedClauseOutput),
      matchedCondition2 = clauseCondition(matchedClause2),
      matchedOutput2 = matchedClause2.map(matchedClauseOutput),
      notMatchedByTargetCondition = clauseCondition(notMatchedByTargetClause),
      notMatchedByTargetOutput = notMatchedByTargetClause.map(notMatchedByTargetClauseOutput),
      notMatchedBySourceCondition = clauseCondition(notMatchedBySourceClause),
      notMatchedBySourceOutput = notMatchedBySourceClause.map(notMatchedBySourceClauseOutput),
      copyRowOutput = resolveOnJoinedPlan(
        targetOutputCols :+
        Literal(TARGET_ROW_COPY) :+
        filenameCol :+
        rowIdCol :+
        incrNoopCountExpr
      ),
      skippedRowOutput = resolveOnJoinedPlan(
        targetOutputCols :+
        Literal(TARGET_ROW_SKIP) :+
        filenameCol :+
        rowIdCol :+
        incrSkippedCountExpr
      ),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder,
      touchedFilesAccum = touchedFilesAccum)

    val calculatedDF =
      Dataset.ofRows(spark, joinedPlan).mapPartitions(processor.processPartition)(outputRowEncoder)
    logDebug("writeAllChanges: join output plan:\n" + calculatedDF.queryExecution)

    // register the processed partion to be cached as the output is used twice:
    // - to test for cartesian product
    // - to calculate the list of records that need to be written
    calculatedDF.persist

    // calculate frequency of matches per source row to determine if join has resulted in duplicate
    // matches this will also execute the processPartition function which will populate the
    // touchedFilesAccum accumulator
    val matchedRowCounts = calculatedDF
      .filter(col(ROW_ID_COL).isNotNull)
      .groupBy(ROW_ID_COL)
      .agg(count("*").alias("count"))
    if (matchedRowCounts.filter("count > 1").count() != 0) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
    }

    // use the accumulated values to detail which files to remove
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSet.toSeq
    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val removeFiles = touchedFileNames
      .map(f => getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    // use the accumulated file names to make a new dataframe for joining to
    val touchedFileNamesDF = touchedFileNames.toDF(TOUCHED_FILE_NAME_COL)

    // join to the touched files dataframe to determine which records need to be written
    val outputDF = calculatedDF
      .join(touchedFileNamesDF, col(FILE_NAME_COL) === col(TOUCHED_FILE_NAME_COL), "left")
      .filter { row =>
        val outcome = row.getInt(row.fieldIndex(TARGET_ROW_OUTCOME))
        val touchedFileNameNotNull = !row.isNullAt(row.fieldIndex(TOUCHED_FILE_NAME_COL))
        (outcome == TARGET_ROW_COPY && touchedFileNameNotNull) ||
        (outcome == TARGET_ROW_INSERT) ||
        (outcome == TARGET_ROW_UPDATE)
      }
      .drop(TARGET_ROW_OUTCOME)
      .drop(TOUCHED_FILE_NAME_COL)
      .drop(FILE_NAME_COL)
      .drop(ROW_ID_COL)


    // Write to Delta
    val newFiles = deltaTxn
      .writeFiles(repartitionIfNeeded(spark, outputDF, deltaTxn.metadata.partitionColumns))
    metrics("numTargetFilesAdded") += newFiles.size

    // remove dataset from cache
    calculatedDF.unpersist

    (removeFiles, newFiles)
  }


  /**
   * Build a new logical plan using the given `files` that has the same output columns (exprIds)
   * as the `target` logical plan, so that existing update/insert expressions can be applied
   * on this new plan.
   */
  private def buildTargetPlanWithFiles(
    deltaTxn: OptimisticTransaction,
    files: Seq[AddFile]): LogicalPlan = {
    val plan = deltaTxn.deltaLog.createDataFrame(deltaTxn.snapshot, files).queryExecution.analyzed

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = getTargetOutputCols(deltaTxn).find(_.name == newAttrib.name)
          .getOrElse {
            throw new AnalysisException(
              s"Could not find ${newAttrib.name} among the existing target output " +
                s"${getTargetOutputCols(deltaTxn)}")
          }.asInstanceOf[AttributeReference]
        Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
    }
    Project(aliases, plan)
  }

  /** Expressions to increment SQL metrics */
  private def makeMetricUpdateUDF(name: String): Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    udf { () => { metric += 1; true }}.asNondeterministic().apply().expr
  }

  private def seqToString(exprs: Seq[Expression]): String = exprs.map(_.sql).mkString("\n\t")

  private def getTargetOutputCols(txn: OptimisticTransaction): Seq[NamedExpression] = {
    txn.metadata.schema.map { col =>
      target.output.find(attr => conf.resolver(attr.name, col.name)).getOrElse {
        Alias(Literal(null, col.dataType), col.name)()
      }
    }
  }

  /**
   * Repartitions the output DataFrame by the partition columns if table is partitioned
   * and `merge.repartitionBeforeWrite.enabled` is set to true.
   */
  protected def repartitionIfNeeded(
      spark: SparkSession,
      df: DataFrame,
      partitionColumns: Seq[String]): DataFrame = {
    if (partitionColumns.nonEmpty && spark.conf.get(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE)) {
      df.repartition(partitionColumns.map(col): _*)
    } else {
      df
    }
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
  val FILE_NAME_COL = "_file_name_"
  val TOUCHED_FILE_NAME_COL = "_touched_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"

  val TARGET_ROW_OUTCOME = "_target_row_outcome_"
  val TARGET_ROW_COPY = 0
  val TARGET_ROW_DELETE = 1
  val TARGET_ROW_INSERT = 2
  val TARGET_ROW_SKIP = 3
  val TARGET_ROW_UPDATE = 4

  class JoinedRowProcessor(
      targetRowHasNoMatch: Expression,
      sourceRowHasNoMatch: Expression,
      matchedCondition1: Option[Expression],
      matchedOutput1: Option[Seq[Expression]],
      matchedCondition2: Option[Expression],
      matchedOutput2: Option[Seq[Expression]],
      notMatchedByTargetCondition: Option[Expression],
      notMatchedByTargetOutput: Option[Seq[Expression]],
      notMatchedBySourceCondition: Option[Expression],
      notMatchedBySourceOutput: Option[Seq[Expression]],
      copyRowOutput: Seq[Expression],
      skippedRowOutput: Seq[Expression],
      joinedAttributes: Seq[Attribute],
      joinedRowEncoder: ExpressionEncoder[Row],
      outputRowEncoder: ExpressionEncoder[Row],
      touchedFilesAccum: SetAccumulator[String]) extends Serializable {

    private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
      UnsafeProjection.create(exprs, joinedAttributes)
    }

    private def generatePredicate(expr: Expression): Predicate = {
      GeneratePredicate.generate(expr, joinedAttributes)
    }

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {

      val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
      val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
      val matchedPred1 = matchedCondition1.map(generatePredicate)
      val matchedOutputProj1 = matchedOutput1.map(generateProjection)
      val matchedPred2 = matchedCondition2.map(generatePredicate)
      val matchedOutputProj2 = matchedOutput2.map(generateProjection)
      val notMatchedByTargetPred = notMatchedByTargetCondition.map(generatePredicate)
      val notMatchedByTargetProj = notMatchedByTargetOutput.map(generateProjection)
      val notMatchedBySourcePred = notMatchedBySourceCondition.map(generatePredicate)
      val notMatchedBySourceProj = notMatchedBySourceOutput.map(generateProjection)
      val copyRowProj = generateProjection(copyRowOutput)
      val skippedRowProj = generateProjection(skippedRowOutput)
      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      def processRow(inputRow: InternalRow, filenameFieldIndex: Int): InternalRow = {

        // if record exists in target but not in source maybe delete
        if (targetRowHasNoMatchPred.eval(inputRow)) {

          if (notMatchedBySourcePred.isDefined && notMatchedBySourcePred.get.eval(inputRow)) {
            touchedFilesAccum.add(inputRow.getString(filenameFieldIndex))
            notMatchedBySourceProj.get.apply(inputRow)
          } else {
            copyRowProj(inputRow)
          }

        // if record exists in source but not in target then maybe insert
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {

          if (notMatchedByTargetPred.isDefined && notMatchedByTargetPred.get.eval(inputRow)) {
            notMatchedByTargetProj.get.apply(inputRow)
          } else {
            skippedRowProj.apply(inputRow)
          }

        // if record exists and source and target then maybe update or delete
        } else {
          // Source row matched with target row, so update the target row
          if (matchedPred1.isDefined && matchedPred1.get.eval(inputRow)) {
            touchedFilesAccum.add(inputRow.getString(filenameFieldIndex))
            matchedOutputProj1.get.apply(inputRow)
          } else if (matchedPred2.isDefined && matchedPred2.get.eval(inputRow)) {
            touchedFilesAccum.add(inputRow.getString(filenameFieldIndex))
            matchedOutputProj2.get.apply(inputRow)
          } else {
            // do not register in touchedFilesAccum
            copyRowProj(inputRow)
          }
        }

      }

      rowIterator
        .map { joinedRowEncoder.toRow }
        .map { row => processRow(row, joinedRowEncoder.schema.fieldIndex(FILE_NAME_COL)) }
        .map { row => outputRowEncoder.fromRow(outputProj(row)) }
    }
  }
}
