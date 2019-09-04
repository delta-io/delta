/*
 * Copyright 2019 Databricks, Inc.
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
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratePredicate, Predicate}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._

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
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 *    for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source            Source data to merge from
 * @param target            Target table to merge into
 * @param targetFileIndex   TahoeFileIndex of the target table
 * @param condition         Condition for a source row to match with a target row
 * @param matchedClauses    All info related to matched clauses.
 * @param notMatchedClause  All info related to not matched clause.
 */
case class MergeIntoCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Expression,
    matchedClauses: Seq[MergeIntoMatchedClause],
    notMatchedClause: Option[MergeIntoInsertClause]) extends RunnableCommand
  with DeltaCommand with PredicateHelper with AnalysisHelper {

  import SQLMetrics._
  import MergeIntoCommand._

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  lazy val updateClause: Option[MergeIntoUpdateClause] =
    matchedClauses.collectFirst { case u: MergeIntoUpdateClause => u }
  lazy val deleteClause: Option[MergeIntoDeleteClause] =
    matchedClauses.collectFirst { case d: MergeIntoDeleteClause => d }

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"))

  override def run(
    spark: SparkSession): Seq[Row] = recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
    targetDeltaLog.withNewTransaction { deltaTxn =>
      val deltaActions = {
        val filesToRewrite =
          recordDeltaOperation(targetDeltaLog, "delta.dml.merge.findTouchedFiles") {
            findTouchedFiles(spark, deltaTxn)
          }

        val newWrittenFiles = writeAllChanges(spark, deltaTxn, filesToRewrite)
        filesToRewrite.map(_.remove) ++ newWrittenFiles
      }
      deltaTxn.commit(
        deltaActions,
        DeltaOperations.Merge(
          Option(condition.sql),
          updateClause.flatMap(_.condition).map(_.sql),
          deleteClause.flatMap(_.condition).map(_.sql),
          notMatchedClause.flatMap(_.condition).map(_.sql)))

      // Record metrics
      val stats = MergeStats(
        // Expressions
        conditionExpr = condition.sql,
        updateConditionExpr = updateClause.flatMap(_.condition).map(_.sql).orNull,
        updateExprs = updateClause.map(_.actions.map(_.sql).toArray).orNull,
        insertConditionExpr = notMatchedClause.flatMap(_.condition).map(_.sql).orNull,
        insertExprs = notMatchedClause.map(_.actions.map(_.sql).toArray).orNull,
        deleteConditionExpr = deleteClause.flatMap(_.condition).map(_.sql).orNull,

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
   * Find the target table files that contain the rows that satisfy the merge condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the merge condition.
   */
  private def findTouchedFiles(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction
  ): Seq[AddFile] = {

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, "MergeIntoDelta.touchedFiles")

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName = udf { (fileName: String) => {
      touchedFilesAccum.add(fileName)
      1
    }}.asNondeterministic()

    // Skip data based on the merge condition
    val targetOnlyPredicates =
      splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // Apply inner join to between source and target using the merge condition to find matches
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val joinToFindTouchedFiles = {
      val sourceDF = Dataset.ofRows(spark, source)
      val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles))
        .withColumn(ROW_ID_COL, monotonically_increasing_id())
        .withColumn(FILE_NAME_COL, input_file_name())
      sourceDF.join(targetDF, new Column(condition), "inner")
    }

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL), recordTouchedFileName(col(FILE_NAME_COL)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))
    if (matchedRowCounts.filter("count > 1").count() != 0) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.map(f =>
      getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetFilesRemoved") += touchedAddFiles.size
    touchedAddFiles
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full-outer-join using the merge condition.
   */
  private def writeAllChanges(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    filesToRewrite: Seq[AddFile]
  ): Seq[AddFile] = {

    // Generate a new logical plan that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val newTarget = buildTargetPlanWithFiles(deltaTxn, filesToRewrite)

    logDebug(s"""writeAllChanges using full outer join:
                |  source.output: ${source.outputSet}
                |  target.output: ${target.outputSet}
                |  condition: $condition
                |  newTarget.output: ${newTarget.outputSet}
       """.stripMargin)

    // UDFs to update metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val incrUpdatedCountExpr = makeMetricUpdateUDF("numTargetRowsUpdated")
    val incrInsertedCountExpr = makeMetricUpdateUDF("numTargetRowsInserted")
    val incrNoopCountExpr = makeMetricUpdateUDF("numTargetRowsCopied")
    val incrDeletedCountExpr = makeMetricUpdateUDF("numTargetRowsDeleted")

    // Apply full outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the full outer join, will allow us to identify whether the resultanet joined row was a
    // matched inner result or an unmatched result with null on one side.
    val joinedDF = {
      val sourceDF = Dataset.ofRows(spark, source)
        .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
      val targetDF = Dataset.ofRows(spark, newTarget)
        .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
      sourceDF.join(targetDF, new Column(condition), "fullOuter")
    }

    val joinedPlan = joinedDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      exprs.map { expr => tryResolveReferences(spark)(expr, joinedPlan) }
    }

    def matchedClauseOutput(clause: MergeIntoMatchedClause): Seq[Expression] = {
      val exprs = clause match {
        case u: MergeIntoUpdateClause =>
          // Generate update expressions and set ROW_DELETED_COL = false
          u.resolvedActions.map(_.expr) :+ Literal(false) :+ incrUpdatedCountExpr
        case _: MergeIntoDeleteClause =>
          // Generate expressions to set the ROW_DELETED_COL = true
          target.output :+ Literal(true) :+ incrDeletedCountExpr
      }
      resolveOnJoinedPlan(exprs)
    }

    def notMatchedClauseOutput(clause: MergeIntoInsertClause): Seq[Expression] = {
      val exprs = clause.resolvedActions.map(_.expr) :+ Literal(false) :+ incrInsertedCountExpr
      resolveOnJoinedPlan(exprs)
    }

    def clauseCondition(clause: Option[MergeIntoClause]): Option[Expression] = {
      val condExprOption = clause.map(_.condition.getOrElse(Literal(true)))
      resolveOnJoinedPlan(condExprOption.toSeq).headOption
    }

    val matchedClause1 = matchedClauses.headOption
    val matchedClause2 = matchedClauses.drop(1).headOption
    val joinedRowEncoder = RowEncoder(joinedPlan.schema)
    val outputRowEncoder = RowEncoder(target.schema).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedCondition1 = clauseCondition(matchedClause1),
      matchedOutput1 = matchedClause1.map(matchedClauseOutput),
      matchedCondition2 = clauseCondition(matchedClause2),
      matchedOutput2 = matchedClause2.map(matchedClauseOutput),
      notMatchedCondition = clauseCondition(notMatchedClause),
      notMatchedOutput = notMatchedClause.map(notMatchedClauseOutput),
      noopCopyOutput = resolveOnJoinedPlan(target.output :+ Literal(false) :+ incrNoopCountExpr),
      deleteRowOutput = resolveOnJoinedPlan(target.output :+ Literal(true) :+ Literal(true)),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    val outputDF =
      Dataset.ofRows(spark, joinedPlan).mapPartitions(processor.processPartition)(outputRowEncoder)
    logDebug("writeAllChanges: join output plan:\n" + outputDF.queryExecution)

    // Write to Delta
    val newFiles = deltaTxn.writeFiles(outputDF)
    metrics("numTargetFilesAdded") += newFiles.size
    newFiles
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
        val existingTargetAttrib = target.output.find(_.name == newAttrib.name).getOrElse {
          throw new AnalysisException(
            s"Could not find ${newAttrib.name} among the existing target output ${target.output}")
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
}

object MergeIntoCommand {
  val ROW_ID_COL = "_row_id_"
  val FILE_NAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"

  class JoinedRowProcessor(
      targetRowHasNoMatch: Expression,
      sourceRowHasNoMatch: Expression,
      matchedCondition1: Option[Expression],
      matchedOutput1: Option[Seq[Expression]],
      matchedCondition2: Option[Expression],
      matchedOutput2: Option[Seq[Expression]],
      notMatchedCondition: Option[Expression],
      notMatchedOutput: Option[Seq[Expression]],
      noopCopyOutput: Seq[Expression],
      deleteRowOutput: Seq[Expression],
      joinedAttributes: Seq[Attribute],
      joinedRowEncoder: ExpressionEncoder[Row],
      outputRowEncoder: ExpressionEncoder[Row]) extends Serializable {

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
      val notMatchedPred = notMatchedCondition.map(generatePredicate)
      val notMatchedProj = notMatchedOutput.map(generateProjection)
      val noopCopyProj = generateProjection(noopCopyOutput)
      val deleteRowProj = generateProjection(deleteRowOutput)
      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      def shouldDeleteRow(row: InternalRow): Boolean =
        row.getBoolean(outputRowEncoder.schema.fields.size)

      def processRow(inputRow: InternalRow): InternalRow = {
        if (targetRowHasNoMatchPred.eval(inputRow)) {
          // Target row did not match any source row, so just copy it to the output
          noopCopyProj.apply(inputRow)
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
          // Source row did not match with any target row, so insert the new source row
          if (notMatchedPred.isDefined && notMatchedPred.get.eval(inputRow)) {
            notMatchedProj.get.apply(inputRow)
          } else {
            deleteRowProj.apply(inputRow)
          }
        } else {
          // Source row matched with target row, so update the target row
          if (matchedPred1.isDefined && matchedPred1.get.eval(inputRow)) {
            matchedOutputProj1.get.apply(inputRow)
          } else if (matchedPred2.isDefined && matchedPred2.get.eval(inputRow)) {
            matchedOutputProj2.get.apply(inputRow)
          } else {
            noopCopyProj(inputRow)
          }
        }
      }

      rowIterator
        .map(joinedRowEncoder.toRow)
        .map(processRow)
        .filter(!shouldDeleteRow(_))
        .map { notDeletedInternalRow =>
          outputRowEncoder.fromRow(outputProj(notDeletedInternalRow))
        }
    }
  }
}
