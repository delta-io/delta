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

import scala.collection.mutable

import org.apache.spark.sql.delta.{RowCommitVersion, RowId}
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase
import org.apache.spark.sql.delta.commands.cdc.CDCReader

import org.apache.spark.sql._
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._

/**
 * Contains logic to transform the merge clauses into expressions that can be evaluated to obtain
 * the output of the merge operation.
 */
trait MergeOutputGeneration { self: MergeIntoCommandBase =>
  import CDCReader._
  import MergeIntoCommandBase._
  import MergeOutputGeneration._

  /**
   * Precompute conditions in MATCHED and NOT MATCHED clauses and generate the source
   * data frame with precomputed boolean columns.
   * @param sourceDF the source DataFrame.
   * @param clauses the merge clauses to precompute.
   * @return Generated sourceDF with precomputed boolean columns, matched clauses with
   *         possible rewritten clause conditions, insert clauses with possible rewritten
   *         clause conditions
   */
  protected def generatePrecomputedConditionsAndDF(
      sourceDF: DataFrame,
      clauses: Seq[DeltaMergeIntoClause]): (DataFrame, Seq[DeltaMergeIntoClause]) = {
    //
    // ==== Precompute conditions in MATCHED and NOT MATCHED clauses ====
    // If there are conditions in the clauses, each condition will be computed once for every
    // column (and obviously for every row) within the per-column CaseWhen expressions. Since, the
    // conditions can be arbitrarily expensive, it is likely to be more efficient to
    // precompute them into boolean columns and use these new columns in the CaseWhen exprs.
    // Then each condition will be computed only once per row, and the resultant boolean reused
    // for all the columns in the row.
    //
    val preComputedClauseConditions = new mutable.ArrayBuffer[(String, Expression)]()

    // Rewrite clause condition into a simple lookup of precomputed column
    def rewriteCondition[T <: DeltaMergeIntoClause](clause: T): T = {
      clause.condition match {
        case Some(conditionExpr) =>
          val colName =
            s"""_${clause.clauseType}${PRECOMPUTED_CONDITION_COL}
            |${preComputedClauseConditions.length}_
            |""".stripMargin.replaceAll("\n", "") // ex: _update_condition_0_
          preComputedClauseConditions += ((colName, conditionExpr))
          clause.makeCopy(Array(Some(UnresolvedAttribute(colName)), clause.actions)).asInstanceOf[T]
        case None => clause
      }
    }

    // Get the clauses with possibly rewritten clause conditions.
    // This will automatically populate the `preComputedClauseConditions`
    // (as part of `rewriteCondition`)
    val clausesWithPrecompConditions = clauses.map(rewriteCondition)

    // Add the columns to the given `sourceDF` to precompute clause conditions
    val sourceWithPrecompConditions = {
      val newCols = preComputedClauseConditions.map { case (colName, conditionExpr) =>
        Column(conditionExpr).as(colName)
      }.toSeq
      sourceDF.select(col("*") +: newCols: _*)
    }
    (sourceWithPrecompConditions, clausesWithPrecompConditions)
  }

  /**
   * Generate the expressions to process full-outer join output and generate target rows.
   *
   * To generate these N + 2 columns, we generate N + 2 expressions and apply them
   * on the joinedDF. The CDC column will be either used for CDC generation or dropped before
   * performing the final write, and the other column will always be dropped after executing the
   * increment metric expression and filtering on ROW_DROPPED_COL.
   */
  protected def generateWriteAllChangesOutputCols(
      targetWriteCols: Seq[Expression],
      rowIdColumnExpressionOpt: Option[NamedExpression],
      rowCommitVersionColumnExpressionOpt: Option[NamedExpression],
      targetWriteColNames: Seq[String],
      noopCopyExprs: Seq[Expression],
      clausesWithPrecompConditions: Seq[DeltaMergeIntoClause],
      cdcEnabled: Boolean,
      shouldCountDeletedRows: Boolean = true): IndexedSeq[Column] = {

    val numOutputCols = targetWriteColNames.size

    // ==== Generate N + 2 (N + 4 preserving Row Tracking) expressions for MATCHED clauses ====
    val processedMatchClauses: Seq[ProcessedClause] = generateAllActionExprs(
      targetWriteCols,
      rowIdColumnExpressionOpt,
      rowCommitVersionColumnExpressionOpt,
      clausesWithPrecompConditions.collect { case c: DeltaMergeIntoMatchedClause => c },
      cdcEnabled,
      shouldCountDeletedRows)
    val matchedExprs: Seq[Expression] = generateClauseOutputExprs(
      numOutputCols,
      processedMatchClauses,
      noopCopyExprs)

    // N + 1 (or N + 2 with CDC, N + 4 preserving Row Tracking and CDC) expressions to delete the
    // unmatched source row when it should not be inserted. `target.output` will produce NULLs
    // which will get deleted eventually.

    val deleteSourceRowExprs =
      (targetWriteCols ++
        rowIdColumnExpressionOpt.map(_ => Literal(null)) ++
        rowCommitVersionColumnExpressionOpt.map(_ => Literal(null)) ++
        Seq(Literal(true))) ++
        (if (cdcEnabled) Seq(CDC_TYPE_NOT_CDC) else Seq())

    // ==== Generate N + 2 (N + 4 preserving Row Tracking) expressions for NOT MATCHED clause ====
    val processedNotMatchClauses: Seq[ProcessedClause] = generateAllActionExprs(
      targetWriteCols,
      rowIdColumnExpressionOpt,
      rowCommitVersionColumnExpressionOpt,
      clausesWithPrecompConditions.collect { case c: DeltaMergeIntoNotMatchedClause => c },
      cdcEnabled,
      shouldCountDeletedRows)
    val notMatchedExprs: Seq[Expression] = generateClauseOutputExprs(
      numOutputCols,
      processedNotMatchClauses,
      deleteSourceRowExprs)

    // === Generate N + 2 (N + 4 with Row Tracking) expressions for NOT MATCHED BY SOURCE clause ===
    val processedNotMatchBySourceClauses: Seq[ProcessedClause] = generateAllActionExprs(
      targetWriteCols,
      rowIdColumnExpressionOpt,
      rowCommitVersionColumnExpressionOpt,
      clausesWithPrecompConditions.collect { case c: DeltaMergeIntoNotMatchedBySourceClause => c },
      cdcEnabled,
      shouldCountDeletedRows)
    val notMatchedBySourceExprs: Seq[Expression] = generateClauseOutputExprs(
      numOutputCols,
      processedNotMatchBySourceClauses,
      noopCopyExprs)

    // ==== Generate N + 2 (N + 4 preserving Row Tracking) expressions that invokes the MATCHED,
    // NOT MATCHED and NOT MATCHED BY SOURCE expressions ====
    // That is, conditionally invokes them based on whether there was a match in the outer join.

    // Predicates to check whether there was a match in the full outer join.
    val ifSourceRowNull = col(SOURCE_ROW_PRESENT_COL).isNull.expr
    val ifTargetRowNull = col(TARGET_ROW_PRESENT_COL).isNull.expr

    val outputCols = targetWriteColNames.zipWithIndex.map { case (name, i) =>
      // Coupled with the clause conditions, the resultant possibly-nested CaseWhens can
      // be the following for every i-th column. (In the case with single matched/not-matched
      // clauses, instead of nested CaseWhens, there will be If/Else.)
      //
      // CASE WHEN <target row not matched>           (source row is null)
      //          CASE WHEN <not-matched-by-source condition 1>
      //               THEN <execute i-th expression of not-matched-by-source action 1>
      //               WHEN <not-matched-by-source condition 2>
      //               THEN <execute i-th expression of not-matched-by-source action 2>
      //               ...
      //               ELSE <execute i-th expression to noop-copy>
      //
      //      WHEN <source row not matched>           (target row is null)
      //      THEN
      //          CASE WHEN <not-matched condition 1>
      //               THEN <execute i-th expression of not-matched (insert) action 1>
      //               WHEN <not-matched condition 2>
      //               THEN <execute i-th expression of not-matched (insert) action 2>
      //               ...
      //               ELSE <execute i-th expression to delete>
      //
      //      ELSE                                    (both source and target row are not null)
      //          CASE WHEN <match condition 1>
      //               THEN <execute i-th expression of match action 1>
      //               WHEN <match condition 2>
      //               THEN <execute i-th expression of match action 2>
      //               ...
      //               ELSE <execute i-th expression to noop-copy>
      //
      val caseWhen = CaseWhen(Seq(
        ifSourceRowNull -> notMatchedBySourceExprs(i),
        ifTargetRowNull -> notMatchedExprs(i)),
        /*  otherwise  */ matchedExprs(i))
      if (rowIdColumnExpressionOpt.exists(_.name == name)) {
        // Add Row ID metadata to allow writing the column.
        Column(Alias(caseWhen, name)(
          explicitMetadata = Some(RowId.columnMetadata(name))))
      } else if (rowCommitVersionColumnExpressionOpt.exists(_.name == name)) {
        // Add Row Commit Versions metadata to allow writing the column.
        Column(Alias(caseWhen, name)(
          explicitMetadata = Some(RowCommitVersion.columnMetadata(name))))
      } else {
        Column(Alias(caseWhen, name)())
      }
    }
    logDebug("writeAllChanges: join output expressions\n\t" + seqToString(outputCols.map(_.expr)))
    outputCols
  }.toIndexedSeq

  /**
   * Represents a merge clause after its condition and action expressions have been processed before
   * generating the final output expression.
   * @param condition Optional precomputed condition.
   * @param actions List of output expressions generated from every action of the clause.
   */
  protected case class ProcessedClause(condition: Option[Expression], actions: Seq[Expression])

  /**
   * Generate expressions for every output column and every merge clause based on the corresponding
   * UPDATE, DELETE and/or INSERT action(s).
   * @param targetWriteCols List of output column expressions from the target table. Used to
   *                        generate CDC data for DELETE.
   * @param rowIdColumnExpressionOpt The optional Row ID preservation column with the physical
   *                                 Row ID name, it stores stable Row IDs of the table.
   * @param rowCommitVersionColumnExpressionOpt The optional Row Commit Version preservation
   *                                 column with the physical Row Commit Version name, it stores
   *                                 stable Row Commit Versions.
   * @param clausesWithPrecompConditions List of merge clauses with precomputed conditions. Action
   *                                     expressions are generated for each of these clauses.
   * @param cdcEnabled Whether the generated expressions should include CDC information.
   * @param shouldCountDeletedRows Whether metrics for number of deleted rows should be incremented
   *                               here.
   * @return For each merge clause, a list of [[ProcessedClause]] each with a precomputed
   *         condition and N+2 action expressions (N output columns + [[ROW_DROPPED_COL]] +
   *         [[CDC_TYPE_COLUMN_NAME]]) to apply on a row when that clause matches.
   */
  protected def generateAllActionExprs(
      targetWriteCols: Seq[Expression],
      rowIdColumnExpressionOpt: Option[NamedExpression],
      rowCommitVersionColumnExpressionOpt: Option[NamedExpression],
      clausesWithPrecompConditions: Seq[DeltaMergeIntoClause],
      cdcEnabled: Boolean,
      shouldCountDeletedRows: Boolean): Seq[ProcessedClause] = {
    clausesWithPrecompConditions.map { clause =>
      val actions = clause match {
        // Seq of up to N+3 expressions to generate output rows based on the UPDATE, DELETE and/or
        // INSERT action(s)
        case u: DeltaMergeIntoMatchedUpdateClause =>
          val incrCountExpr = incrementMetricsAndReturnBool(
            names = Seq("numTargetRowsUpdated", "numTargetRowsMatchedUpdated"),
            valueToReturn = false)
          // Generate update expressions and set ROW_DROPPED_COL = false
          u.resolvedActions.map(_.expr) ++
            rowIdColumnExpressionOpt ++
            rowCommitVersionColumnExpressionOpt.map(_ => Literal(null)) ++
            Seq(incrCountExpr) ++
            (if (cdcEnabled) Some(Literal(CDC_TYPE_UPDATE_POSTIMAGE)) else None)
        case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
          val incrCountExpr = incrementMetricsAndReturnBool(
            names = Seq("numTargetRowsUpdated", "numTargetRowsNotMatchedBySourceUpdated"),
            valueToReturn = false)
          // Generate update expressions and set ROW_DROPPED_COL = false
          u.resolvedActions.map(_.expr) ++
            rowIdColumnExpressionOpt ++
            rowCommitVersionColumnExpressionOpt.map(_ => Literal(null)) ++
            Seq(incrCountExpr) ++
            (if (cdcEnabled) Some(Literal(CDC_TYPE_UPDATE_POSTIMAGE)) else None)
        case _: DeltaMergeIntoMatchedDeleteClause =>
          val incrCountExpr = {
            if (shouldCountDeletedRows) {
              incrementMetricsAndReturnBool(
                names = Seq("numTargetRowsDeleted", "numTargetRowsMatchedDeleted"),
                valueToReturn = true)
            } else {
              Literal.TrueLiteral
            }
          }
          // Generate expressions to set the ROW_DROPPED_COL = true and mark as a DELETE
          targetWriteCols ++
            rowIdColumnExpressionOpt ++
            rowCommitVersionColumnExpressionOpt ++
            Seq(incrCountExpr) ++
            (if (cdcEnabled) Some(CDC_TYPE_DELETE) else None)
        case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
          val incrCountExpr = {
            if (shouldCountDeletedRows) {
              incrementMetricsAndReturnBool(
                names = Seq("numTargetRowsDeleted", "numTargetRowsNotMatchedBySourceDeleted"),
                valueToReturn = true)
            } else {
              Literal.TrueLiteral
            }
          }
          // Generate expressions to set the ROW_DROPPED_COL = true and mark as a DELETE
          targetWriteCols ++
            rowIdColumnExpressionOpt ++
            rowCommitVersionColumnExpressionOpt ++
            Seq(incrCountExpr) ++
            (if (cdcEnabled) Some(CDC_TYPE_DELETE) else None)
        case i: DeltaMergeIntoNotMatchedInsertClause =>
          val incrInsertedCountExpr = incrementMetricsAndReturnBool(
            names = Seq("numTargetRowsInserted"),
            valueToReturn = false)
          i.resolvedActions.map(_.expr) ++
            rowIdColumnExpressionOpt.map(_ => Literal(null)) ++
            rowCommitVersionColumnExpressionOpt.map(_ => Literal(null)) ++
            Seq(incrInsertedCountExpr) ++
            (if (cdcEnabled) Some(Literal(CDC_TYPE_INSERT)) else None)
      }
      ProcessedClause(clause.condition, actions)
    }
  }

  /**
   * Generate the output expression for each output column to apply the correct action for a type of
   * merge clause. For each output column, the resulting expression dispatches the correct action
   * based on all clause conditions.
   * @param numOutputCols Number of output columns.
   * @param clauses List of preprocessed merge clauses to bind together.
   * @param noopExprs Default expression to apply when no condition holds.
   * @return A list of one expression per output column to apply for a type of merge clause.
   */
  protected def generateClauseOutputExprs(
      numOutputCols: Int,
      clauses: Seq[ProcessedClause],
      noopExprs: Seq[Expression]): Seq[Expression] = {
    val clauseExprs = if (clauses.isEmpty) {
      // Nothing to update or delete
      noopExprs
    } else {
      if (clauses.head.condition.isEmpty) {
        // Only one clause without any condition, so the corresponding action expressions
        // can be evaluated directly to generate the output columns.
        clauses.head.actions
      } else if (clauses.length == 1) {
        // Only one clause _with_ a condition, so generate IF/THEN instead of CASE WHEN.
        //
        // For the i-th output column, generate
        // IF <condition> THEN <execute i-th expression of action>
        //                ELSE <execute i-th expression to noop-copy>
        //
        val condition = clauses.head.condition.get
        clauses.head.actions.zip(noopExprs).map { case (a, noop) => If(condition, a, noop) }
      } else {
        // There are multiple clauses. Use `CaseWhen` to conditionally evaluate the right
        // action expressions to output columns
        Seq.range(0, numOutputCols).map { i =>
          // For the i-th output column, generate
          // CASE
          //     WHEN <condition 1> THEN <execute i-th expression of action 1>
          //     WHEN <condition 2> THEN <execute i-th expression of action 2>
          //                        ...
          //                        ELSE <execute i-th expression to noop-copy>
          //
          val conditionalBranches = clauses.map { precomp =>
            precomp.condition.getOrElse(Literal.TrueLiteral) -> precomp.actions(i)
          }
          CaseWhen(conditionalBranches, Some(noopExprs(i)))
        }
      }
    }
    assert(clauseExprs.size == numOutputCols,
      s"incorrect # expressions:\n\t" + seqToString(clauseExprs))
    logDebug(s"writeAllChanges: expressions\n\t" + seqToString(clauseExprs))
    clauseExprs
  }

  /**
   * Build the full output as an array of packed rows, then explode into the final result. Based
   * on the CDC type as originally marked, we produce both rows for the CDC_TYPE_NOT_CDC partition
   * to be written to the main table and rows for the CDC partitions to be written as CDC files.
   *
   * See [[CDCReader]] for general details on how partitioning on the CDC type column works.
   */
  protected def generateCdcAndOutputRows(
      sourceDf: DataFrame,
      outputCols: Seq[Column],
      outputColNames: Seq[String],
      noopCopyExprs: Seq[Expression],
      rowIdColumnNameOpt: Option[String],
      rowCommitVersionColumnNameOpt: Option[String],
      deduplicateDeletes: DeduplicateCDFDeletes): DataFrame = {
    import org.apache.spark.sql.delta.commands.cdc.CDCReader._
    // The main partition just needs to swap in the CDC_TYPE_NOT_CDC value.
    val mainDataOutput =
      outputCols.dropRight(1) :+ Column(CDC_TYPE_NOT_CDC).as(CDC_TYPE_COLUMN_NAME)

    // Deleted rows are sent to the CDC partition instead of the main partition. These rows are
    // marked as dropped, we need to retain them while incrementing the original metric column
    // ourselves.
    val keepRowAndIncrDeletedCountExpr = !outputCols(outputCols.length - 2)
    val deleteCdcOutput = outputCols
      .updated(outputCols.length - 2, keepRowAndIncrDeletedCountExpr.as(ROW_DROPPED_COL))

    // Update preimages need special handling. This is conceptually the same as the
    // transformation for cdcOutputCols, but we have to transform the noop exprs to columns
    // ourselves because it hasn't already been done.
    val cdcNoopExprs = noopCopyExprs.dropRight(2) :+
      Literal.FalseLiteral :+ Literal(CDC_TYPE_UPDATE_PREIMAGE)
    val updatePreimageCdcOutput = cdcNoopExprs.zipWithIndex.map {
      case (e, i) => Column(Alias(e, outputColNames(i))())
    }

    // To avoid duplicate evaluation of nondeterministic column values such as
    // [[GenerateIdentityValues]], we EXPLODE CDC rows first, from which we EXPLODE again,
    // and for each of "insert" and "update_postimage" rows, generate main data rows.
    // The first EXPLODE will force evaluation all nondeterministic expressions,
    // and the second EXPLODE will just copy the generated value from CDC rows
    // to main data. By doing so we ensure nondeterministic column values in CDC and
    // main data rows stay the same.

    val cdcTypeCol = outputCols.last
    val cdcArray = Column(CaseWhen(Seq(
      EqualNullSafe(cdcTypeCol.expr, Literal(CDC_TYPE_INSERT)) -> array(
        struct(outputCols: _*)).expr,

      EqualNullSafe(cdcTypeCol.expr, Literal(CDC_TYPE_UPDATE_POSTIMAGE)) -> array(
        struct(updatePreimageCdcOutput: _*),
        struct(outputCols: _*)).expr,

      EqualNullSafe(cdcTypeCol.expr, CDC_TYPE_DELETE) -> array(
        struct(deleteCdcOutput: _*)).expr),

      // If none of the CDC cases apply (true for purely rewritten target rows, dropped source
      // rows, etc.) just stick to the normal output.
      array(struct(mainDataOutput: _*)).expr
    ))

    val cdcToMainDataArray = Column(If(
      Or(
        EqualNullSafe(col(s"packedCdc.$CDC_TYPE_COLUMN_NAME").expr,
          Literal(CDC_TYPE_INSERT)),
        EqualNullSafe(col(s"packedCdc.$CDC_TYPE_COLUMN_NAME").expr,
          Literal(CDC_TYPE_UPDATE_POSTIMAGE))),
      array(
        col("packedCdc"),
        struct(
          outputColNames
            .dropRight(1)
            .map { n => col(s"packedCdc.`$n`") }
            :+ Column(CDC_TYPE_NOT_CDC).as(CDC_TYPE_COLUMN_NAME): _*)
      ).expr,
      array(col("packedCdc")).expr
    ))

    if (deduplicateDeletes.enabled) {
      deduplicateCDFDeletes(
        deduplicateDeletes,
        sourceDf,
        cdcArray,
        cdcToMainDataArray,
        rowIdColumnNameOpt,
        rowCommitVersionColumnNameOpt,
        outputColNames)
    } else {
      packAndExplodeCDCOutput(
        sourceDf,
        cdcArray,
        cdcToMainDataArray,
        rowIdColumnNameOpt,
        rowCommitVersionColumnNameOpt,
        outputColNames,
        dedupColumns = Nil)
    }
  }

  /**
   * Applies the transformations to generate the CDC output:
   *  1. Transform each input row into its corresponding array of CDC rows, e.g. an updated row
   *     yields: array(update_preimage, update_postimage).
   *  2. Add the main data output for inserted/updated rows to the previously packed CDC data.
   *  3. Explode the result to flatten the packed arrays.
   *
   * @param sourceDf The dataframe generated after processing the merge output.
   * @param cdcArray Transforms the merge output into the corresponding packed CDC data that will be
   *                 written to the CDC partition.
   * @param cdcToMainDataArray Transforms the packed CDC data to add the main data output, i.e. rows
   *                           that are inserted or updated and will be written to the main
   *                           partition.
   * @param rowIdColumnNameOpt The optional Row ID preservation column with the physical Row ID
   *                           name, it stores stable Row IDs.
   * @param rowCommitVersionColumnNameOpt The optional Row Commit Version preservation column
   *                                      with the physical Row Commit Version name, it stores
   *                                      stable Row Commit Versions.
   * @param outputColNames All the main and CDC columns to use in the output.
   * @param dedupColumns Additional columns to add to enable deduplication.
   */
  private def packAndExplodeCDCOutput(
      sourceDf: DataFrame,
      cdcArray: Column,
      cdcToMainDataArray: Column,
      rowIdColumnNameOpt: Option[String],
      rowCommitVersionColumnNameOpt: Option[String],
      outputColNames: Seq[String],
      dedupColumns: Seq[Column]): DataFrame = {
    val unpackedCols = outputColNames.map { name =>
      if (rowIdColumnNameOpt.contains(name)) {
        // Add metadata to allow writing the column although it is not part of the schema.
        col(s"packedData.`$name`").as(name, RowId.columnMetadata(name))
      } else if (rowCommitVersionColumnNameOpt.contains(name)) {
        col(s"packedData.`$name`").as(name, RowCommitVersion.columnMetadata(name))
      } else {
        col(s"packedData.`$name`").as(name)
      }
    }

    sourceDf
      // `explode()` creates a [[Generator]] which can't handle non-deterministic expressions that
      // we use to increment metric counters. We first project the CDC array so that the expressions
      // are evaluated before we explode the array.
      .select(cdcArray.as("projectedCDC") +: dedupColumns: _*)
      .select(explode(col("projectedCDC")).as("packedCdc") +: dedupColumns: _*)
      .select(explode(cdcToMainDataArray).as("packedData") +: dedupColumns: _*)
      .select(unpackedCols ++ dedupColumns: _*)
  }

  /**
   * This method deduplicates CDF deletes where a target row has potentially multiple matches. It
   * assumes that the input dataframe contains the [[TARGET_ROW_INDEX_COL]] and
   * to detect inserts the [[SOURCE_ROW_INDEX_COL]] column to track the origin of the row.
   *
   * All duplicates of deleted rows have the same [[TARGET_ROW_INDEX_COL]] and
   * [[CDC_TYPE_COLUMN_NAME]] therefore we use both columns as compound deduplication key.
   * In case the input data frame contains additional insert rows we leave them untouched by using
   * the [[SOURCE_ROW_INDEX_COL]] to fill the null values of the [[TARGET_ROW_INDEX_COL]]. This
   * may lead to duplicates as part of the final row index but this is not a problem since if
   * an insert and a delete have the same [[TARGET_ROW_INDEX_COL]] they definitely have a
   * different [[CDC_TYPE_COLUMN_NAME]].
   */
  private def deduplicateCDFDeletes(
      deduplicateDeletes: DeduplicateCDFDeletes,
      df: DataFrame,
      cdcArray: Column,
      cdcToMainDataArray: Column,
      rowIdColumnNameOpt: Option[String],
      rowCommitVersionColumnNameOpt: Option[String],
      outputColNames: Seq[String]): DataFrame = {
    val dedupColumns = if (deduplicateDeletes.includesInserts) {
      Seq(col(TARGET_ROW_INDEX_COL), col(SOURCE_ROW_INDEX_COL))
    } else {
      Seq(col(TARGET_ROW_INDEX_COL))
    }

    val cdcDf = packAndExplodeCDCOutput(
      df,
      cdcArray,
      cdcToMainDataArray,
      rowIdColumnNameOpt,
      rowCommitVersionColumnNameOpt,
      outputColNames,
      dedupColumns
    )

    val cdcDfWithIncreasingIds = if (deduplicateDeletes.includesInserts) {
      cdcDf.withColumn(
        TARGET_ROW_INDEX_COL,
        coalesce(col(TARGET_ROW_INDEX_COL), col(SOURCE_ROW_INDEX_COL)))
    } else {
      cdcDf
    }
    cdcDfWithIncreasingIds
      .dropDuplicates(TARGET_ROW_INDEX_COL, CDC_TYPE_COLUMN_NAME)
      .drop(TARGET_ROW_INDEX_COL, SOURCE_ROW_INDEX_COL)
  }
}

/**
 * This class enables and configures the deduplication of CDF deletes in case the merge statement
 * contains an unconditional delete statement that matches multiple target rows.
 *
 * @param enabled CDF generation should be enabled and duplicate target matches are detected
 * @param includesInserts in addition to the unconditional deletes the merge also inserts rows
 */
case class DeduplicateCDFDeletes(
  enabled: Boolean,
  includesInserts: Boolean)

object MergeOutputGeneration {
  final val TARGET_ROW_INDEX_COL = "_target_row_index_"
  final val SOURCE_ROW_INDEX_COL = "_source_row_index"
}
