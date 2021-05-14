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

package io.delta.tables

import scala.collection.JavaConverters._
import scala.collection.Map

import org.apache.spark.sql.delta.{DeltaErrors, PreprocessTableMerge}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.annotation._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.expr

/**
 * Builder to specify how to merge data from source DataFrame into the target Delta table.
 * You can specify 1, 2 or 3 `when` clauses of which there can be at most 2 `whenMatched` clauses
 * and at most 1 `whenNotMatched` clause. Here are the constraints on these clauses.
 *
 *   - `whenMatched` clauses:
 *
 *     - There can be at most one `update` action and one `delete` action in `whenMatched` clauses.
 *
 *     - Each `whenMatched` clause can have an optional condition. However, if there are two
 *       `whenMatched` clauses, then the first one must have a condition.
 *
 *     - When there are two `whenMatched` clauses and there are conditions (or the lack of)
 *       such that a row matches both clauses, then the first clause/action is executed.
 *       In other words, the order of the `whenMatched` clauses matter.
 *
 *     - If none of the `whenMatched` clauses match a source-target row pair that satisfy
 *       the merge condition, then the target rows will not be updated or deleted.
 *
 *     - If you want to update all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use the
 *       `whenMatched(...).updateAll()`. This is equivalent to
 *       <pre>
 *         whenMatched(...).updateExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       </pre>
 *
 *   - `whenNotMatched` clauses:
 *
 *     - This clause can have only an `insert` action, which can have an optional condition.
 *
 *     - If the `whenNotMatched` clause is not present or if it is present but the non-matching
 *       source row does not satisfy the condition, then the source row is not inserted.
 *
 *     - If you want to insert all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use
 *       `whenMatched(...).insertAll()`. This is equivalent to
 *       <pre>
 *         whenMatched(...).insertExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       </pre>
 *
 * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .whenMatched
 *     .updateExpr(Map(
 *       "value" -> "source.value"))
 *     .whenNotMatched
 *     .insertExpr(Map(
 *       "key" -> "source.key",
 *       "value" -> "source.value"))
 *     .execute()
 * }}}
 *
 * Java example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .whenMatched
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *          put("value", "source.value");
 *        }})
 *     .whenNotMatched
 *     .insertExpr(
 *        new HashMap<String, String>() {{
 *         put("key", "source.key");
 *         put("value", "source.value");
 *       }})
 *     .execute();
 * }}}
 *
 * @since 0.3.0
 */
class DeltaMergeBuilder private(
    private val targetTable: DeltaTable,
    private val source: DataFrame,
    private val onCondition: Column,
    private val whenClauses: Seq[DeltaMergeIntoClause])
  extends AnalysisHelper
  with Logging
  {

  /**
   * Build the actions to perform when the merge condition was matched.  This returns
   * [[DeltaMergeMatchedActionBuilder]] object which can be used to specify how
   * to update or delete the matched target table row with the source row.
   * @since 0.3.0
   */
  def whenMatched(): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 0.3.0
   */
  def whenMatched(condition: String): DeltaMergeMatchedActionBuilder = {
    whenMatched(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns a [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a Column object
   * @since 0.3.0
   */
  def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, Some(condition))
  }

  /**
   * Build the action to perform when the merge condition was not matched. This returns
   * [[DeltaMergeNotMatchedActionBuilder]] object which can be used to specify how
   * to insert the new sourced row into the target table.
   * @since 0.3.0
   */
  def whenNotMatched(): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 0.3.0
   */
  def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder = {
    whenNotMatched(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a Column object
   * @since 0.3.0
   */
  def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, Some(condition))
  }

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   *
   * @since 0.3.0
   */
  def execute(): Unit = improveUnsupportedOpError {
    val sparkSession = targetTable.toDF.sparkSession
    // Note: We are explicitly resolving DeltaMergeInto plan rather than going to through the
    // Analyzer using `Dataset.ofRows()` because the Analyzer incorrectly resolves all
    // references in the DeltaMergeInto using both source and target child plans, even before
    // DeltaAnalysis rule kicks in. This is because the Analyzer  understands only MergeIntoTable,
    // and handles that separately by skipping resolution (for Delta) and letting the
    // DeltaAnalysis rule do the resolving correctly. This can be solved by generating
    // MergeIntoTable instead, which blocked by the different issue with MergeIntoTable as explained
    // in the function `mergePlan` and https://issues.apache.org/jira/browse/SPARK-34962.
    val resolvedMergeInto =
      DeltaMergeInto.resolveReferences(mergePlan, sparkSession.sessionState.conf)(
        tryResolveReferences(sparkSession) _)
    if (!resolvedMergeInto.resolved) {
      throw DeltaErrors.analysisException("Failed to resolve\n", plan = Some(resolvedMergeInto))
    }
    // Preprocess the actions and verify
    val mergeIntoCommand = PreprocessTableMerge(sparkSession.sessionState.conf)(resolvedMergeInto)
    sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
    mergeIntoCommand.run(sparkSession)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withClause(clause: DeltaMergeIntoClause): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable, this.source, this.onCondition, this.whenClauses :+ clause)
  }

  private def mergePlan: DeltaMergeInto = {
    var targetPlan = targetTable.toDF.queryExecution.analyzed
    val sourcePlan = source.queryExecution.analyzed

    // If source and target have duplicate, pre-resolved references (can happen with self-merge),
    // then rewrite the references in target with new exprId to avoid ambiguity.
    // We rewrite the target instead of ths source because the source plan can be arbitrary and
    // we know that the target plan is simple combination of LogicalPlan and an
    // optional SubqueryAlias.
    val duplicateResolvedRefs = targetPlan.outputSet.intersect(sourcePlan.outputSet)
    if (duplicateResolvedRefs.nonEmpty) {
      val refReplacementMap = duplicateResolvedRefs.toSeq.flatMap {
        case a: AttributeReference =>
          Some(a.exprId -> a.withExprId(NamedExpression.newExprId))
        case _ => None
      }.toMap
      targetPlan = targetPlan.transformAllExpressions {
        case a: AttributeReference if refReplacementMap.contains(a.exprId) =>
          refReplacementMap(a.exprId)
      }
      logInfo("Rewritten duplicate refs between target and source plans: "
        + refReplacementMap.toSeq.mkString(", "))
    }

    // Note: The Scala API cannot generate MergeIntoTable just like the SQL parser because
    // UpdateAction in MergeIntoTable does not have any way to differentiate between
    // the representations of `updateAll()` and `update(some-condition, empty-actions)`.
    // More specifically, UpdateAction with a list of empty Assignments implicitly represents
    // `updateAll()`, so there is no way to represent `update()` with zero column assignments
    // (possible in Scala API, but syntactically not possible in SQL). This issue is tracked
    // by https://issues.apache.org/jira/browse/SPARK-34962.
    val merge = DeltaMergeInto(targetPlan, sourcePlan, onCondition.expr, whenClauses)
    val finalMerge = if (duplicateResolvedRefs.nonEmpty) {
      // If any expression contain duplicate, pre-resolved references, we can't simply
      // replace the references in the same way as the target because we don't know
      // whether the user intended to refer to the source or the target columns. Instead,
      // we unresolve them (only the duplicate refs) and let the analysis resolve the ambiguity
      // and throw the usual error messages when needed.
      merge.transformExpressions {
        case a: AttributeReference if duplicateResolvedRefs.contains(a) =>
          UnresolvedAttribute(a.qualifier :+ a.name)
      }
    } else merge
    logDebug("Generated merged plan:\n" + finalMerge)
    finalMerge
  }
}

object DeltaMergeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column): DeltaMergeBuilder = {
    new DeltaMergeBuilder(targetTable, source, onCondition, Nil)
  }
}

/**
 * Builder class to specify the actions to perform when a target table row has matched a
 * source row based on the given merge condition and optional match condition.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 0.3.0
 */
class DeltaMergeMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column]) {

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala)
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala))
  }

  /**
   * Update all the columns of the matched table row with the values of the
   * corresponding columns in the source row.
   * @since 0.3.0
   */
  def updateAll(): DeltaMergeBuilder = {
    val updateClause = DeltaMergeIntoUpdateClause(
      matchCondition.map(_.expr),
      DeltaMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(updateClause)
  }

  /**
   * Delete a matched row from the table.
   * @since 0.3.0
   */
  def delete(): DeltaMergeBuilder = {
    val deleteClause = DeltaMergeIntoDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      // Nothing to update = no need to add an update clause
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = DeltaMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = DeltaMergeIntoUpdateClause(matchCondition.map(_.expr), updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

object DeltaMergeMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      matchCondition: Option[Column]): DeltaMergeMatchedActionBuilder = {
    new DeltaMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}


/**
 * Builder class to specify the actions to perform when a source row has not matched any target
 * Delta table row based on the merge condition, but has matched the additional condition
 * if specified.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 0.3.0
 */
class DeltaMergeNotMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchCondition: Option[Column]) {

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as Column objects.
   * @since 0.3.0
   */
  def insert(values: Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def insertExpr(values: Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values))
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as Column objects.
   * @since 0.3.0
   */
  def insert(values: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values.asScala)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as SQL formatted strings.
   *
   * @since 0.3.0
   */
  def insertExpr(values: java.util.Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values.asScala))
  }

  /**
   * Insert a new target Delta table row by assigning the target columns to the values of the
   * corresponding columns in the source row.
   * @since 0.3.0
   */
  def insertAll(): DeltaMergeBuilder = {
    val insertClause = DeltaMergeIntoInsertClause(
      notMatchCondition.map(_.expr),
      DeltaMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(insertClause)
  }

  private def addInsertClause(setValues: Map[String, Column]): DeltaMergeBuilder = {
    val values = setValues.toSeq
    val insertActions = DeltaMergeIntoClause.toActions(
      colNames = values.map(x => UnresolvedAttribute.quotedString(x._1)),
      exprs = values.map(x => x._2.expr),
      isEmptySeqEqualToStar = false)
    val insertClause = DeltaMergeIntoInsertClause(notMatchCondition.map(_.expr), insertActions)
    mergeBuilder.withClause(insertClause)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

object DeltaMergeNotMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchCondition: Option[Column]): DeltaMergeNotMatchedActionBuilder = {
    new DeltaMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}
