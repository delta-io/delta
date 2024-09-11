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

package io.delta.tables

import scala.collection.JavaConverters._
import scala.collection.Map

import org.apache.spark.sql.delta.{DeltaAnalysisException, PostHocResolveUpCast, PreprocessTableMerge, ResolveDeltaMergeInto}
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.DeltaViewHelper
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.annotation._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf

/**
 * Builder to specify how to merge data from source DataFrame into the target Delta table.
 * You can specify any number of `whenMatched` and `whenNotMatched` clauses.
 * Here are the constraints on these clauses.
 *
 *   - `whenMatched` clauses:
 *
 *     - The condition in a `whenMatched` clause is optional. However, if there are multiple
 *       `whenMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenMatched` clauses and there are conditions (or the lack
 *       of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenMatched` clauses matters.
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
 *     - The condition in a `whenNotMatched` clause is optional. However, if there are
 *       multiple `whenNotMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatched` clauses and there are conditions (or the
 *       lack of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenNotMatched` clauses matters.
 *
 *     - If no `whenNotMatched` clause is present or if it is present but the non-matching source
 *       row does not satisfy the condition, then the source row is not inserted.
 *
 *     - If you want to insert all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use
 *       `whenNotMatched(...).insertAll()`. This is equivalent to
 *       <pre>
 *         whenNotMatched(...).insertExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       </pre>
 *
 *   - `whenNotMatchedBySource` clauses:
 *
 *     - The condition in a `whenNotMatchedBySource` clause is optional. However, if there are
 *       multiple `whenNotMatchedBySource` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatchedBySource` clauses and there are conditions (or
 *       the lack of) such that a row satisfies multiple clauses, then the action for the first
 *       clause satisfied is executed. In other words, the order of the `whenNotMatchedBySource`
 *       clauses matters.
 *
 *     - If no `whenNotMatchedBySource` clause is present or if it is present but the
 *       non-matching target row does not satisfy any of the `whenNotMatchedBySource` clause
 *       condition, then the target row will not be updated or deleted.
 *
 *
 * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .withSchemaEvolution()
 *     .whenMatched()
 *     .updateExpr(Map(
 *       "value" -> "source.value"))
 *     .whenNotMatched()
 *     .insertExpr(Map(
 *       "key" -> "source.key",
 *       "value" -> "source.value"))
 *     .whenNotMatchedBySource()
 *     .updateExpr(Map(
 *       "value" -> "target.value + 1"))
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
 *     .withSchemaEvolution()
 *     .whenMatched()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *          put("value", "source.value");
 *        }})
 *     .whenNotMatched()
 *     .insertExpr(
 *        new HashMap<String, String>() {{
 *         put("key", "source.key");
 *         put("value", "source.value");
 *       }})
 *     .whenNotMatchedBySource()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *         put("value", "target.value + 1");
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
    private val whenClauses: Seq[DeltaMergeIntoClause],
    private val schemaEvolutionEnabled: Boolean)
  extends AnalysisHelper
  with Logging
  {

  def this(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column,
      whenClauses: Seq[DeltaMergeIntoClause]) =
    this(targetTable, source, onCondition, whenClauses, schemaEvolutionEnabled = false)

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
   * Build the actions to perform when the merge condition was not matched by the source. This
   * returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object which can be used to specify how
   * to update or delete the target table row.
   * @since 2.3.0
   */
  def whenNotMatchedBySource(): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: String): DeltaMergeNotMatchedBySourceActionBuilder = {
    whenNotMatchedBySource(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row .
   *
   * @param condition boolean expression as a Column object
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: Column): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, Some(condition))
  }

  /**
   * Enable schema evolution for the merge operation. This allows the schema of the target
   * table/columns to be automatically updated based on the schema of the source table/columns.
   *
   * @since 3.2.0
   */
  def withSchemaEvolution(): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenClauses,
      schemaEvolutionEnabled = true)
  }

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   *
   * @since 0.3.0
   */
  def execute(): Unit = improveUnsupportedOpError {
    val sparkSession = targetTable.toDF.sparkSession
    withActiveSession(sparkSession) {
      // Note: We are explicitly resolving DeltaMergeInto plan rather than going to through the
      // Analyzer using `Dataset.ofRows()` because the Analyzer incorrectly resolves all
      // references in the DeltaMergeInto using both source and target child plans, even before
      // DeltaAnalysis rule kicks in. This is because the Analyzer  understands only MergeIntoTable,
      // and handles that separately by skipping resolution (for Delta) and letting the
      // DeltaAnalysis rule do the resolving correctly. This can be solved by generating
      // MergeIntoTable instead, which blocked by the different issue with MergeIntoTable as
      // explained in the function `mergePlan` and
      // https://issues.apache.org/jira/browse/SPARK-34962.
      val resolvedMergeInto =
      ResolveDeltaMergeInto.resolveReferencesAndSchema(mergePlan, sparkSession.sessionState.conf)(
        tryResolveReferencesForExpressions(sparkSession))
      if (!resolvedMergeInto.resolved) {
        throw new ExtendedAnalysisException(
          new DeltaAnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_DELTA_0011",
            messageParameters = Array.empty
          ),
          resolvedMergeInto
        )
      }
      val strippedMergeInto = resolvedMergeInto.copy(
        target = DeltaViewHelper.stripTempViewForMerge(resolvedMergeInto.target, SQLConf.get)
      )
      // Preprocess the actions and verify
      var mergeIntoCommand =
        PreprocessTableMerge(sparkSession.sessionState.conf)(strippedMergeInto)
      // Resolve UpCast expressions that `PreprocessTableMerge` may have introduced.
      mergeIntoCommand = PostHocResolveUpCast(sparkSession).apply(mergeIntoCommand)
      sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
      toDataset(sparkSession, mergeIntoCommand)
    }
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withClause(clause: DeltaMergeIntoClause): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenClauses :+ clause,
      this.schemaEvolutionEnabled)
  }

  private def mergePlan: DeltaMergeInto = {
    var targetPlan = targetTable.toDF.queryExecution.analyzed
    var sourcePlan = source.queryExecution.analyzed
    var condition = onCondition.expr
    var clauses = whenClauses

    // If source and target have duplicate, pre-resolved references (can happen with self-merge),
    // then rewrite the references in target with new exprId to avoid ambiguity.
    // We rewrite the target instead of ths source because the source plan can be arbitrary and
    // we know that the target plan is simple combination of LogicalPlan and an
    // optional SubqueryAlias.
    val duplicateResolvedRefs = targetPlan.outputSet.intersect(sourcePlan.outputSet)
    if (duplicateResolvedRefs.nonEmpty) {
      val exprs = (condition +: clauses).map(_.transform {
        // If any expression contain duplicate, pre-resolved references, we can't simply
        // replace the references in the same way as the target because we don't know
        // whether the user intended to refer to the source or the target columns. Instead,
        // we unresolve them (only the duplicate refs) and let the analysis resolve the ambiguity
        // and throw the usual error messages when needed.
        case a: AttributeReference if duplicateResolvedRefs.contains(a) =>
          UnresolvedAttribute(a.qualifier :+ a.name)
      })
      // Deduplicate the attribute IDs in the target and source plans, and all the MERGE
      // expressions (condition and MERGE clauses), so that we can avoid duplicated attribute ID
      // when building the MERGE command later.
      val fakePlan = AnalysisHelper.FakeLogicalPlan(exprs, Seq(sourcePlan, targetPlan))
      val newPlan = org.apache.spark.sql.catalyst.analysis.DeduplicateRelations(fakePlan)
        .asInstanceOf[AnalysisHelper.FakeLogicalPlan]
      sourcePlan = newPlan.children(0)
      targetPlan = newPlan.children(1)
      condition = newPlan.exprs.head
      clauses = newPlan.exprs.takeRight(clauses.size).asInstanceOf[Seq[DeltaMergeIntoClause]]
    }

    // Note: The Scala API cannot generate MergeIntoTable just like the SQL parser because
    // UpdateAction in MergeIntoTable does not have any way to differentiate between
    // the representations of `updateAll()` and `update(some-condition, empty-actions)`.
    // More specifically, UpdateAction with a list of empty Assignments implicitly represents
    // `updateAll()`, so there is no way to represent `update()` with zero column assignments
    // (possible in Scala API, but syntactically not possible in SQL). This issue is tracked
    // by https://issues.apache.org/jira/browse/SPARK-34962.
    val merge = DeltaMergeInto(
      targetPlan, sourcePlan, condition, clauses, withSchemaEvolution = schemaEvolutionEnabled)
    logDebug("Generated merged plan:\n" + merge)
    merge
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
    val updateClause = DeltaMergeIntoMatchedUpdateClause(
      matchCondition.map(_.expr),
      DeltaMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(updateClause)
  }

  /**
   * Delete a matched row from the table.
   * @since 0.3.0
   */
  def delete(): DeltaMergeBuilder = {
    val deleteClause = DeltaMergeIntoMatchedDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = DeltaMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = DeltaMergeIntoMatchedUpdateClause(
        matchCondition.map(_.expr),
        updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
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
    val insertClause = DeltaMergeIntoNotMatchedInsertClause(
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
    val insertClause = DeltaMergeIntoNotMatchedInsertClause(
      notMatchCondition.map(_.expr),
      insertActions)
    mergeBuilder.withClause(insertClause)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
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

/**
 * Builder class to specify the actions to perform when a target table row has no match in the
 * source table based on the given merge condition and optional match condition.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 2.3.0
 */
class DeltaMergeNotMatchedBySourceActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchBySourceCondition: Option[Column]) {

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 2.3.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 2.3.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @since 2.3.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala)
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @since 2.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala))
  }

  /**
   * Delete an unmatched row from the target table.
   * @since 2.3.0
   */
  def delete(): DeltaMergeBuilder = {
    val deleteClause =
      DeltaMergeIntoNotMatchedBySourceDeleteClause(notMatchBySourceCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && notMatchBySourceCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = DeltaMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = DeltaMergeIntoNotMatchedBySourceUpdateClause(
        notMatchBySourceCondition.map(_.expr),
        updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
}

object DeltaMergeNotMatchedBySourceActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchBySourceCondition: Option[Column]): DeltaMergeNotMatchedBySourceActionBuilder = {
    new DeltaMergeNotMatchedBySourceActionBuilder(mergeBuilder, notMatchBySourceCondition)
  }
}
