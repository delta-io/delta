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

package org.apache.spark.sql.catalyst.plans.logical

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * Represents an action in MERGE's UPDATE or INSERT clause where a target columns is assigned the
 * value of an expression
 * @param targetColNameParts The name parts of the target column. This is a sequence to support
 *                           nested fields as targets.
 * @param expr Expression to generate the value of the target column.
 */
case class MergeAction(targetColNameParts: Seq[String], expr: Expression)
  extends UnaryExpression with Unevaluable {
  override def child: Expression = expr
  override def foldable: Boolean = false
  override def dataType: DataType = expr.dataType
  override def sql: String = s"${targetColNameParts.mkString("`", "`.`", "`")} = ${expr.sql}"
  override def toString: String = s"$prettyName ( $sql )"
}


/**
 * Trait that represents a WHEN clause in MERGE. See [[MergeInto]]. It extends [[Expression]]
 * so that Catalyst can find all the expressions in the clause implementations.
 */
sealed trait MergeIntoClause extends Expression with Unevaluable {
  /** Optional condition of the clause */
  def condition: Option[Expression]

  /**
   * Sequence of actions represented as expressions. Note that this can be only be either
   * UnresolvedStar, or MergeAction.
   */
  def actions: Seq[Expression]

  /**
   * Sequence of resolved actions represented as Aliases. Actions, once resolved, must
   * be Aliases and not any other NamedExpressions. So it should be safe to do this casting
   * as long as this is called after the clause has been resolved.
   */
  def resolvedActions: Seq[MergeAction] = {
    assert(actions.forall(_.resolved), "all actions have not been resolved yet")
    actions.map(_.asInstanceOf[MergeAction])
  }

  def clauseType: String = getClass.getSimpleName.replace("MergeInto", "").replace("Clause", "")

  override def toString: String = {
    val condStr = condition.map { c => s"condition: ${c.sql}" }.getOrElse("")
    val actionStr = if (actions.isEmpty) "" else {
      "actions: " + actions.map(_.sql).mkString(", ")
    }
    s"$clauseType [${Seq(condStr, actionStr).mkString(", ")}]"
  }

  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = null
  override def children: Seq[Expression] = condition.toSeq ++ actions

  /** Verify whether the expressions in the actions are of the right type */
  protected[logical] def verifyActions(): Unit = actions.foreach {
    case _: UnresolvedStar =>
    case _: MergeAction =>
    case a => throw new IllegalArgumentException(s"Unexpected action expression $a in $this")
  }
}


object MergeIntoClause {
  /**
   * Convert the parsed columns names and expressions into action for MergeInto. Note:
   * - Size of column names and expressions must be the same.
   * - If the sizes are zeros and `emptySeqIsStar` is true, this function assumes
   *   that query had `*` as an action, and therefore generates a single action
   *   with `UnresolvedStar`. This will be expanded later during analysis.
   * - Otherwise, this will convert the names and expressions to MergeActions.
   */
  def toActions(
      colNames: Seq[UnresolvedAttribute],
      exprs: Seq[Expression],
      isEmptySeqEqualToStar: Boolean = true): Seq[Expression] = {
    assert(colNames.size == exprs.size)
    if (colNames.isEmpty && isEmptySeqEqualToStar) {
      Seq[Expression](UnresolvedStar(None))
    } else {
      colNames.zip(exprs).map { case (col, expr) => MergeAction(col.nameParts, expr) }
    }
  }
}

/** Trait that represents WHEN MATCHED clause in MERGE. See [[MergeInto]]. */
sealed trait MergeIntoMatchedClause extends MergeIntoClause

/** Represents the clause WHEN MATCHED THEN UPDATE in MERGE. See [[MergeInto]]. */
case class MergeIntoUpdateClause(condition: Option[Expression], actions: Seq[Expression])
  extends MergeIntoMatchedClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, MergeIntoClause.toActions(cols, exprs))
}

/** Represents the clause WHEN MATCHED THEN DELETE in MERGE. See [[MergeInto]]. */
case class MergeIntoDeleteClause(condition: Option[Expression]) extends MergeIntoMatchedClause {
  def this(condition: Option[Expression], actions: Seq[MergeAction]) = this(condition)

  override def actions: Seq[Expression] = Seq.empty
}

/** Represents the clause WHEN NOT MATCHED THEN INSERT in MERGE. See [[MergeInto]]. */
case class MergeIntoInsertClause(condition: Option[Expression], actions: Seq[Expression])
  extends MergeIntoClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, MergeIntoClause.toActions(cols, exprs))
}

/**
 * Merges changes specified in the source plan into a target table, based on the given search
 * condition and the actions to perform when the condition is matched or not matched by the rows.
 *
 * The syntax of the MERGE statement is as follows.
 * {{{
 *    MERGE INTO <target_table_with_alias>
 *    USING <source_table_with_alias>
 *    ON <search_condition>
 *    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
 *    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
 *    [ WHEN NOT MATCHED [ AND <condition> ] THEN <not_matched_action> ]
 *
 *    where
 *    <matched_action> = DELETE | UPDATE SET column1 = value1 [, column2 = value2 ...]
 *    <not_matched_action> = INSERT (column1 [, column2 ...]) VALUES (expr1 [, expr2 ...])
 * }}}
 *
 * - There can be 1, 2 or 3 WHEN clauses. Of these, at most 2 can be WHEN MATCHED clauses, and
 * at most 1 can be WHEN NOT MATCHED clause.
 * - WHEN MATCHED clauses:
 *    - There can be at most one UPDATE action and DELETE action in the MATCHED clauses.
 *    - Each WHEN MATCHED clause can have an optional condition. However, If there are two
 * WHEN MATCHED clauses, then the first one must have a condition.
 *    - When there are two MATCHED clauses and there are conditions (or the lack of) that
 * allow a row to match both MATCHED clauses, then the first clause/action is executed.
 * In other words, the order of the MATCHED clauses matter.
 * - WHEN NOT MATCHED clause:
 *    - Can only have the INSERT action. If present, it must be the last WHEN clause.
 *    - WHEN NOT MATCHED clause can have an optional condition.
 */
case class MergeInto(
    target: LogicalPlan,
    source: LogicalPlan,
    condition: Expression,
    matchedClauses: Seq[MergeIntoMatchedClause],
    notMatchedClause: Option[MergeIntoInsertClause]) extends Command {

  (matchedClauses ++ notMatchedClause).foreach(_.verifyActions())

  override def children: Seq[LogicalPlan] = Seq(target, source)
  override def output: Seq[Attribute] = Seq.empty
}

object MergeInto {
  def apply(
      target: LogicalPlan,
      source: LogicalPlan,
      condition: Expression,
      whenClauses: Seq[MergeIntoClause]): MergeInto = {
    val deleteClauses = whenClauses.collect { case x: MergeIntoDeleteClause => x }
    val updateClauses = whenClauses.collect { case x: MergeIntoUpdateClause => x }
    val insertClauses = whenClauses.collect { case x: MergeIntoInsertClause => x }
    val matchedClauses = whenClauses.collect { case x: MergeIntoMatchedClause => x }

    // grammar enforcement goes here.
    if (whenClauses.isEmpty) {
      throw new AnalysisException("There must be at least one WHEN clause in a MERGE query")
    }

    if (matchedClauses.length == 2 &&
      matchedClauses.apply(0).condition.isEmpty) {
      throw new AnalysisException("When there are 2 MATCHED clauses in a MERGE query, " +
        "the first MATCHED clause must have a condition")
    }

    if (matchedClauses.length > 2) {
      throw new AnalysisException("There must be at most two match clauses in a MERGE query")
    }

    if (updateClauses.length >= 2 ||
      deleteClauses.length >= 2 ||
      insertClauses.length >= 2) {
      throw new AnalysisException("INSERT, UPDATE and DELETE cannot appear twice in " +
        "one MERGE query")
    }

    MergeInto(
      target,
      source,
      condition,
      whenClauses.collect { case x: MergeIntoMatchedClause => x }.take(2),
      whenClauses.collectFirst { case x: MergeIntoInsertClause => x })
  }

  def resolveReferences(merge: MergeInto)(
      resolveExpr: (Expression, LogicalPlan) => Expression): MergeInto = {

    val MergeInto(target, source, condition, matchedClauses, notMatchedClause) = merge

    // We must do manual resolution as the expressions in different clauses of the MERGE have
    // visibility of the source, the target or both. Additionally, the resolution logic operates
    // on the output of the `children` of the operator in question. Since for MERGE we must
    // consider each child separately, we also must make these dummy nodes to avoid
    // "skipping" the effects of the top node in these query plans.

    val fakeSourcePlan = Project(source.output, source)
    val fakeTargetPlan = Project(target.output, target)

    /**
     * Resolves expression with given plan or fail using given message. It makes a best-effort
     * attempt to throw specific error messages on which part of the query has a problem.
     */
    def resolveOrFail(expr: Expression, plan: LogicalPlan, mergeClauseType: String): Expression = {
      val resolvedExpr = resolveExpr(expr, plan)
      resolvedExpr.flatMap(_.references).filter(!_.resolved).foreach { a =>
        // Note: This will throw error only on unresolved attribute issues,
        // not other resolution errors like mismatched data types.
        val cols = "columns " + plan.references.map(_.sql).mkString(", ")
        a.failAnalysis(s"cannot resolve ${a.sql} in $mergeClauseType given $cols")
      }
      resolvedExpr
    }

    /**
     * Resolves a clause using the given plan (used for resolving the action exprs) and
     * returns the resolved clause.
     */
    def resolveClause[T <: MergeIntoClause](clause: T, planToResolveAction: LogicalPlan): T = {
      val typ = clause.clauseType.toUpperCase(Locale.ROOT)
      val resolvedActions: Seq[MergeAction] = clause.actions.flatMap { action =>
        action match {
          // For actions like `UPDATE SET *` or `INSERT *`
          case _: UnresolvedStar =>
            // Expand `*` into seq of [ `columnName = sourceColumnBySameName` ] for every target
            // column name. The target columns do not need resolution. The right hand side
            // expression (i.e. sourceColumnBySameName) needs to be resolved only by the source
            // plan.
            fakeTargetPlan.output.map(_.name).map { tgtColName =>
              val resolvedExpr =
                resolveOrFail(UnresolvedAttribute(tgtColName), fakeSourcePlan, s"$typ clause")
              MergeAction(Seq(tgtColName), resolvedExpr)
            }

          // For actions like `UPDATE SET x = a, y = b` or `INSERT (x, y) VALUES (a, b)`
          case MergeAction(colNameParts, expr) =>

            val unresolvedAttrib = UnresolvedAttribute(colNameParts)
            val resolutionErrorMsg =
              s"Cannot resolve ${unresolvedAttrib.sql} in target columns in $typ " +
                s"clause given columns ${target.output.map(_.sql).mkString(", ")}"

            // Resolve the target column name without database/table/view qualifiers
            // If clause allows nested field to be target, then this will return the all the
            // parts of the name (e.g., "a.b" -> Seq("a", "b")). Otherwise, this will
            // return only one string.
            val resolvedNameParts = UpdateTable.getNameParts(
              resolveOrFail(unresolvedAttrib, fakeTargetPlan, s"$typ clause"),
              resolutionErrorMsg,
              merge)

            val resolvedExpr = resolveOrFail(expr, planToResolveAction, s"$typ clause")
            Seq(MergeAction(resolvedNameParts, resolvedExpr))

          case _ =>
            action.failAnalysis(s"Unexpected action expression '$action' in clause $clause")
        }
      }

      val resolvedCondition = clause.condition.map(resolveOrFail(_, merge, s"$typ condition"))
      clause.makeCopy(Array(resolvedCondition, resolvedActions)).asInstanceOf[T]
    }

    // Resolve everything
    val resolvedCond = resolveOrFail(condition, merge, "search condition")
    val resolvedMatchedClauses = matchedClauses.map {
      resolveClause(_, merge)
    }
    val resolvedNotMatchedClause = notMatchedClause.map {
      resolveClause(_, fakeSourcePlan)
    }
    MergeInto(target, source, resolvedCond, resolvedMatchedClauses, resolvedNotMatchedClause)
  }
}
