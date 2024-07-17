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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.ResolveDeltaMergeInto.ResolveExpressionsFn

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

case class TargetTableResolutionResult(
    unresolvedAttribute: UnresolvedAttribute,
    expr: Expression
)

/** Base trait with helpers for resolving DeltaMergeAction. */
trait DeltaMergeActionResolverBase {
  /** The SQL configuration for this query. */
  def conf: SQLConf
  /** Function we want to use for resolving expressions. */
  def resolveExprsFn: ResolveExpressionsFn
  /** The resolved target plan of the MERGE INTO statement. */
  def target: LogicalPlan
  /** The resolved source plan of the MERGE INTO statement. */
  def source: LogicalPlan

  /** Used for constructing error messages. */
  private lazy val colsAsSQLText = target.output.map(_.sql).mkString(", ")

  /** Try to resolve a single target column in the Merge action. */
  protected def resolveSingleTargetColumn(
       unresolvedAttribute: UnresolvedAttribute,
       mergeClauseTypeStr: String,
       shouldTryUnresolvedTargetExprOnSource: Boolean): Expression = {
    // Resolve the target column name without database/table/view qualifiers
    // If clause allows nested field to be target, then this will return all the
    // parts of the name (e.g., "a.b" -> Seq("a", "b")). Otherwise, this will
    // return only one string.
    try {
      ResolveDeltaMergeInto.resolveSingleExprOrFail(
        resolveExprsFn,
        unresolvedAttribute,
        Seq(target),
        mergeClauseTypeStr
      )
    } catch {
      // Allow schema evolution for update and insert non-star when the column is not in
      // the target.
      case _: AnalysisException if shouldTryUnresolvedTargetExprOnSource =>
        ResolveDeltaMergeInto.resolveSingleExprOrFail(
          resolveExprsFn,
          unresolvedAttribute,
          Seq(source),
          mergeClauseTypeStr
        )
    }
  }

  /**
   * Takes the resolvedKey which refers to the target column in the relation and
   * the corresponding resolvedRHSExpr which describes the assignment value and return
   * a resolved DeltaMergeAction.
   */
  protected def buildDeltaMergeAction(
      resolvedKey: Expression,
      resolvedRHSExpr: Expression,
      mergeClauseTypeStr: String): DeltaMergeAction = {
    val sqlText = resolvedKey.sql
    lazy val resolutionErrorMsg =
      s"Cannot resolve $sqlText in target columns in $mergeClauseTypeStr given " +
        s"columns $colsAsSQLText"
    val resolvedNameParts =
      DeltaUpdateTable.getTargetColNameParts(resolvedKey, resolutionErrorMsg)
    DeltaMergeAction(resolvedNameParts, resolvedRHSExpr, targetColNameResolved = true)
  }

  /**
   * Takes a sequence of DeltaMergeActions and returns the
   * corresponding resolved DeltaMergeActions.
   */
  def resolve(
      clauseType: String,
      plansToResolveAction: Seq[LogicalPlan],
      shouldTryUnresolvedTargetExprOnSource: Boolean,
      deltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction]
}

class IndividualDeltaMergeActionResolver(
    override val target: LogicalPlan,
    override val source: LogicalPlan,
    override val conf: SQLConf,
    override val resolveExprsFn: ResolveExpressionsFn
  ) extends DeltaMergeActionResolverBase {

  /** Resolve DeltaMergeAction, one at a time. */
  override def resolve(
      mergeClauseTypeStr: String,
      plansToResolveAction: Seq[LogicalPlan],
      shouldTryUnresolvedTargetExprOnSource: Boolean,
      deltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction] = {
    deltaMergeActions.map {
      case d @ DeltaMergeAction(colNameParts, expr, _) if !d.resolved =>
        val unresolvedAttrib = UnresolvedAttribute(colNameParts)
        val resolvedKey = resolveSingleTargetColumn(
          unresolvedAttrib, mergeClauseTypeStr, shouldTryUnresolvedTargetExprOnSource)
        val resolvedExpr =
          resolveExprsFn(Seq(expr), plansToResolveAction).head
        buildDeltaMergeAction(resolvedKey, resolvedExpr, mergeClauseTypeStr)
      // Already resolved
      case d => d
    }
  }
}

class BatchedDeltaMergeActionResolver(
    override val target: LogicalPlan,
    override val source: LogicalPlan,
    override val conf: SQLConf,
    override val resolveExprsFn: ResolveExpressionsFn
  ) extends DeltaMergeActionResolverBase {

  /**
   * Attempt to batch resolve the target columns reference all at once. If we are
   * unable to resolve against the target plan, we retry against the source plan
   * if schema evolution is enabled and it's appropriate for the clause type.
   */
  private def batchResolveTargetColumns(
      unresolvedAttrSeq: Seq[UnresolvedAttribute],
      shouldTryUnresolvedTargetExprOnSource: Boolean,
      mergeClauseTypeStr: String): Seq[Expression] = {
    val resolvedExprs = try {
      // Note: unlike [[resolveSingleTargetColumn]], this is not a resolveOrFail.
      // We will not throw an exception if something was not resolved, because we
      // want to resolve as much as possible and only retry to resolve against the
      // source the few columns that failed to resolve. But we must wrap this in a
      // try-catch to swallow exception that come from other parts of invoking the
      // analyzer. We need this to preserve the behaviour where we throw a different
      // exception in PreprocessTableMerge later on...
      resolveExprsFn(unresolvedAttrSeq, Seq(target))
    } catch {
      // We don't know which attribute in the Seq lead to this exception.
      // We need to resolve this one by one, so we can return early here.
      case _: AnalysisException if shouldTryUnresolvedTargetExprOnSource =>
        return unresolvedAttrSeq.map(
          resolveSingleTargetColumn(_, mergeClauseTypeStr, shouldTryUnresolvedTargetExprOnSource))
    }
    assert(unresolvedAttrSeq.length == resolvedExprs.length, "The number of " +
      "resolved expressions should match the number of unresolved expressions")

    val targetTableResolutionResult: Seq[TargetTableResolutionResult] =
      resolvedExprs.zip(unresolvedAttrSeq).map { case (expr, unresolvedAttr) =>
        TargetTableResolutionResult(unresolvedAttr, expr)
      }
    val exprNeedsResolution: Seq[Expression] =
      targetTableResolutionResult.filterNot(_.expr.resolved).map(_.unresolvedAttribute)

    // If we are not supposed to resolve the target column against the source and
    // we were not able to resolve the column, then we should throw an exception
    // at this point.
    if (!shouldTryUnresolvedTargetExprOnSource && exprNeedsResolution.nonEmpty) {
      ResolveDeltaMergeInto.throwIfNotResolved(
        // Use the first of the unresolved attributes to throw the exception.
        targetTableResolutionResult.find(!_.expr.resolved).map(_.expr).get,
        Seq(target),
        mergeClauseTypeStr
      )
    }

    val resolvedExprAgainstSource: Seq[Expression] = ResolveDeltaMergeInto.resolveOrFail(
      resolveExprsFn,
      exprNeedsResolution,
      Seq(source),
      mergeClauseTypeStr
    )
    var index = -1
    val orderedResolvedTargetExprs = targetTableResolutionResult.map {
      case TargetTableResolutionResult(_, expr) =>
        if (expr.resolved) {
          expr
        } else {
          index += 1
          resolvedExprAgainstSource(index)
        }
    }

    orderedResolvedTargetExprs
  }

  /**
   * Batch the resolution of the target column name parts against the target relation
   * and the resolution of assignment expression together.
   *
   * Fundamental assumption: The result of any relational ordering must be preserved
   * with [[resolveExprsFn]]. This is true in SQL in general because of operations like UNION.
   */
  override def resolve(
      mergeClauseTypeStr: String,
      plansToResolveAction: Seq[LogicalPlan],
      shouldTryUnresolvedTargetExprOnSource: Boolean,
      deltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction] = {
    val (alreadyResolvedDeltaMergeActions, unresolvedDeltaMergeActions) =
      deltaMergeActions.partition(_.resolved)

    // Batch the unresolved attributes to resolve them in a single pass.
    val unresolvedAttrSeq = unresolvedDeltaMergeActions
      .map(mergeAction => UnresolvedAttribute(mergeAction.targetColNameParts))
    val orderedResolvedTargetExprs = batchResolveTargetColumns(
      unresolvedAttrSeq,
      shouldTryUnresolvedTargetExprOnSource,
      mergeClauseTypeStr)

    // Now we deal with the expressions for each target column (RHS assignment).
    val unresolvedRHSExprSeq = unresolvedDeltaMergeActions.map(_.expr)
    val resolvedExprsSeq =
      resolveExprsFn(unresolvedRHSExprSeq, plansToResolveAction)
    assert(resolvedExprsSeq.length == orderedResolvedTargetExprs.length)
    resolvedExprsSeq.foreach(
      ResolveDeltaMergeInto.throwIfNotResolved(_, plansToResolveAction, mergeClauseTypeStr))

    // Combine the resolved target columns and the resolved expressions to create
    // the final resolved DeltaMergeAction
    val resolvedDeltaMergeActions: Seq[DeltaMergeAction] =
      orderedResolvedTargetExprs.zip(resolvedExprsSeq).map {
        case (resolvedKey, resolvedExpr) =>
          buildDeltaMergeAction(resolvedKey, resolvedExpr, mergeClauseTypeStr)
      }

    // The order for this Seq doesn't matter.
    alreadyResolvedDeltaMergeActions ++ resolvedDeltaMergeActions
  }
}
