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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

case class TargetTableResolutionResult(
    unresolvedAttribute: UnresolvedAttribute,
    expr: Expression
)

trait DeltaMergeActionResolverBase {
  def conf: SQLConf
  def resolveExprsFn: (Seq[Expression], Seq[LogicalPlan]) => Seq[Expression]
  def typ: String
  def plansToResolveAction: Seq[LogicalPlan]
  def shouldTryUnresolvedTargetExprOnSource: Boolean
  def merge: DeltaMergeInto

  val DeltaMergeInto(
    target,
    source,
    _,
    _,
    _,
    _,
    _,
    _) = merge

  protected val mergeClauseTypeStr = s"$typ clause"
  private val colsAsSQLText = target.output.map(_.sql).mkString(", ")

  /** Try to resolve a single target column in the Merge action. */
  protected def resolveSingleTargetColumn(unresolvedAttribute: UnresolvedAttribute): Expression = {
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
   * Takes the resolvedKey which is refers the target column in the relation and
   * the corresponding resolvedRHSExpr which describes the assignment value and return
   * a resolved DeltaMergeAction.
   */
  protected def buildDeltaMergeAction(
      resolvedKey: Expression,
      resolvedRHSExpr: Expression): DeltaMergeAction = {
    val sqlText = resolvedKey.sql
    val resolutionErrorMsg =
      s"Cannot resolve $sqlText in target columns in $typ clause given " +
        s"columns $colsAsSQLText"
    val resolvedNameParts =
      DeltaUpdateTable.getTargetColNameParts(resolvedKey, resolutionErrorMsg)
    DeltaMergeAction(resolvedNameParts, resolvedRHSExpr, targetColNameResolved = true)
  }

  def resolve(
      unresolvedDeltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction]
}

class IndividualDeltaMergeActionResolver(
    override val merge: DeltaMergeInto,
    override val conf: SQLConf,
    override val resolveExprsFn: (Seq[Expression], Seq[LogicalPlan]) => Seq[Expression],
    override val typ: String,
    override val plansToResolveAction: Seq[LogicalPlan],
    override val shouldTryUnresolvedTargetExprOnSource: Boolean
  ) extends DeltaMergeActionResolverBase {
  def resolve(
      unresolvedDeltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction] = {
    unresolvedDeltaMergeActions.map { case DeltaMergeAction(colNameParts, expr, _) =>
      val unresolvedAttrib = UnresolvedAttribute(colNameParts)
      val resolvedKey = resolveSingleTargetColumn(unresolvedAttrib)
      val resolvedExpr =
      resolveExprsFn(Seq(expr), plansToResolveAction).head
      buildDeltaMergeAction(resolvedKey, resolvedExpr)
    }
  }
}

class BatchedDeltaMergeActionResolver(
    override val merge: DeltaMergeInto,
    override val conf: SQLConf,
    override val resolveExprsFn: (Seq[Expression], Seq[LogicalPlan]) => Seq[Expression],
    override val typ: String,
    override val plansToResolveAction: Seq[LogicalPlan],
    override val shouldTryUnresolvedTargetExprOnSource: Boolean
  ) extends DeltaMergeActionResolverBase {

  private def batchResolveTargetColumns(
      unresolvedAttrSeq: Seq[UnresolvedAttribute],
      shouldTryUnresolvedTargetExprOnSource: Boolean,
      mergeClauseType: String): Seq[Expression] = {
    val resolvedExprs = try {
      // Note: unlike resolveSingleTargetColumn, this is not a resolveOrFail.
      // We will not throw an exception if something was not resolved, but we must catch
      // exception that come from other parts of invoking the analyzer.
      resolveExprsFn(unresolvedAttrSeq, Seq(target))
    } catch {
      // We don't know which attribute in the Seq lead to this exception.
      // We need to resolve this one by one...
      // Note: We catch the exception here so that we preserve the behaviour where we
      // throw a different exception in PreprocessTableMerge later on...
      case _: AnalysisException if shouldTryUnresolvedTargetExprOnSource =>
        unresolvedAttrSeq.map(resolveSingleTargetColumn)
    }
    assert(unresolvedAttrSeq.length == resolvedExprs.length, "Unexpected error. The number of " +
      "resolved expressions should match the number of unresolved expressions")

    val targetTableResolutionResult: Seq[TargetTableResolutionResult] =
      resolvedExprs.zipWithIndex.map { case (expr, index) =>
        TargetTableResolutionResult(unresolvedAttrSeq(index), expr)
      }
    val exprNeedsResolution: Seq[Expression] =
      targetTableResolutionResult.filter(!_.expr.resolved).map(_.unresolvedAttribute)

    // If we are not supposed to resolve the target column against the source and
    // we were not able to resolve the column, then we should throw an exception
    // at this point.
    if (!shouldTryUnresolvedTargetExprOnSource && exprNeedsResolution.nonEmpty) {
      ResolveDeltaMergeInto.throwIfNotResolved(
        // Use the first of the unresolved attributes to throw the exception.
        targetTableResolutionResult.filter(!_.expr.resolved).map(_.expr).head,
        Seq(target),
        mergeClauseType
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
   */
  override def resolve(
      unresolvedDeltaMergeActions: Seq[DeltaMergeAction]): Seq[DeltaMergeAction] = {
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

    val resolvedDeltaMergeActions: Seq[DeltaMergeAction] =
      orderedResolvedTargetExprs.zip(resolvedExprsSeq).map {
        case (resolvedKey, resolvedExpr) => buildDeltaMergeAction(resolvedKey, resolvedExpr)
      }

    resolvedDeltaMergeActions
  }
}
