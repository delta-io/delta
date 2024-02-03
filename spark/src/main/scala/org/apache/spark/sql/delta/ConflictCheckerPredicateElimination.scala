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

import org.apache.spark.sql.delta.util.DeltaSparkPlanUtils
import org.apache.spark.sql.delta.util.DeltaSparkPlanUtils.CheckDeterministicOptions

import org.apache.spark.sql.catalyst.expressions.{And, EmptyRow, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private[delta] trait ConflictCheckerPredicateElimination extends DeltaSparkPlanUtils {

  /**
   * This class represents the state of a expression tree transformation, whereby we try to
   * eliminate predicates that are non-deterministic in a way that widens the set of rows to
   * include any row that could be read by the original predicate.
   *
   * Example: `c1 = 5 AND c2 IN (SELECT c FROM <parquet table>)` would be widened to
   * `c1 = 5 AND True` eliminating the non-deterministic parquet table read by assuming it would
   * have matched all c2 values.
   *
   * `c1 = 5 OR NOT some_udf(c2)` would be widened to `c1 = 5 OR True`, eliminating the
   * non-deterministic `some_udf` by assuming `NOT some_udf(c2)` would have selected all rows.
   *
   * @param newPredicates
   *   The (potentially widened) list of predicates.
   * @param eliminatedPredicates
   *   The predicates that were eliminated as non-deterministic.
   */
  protected case class PredicateElimination(
      newPredicates: Seq[Expression],
      eliminatedPredicates: Seq[String])
  protected object PredicateElimination {
    final val EMPTY: PredicateElimination = PredicateElimination(Seq.empty, Seq.empty)

    def eliminate(p: Expression, eliminated: Option[String] = None): PredicateElimination =
      PredicateElimination(
        // Always eliminate with a `TrueLiteral`, implying that the eliminated expression would
        // have read the entire table.
        newPredicates = Seq(TrueLiteral),
        eliminatedPredicates = Seq(eliminated.getOrElse(p.prettyName)))

    def keep(p: Expression): PredicateElimination =
      PredicateElimination(newPredicates = Seq(p), eliminatedPredicates = Seq.empty)

    def recurse(
        p: Expression,
        recFun: Seq[Expression] => PredicateElimination): PredicateElimination = {
      val eliminatedChildren = recFun(p.children)
      if (eliminatedChildren.eliminatedPredicates.isEmpty) {
        // All children were ok, so keep the current expression.
        keep(p)
      } else {
        // Fold the new predicates after sub-expression widening.
        val newPredicate = p.withNewChildren(eliminatedChildren.newPredicates) match {
          case p if p.foldable => Literal.create(p.eval(EmptyRow), p.dataType)
          case Or(TrueLiteral, _) => TrueLiteral
          case Or(_, TrueLiteral) => TrueLiteral
          case And(left, TrueLiteral) => left
          case And(TrueLiteral, right) => right
          case p => p
        }
        PredicateElimination(
          newPredicates = Seq(newPredicate),
          eliminatedPredicates = eliminatedChildren.eliminatedPredicates)
      }
    }
  }

  /**
   * Replace non-deterministic expressions in a way that can only increase the number of selected
   * files when these predicates are used for file skipping.
   */
  protected def eliminateNonDeterministicPredicates(
      predicates: Seq[Expression],
      checkDeterministicOptions: CheckDeterministicOptions): PredicateElimination = {
    eliminateUnsupportedPredicates(predicates) {
      case p @ SubqueryExpression(plan) =>
        findFirstNonDeltaScan(plan) match {
          case Some(plan) => PredicateElimination.eliminate(p, eliminated = Some(plan.nodeName))
          case None =>
            findFirstNonDeterministicNode(plan, checkDeterministicOptions) match {
              case Some(node) =>
                PredicateElimination.eliminate(p, eliminated = Some(planOrExpressionName(node)))
              case None => PredicateElimination.keep(p)
            }
        }
      // And and Or can safely be recursed through. Replacing any non-deterministic sub-tree
      // with `True` will lead us to at most select more files than necessary later.
      case p: And => PredicateElimination.recurse(p,
        p => eliminateNonDeterministicPredicates(p, checkDeterministicOptions))
      case p: Or => PredicateElimination.recurse(p,
        p => eliminateNonDeterministicPredicates(p, checkDeterministicOptions))
      // All other expressions must either be completely deterministic,
      // or must be replaced entirely, since replacing only their non-deterministic children
      // may lead to files wrongly being deselected (e.g. `NOT True`).
      case p =>
        // We always look for non-deterministic child nodes, whether or not `p` is actually
        // deterministic. This gives us better feedback on what caused the non-determinism in
        // cases where `p` itself it deterministic but `p.deterministic = false` due to correctly
        // detected non-deterministic child nodes.
        findFirstNonDeterministicChildNode(p.children, checkDeterministicOptions) match {
          case Some(node) =>
            PredicateElimination.eliminate(p, eliminated = Some(planOrExpressionName(node)))
          case None => if (p.deterministic) {
            PredicateElimination.keep(p)
          } else {
            PredicateElimination.eliminate(p)
          }
        }
    }
  }

  private def eliminateUnsupportedPredicates(predicates: Seq[Expression])(
     eliminatePredicates: Expression => PredicateElimination): PredicateElimination = {
    predicates
      .map(eliminatePredicates)
      .foldLeft(PredicateElimination.EMPTY) { case (acc, predicates) =>
        acc.copy(
          newPredicates = acc.newPredicates ++ predicates.newPredicates,
          eliminatedPredicates = acc.eliminatedPredicates ++ predicates.eliminatedPredicates)
      }
  }

  private def planOrExpressionName(e: Either[LogicalPlan, Expression]): String = e match {
    case scala.util.Left(plan) => plan.nodeName
    case scala.util.Right(expression) => expression.prettyName
  }
}
