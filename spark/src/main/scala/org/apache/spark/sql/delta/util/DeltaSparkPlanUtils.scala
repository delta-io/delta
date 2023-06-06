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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.DeltaTable

import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, InSubquery, LateralSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Filter, LeafNode, LogicalPlan, OneRowRelation, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation


trait DeltaSparkPlanUtils {
  protected def planContainsOnlyDeltaScans(source: LogicalPlan): Boolean =
    findFirstNonDeltaScan(source).isEmpty

  protected def findFirstNonDeltaScan(source: LogicalPlan): Option[LogicalPlan] = {
    source match {
      case l: LogicalRelation =>
        l match {
          case DeltaTable(_) => None
          case _ => Some(l)
        }
      case OneRowRelation() => None
      case leaf: LeafNode => Some(leaf) // Any other LeafNode is a non Delta scan.
      case node => collectFirst(node.children, findFirstNonDeltaScan)
    }
  }

  /**
   * Returns `true` if `plan` has a safe level of determinism. This is a conservative
   * approximation of `plan` being a truly deterministic query.
   */
  protected def planIsDeterministic(plan: LogicalPlan): Boolean =
    findFirstNonDeterministicNode(plan).isEmpty

  type PlanOrExpression = Either[LogicalPlan, Expression]

  /**
   * Returns a part of the `plan` that does not have a safe level of determinism.
   * This is a conservative approximation of `plan` being a truly deterministic query.
   */
  protected def findFirstNonDeterministicNode(plan: LogicalPlan): Option[PlanOrExpression] = {
    plan match {
      // This is very restrictive, allowing only deterministic filters and projections directly
      // on top of a Delta Table.
      case Distinct(child) => findFirstNonDeterministicNode(child)
      case Project(projectList, child) =>
        findFirstNonDeterministicChildNode(projectList) orElse {
            findFirstNonDeterministicNode(child)
        }
      case Filter(cond, child) =>
        findFirstNonDeterministicNode(cond) orElse {
          findFirstNonDeterministicNode(child)
        }
      case Union(children, _, _) => collectFirst[LogicalPlan, PlanOrExpression](
        children,
        findFirstNonDeterministicNode)
      case SubqueryAlias(_, child) => findFirstNonDeterministicNode(child)
      case DeltaTable(_) => None
      case OneRowRelation() => None
      case node => Some(Left(node))
    }
  }

  protected def findFirstNonDeterministicChildNode(
      children: Seq[Expression]): Option[PlanOrExpression] =
    collectFirst[Expression, PlanOrExpression](
      children,
      findFirstNonDeterministicNode)

  protected def findFirstNonDeterministicNode(child: Expression): Option[PlanOrExpression] = {
    child match {
      case SubqueryExpression(plan) =>
        findFirstNonDeltaScan(plan).map(Left(_)).orElse(findFirstNonDeterministicNode(plan))
      case p =>
        collectFirst[Expression, PlanOrExpression](
          p.children,
          findFirstNonDeterministicNode) orElse {
          if (p.deterministic) None else Some(Right(p))
        }
    }
  }

  protected def collectFirst[In, Out](
      input: Iterable[In],
      recurse: In => Option[Out]): Option[Out] = {
    input.foldLeft(Option.empty[Out]) { case (acc, value) =>
      acc.orElse(recurse(value))
    }
  }

  /** Extractor object for the subquery plan of expressions that contain subqueries. */
  object SubqueryExpression {
    def unapply(expr: Expression): Option[LogicalPlan] = expr match {
      case subquery: ScalarSubquery => Some(subquery.plan)
      case exists: Exists => Some(exists.plan)
      case subquery: InSubquery => Some(subquery.query.plan)
      case subquery: LateralSubquery => Some(subquery.plan)
      case _ => None
    }
  }
}
