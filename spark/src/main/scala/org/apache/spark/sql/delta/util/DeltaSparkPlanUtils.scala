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

import org.apache.spark.sql.delta.{DeltaTable, DeltaTableReadPredicate}

import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, InSubquery, LateralSubquery, ScalarSubquery, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Filter, LeafNode, LogicalPlan, OneRowRelation, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation


trait DeltaSparkPlanUtils {
  import DeltaSparkPlanUtils._

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
   *
   */
  protected def planIsDeterministic(
      plan: LogicalPlan,
      checkDeterministicOptions: CheckDeterministicOptions): Boolean =
    findFirstNonDeterministicNode(plan, checkDeterministicOptions).isEmpty

  type PlanOrExpression = Either[LogicalPlan, Expression]

  /**
   * Returns a part of the `plan` that does not have a safe level of determinism.
   * This is a conservative approximation of `plan` being a truly deterministic query.
   */
  protected def findFirstNonDeterministicNode(
      plan: LogicalPlan,
      checkDeterministicOptions: CheckDeterministicOptions): Option[PlanOrExpression] = {
    plan match {
      // This is very restrictive, allowing only deterministic filters and projections directly
      // on top of a Delta Table.
      case Distinct(child) => findFirstNonDeterministicNode(child, checkDeterministicOptions)
      case Project(projectList, child) =>
        findFirstNonDeterministicChildNode(projectList, checkDeterministicOptions) orElse {
            findFirstNonDeterministicNode(child, checkDeterministicOptions)
        }
      case Filter(cond, child) =>
        findFirstNonDeterministicNode(cond, checkDeterministicOptions) orElse {
          findFirstNonDeterministicNode(child, checkDeterministicOptions)
        }
      case Union(children, _, _) => collectFirst[LogicalPlan, PlanOrExpression](
        children,
        c => findFirstNonDeterministicNode(c, checkDeterministicOptions))
      case SubqueryAlias(_, child) =>
        findFirstNonDeterministicNode(child, checkDeterministicOptions)
      case DeltaTable(_) => None
      case OneRowRelation() => None
      case node => Some(Left(node))
    }
  }

  protected def planContainsUdf(plan: LogicalPlan): Boolean = {
    plan.collectWithSubqueries {
      case node if node.expressions.exists(_.exists(_.isInstanceOf[UserDefinedExpression])) => ()
    }.nonEmpty
  }

  protected def findFirstNonDeterministicChildNode(
      children: Seq[Expression],
      checkDeterministicOptions: CheckDeterministicOptions): Option[PlanOrExpression] =
    collectFirst[Expression, PlanOrExpression](
      children,
      c => findFirstNonDeterministicNode(c, checkDeterministicOptions))

  protected def findFirstNonDeterministicNode(
      child: Expression,
      checkDeterministicOptions: CheckDeterministicOptions): Option[PlanOrExpression] = {
    child match {
      case SubqueryExpression(plan) =>
        findFirstNonDeltaScan(plan).map(Left(_))
          .orElse(findFirstNonDeterministicNode(plan, checkDeterministicOptions))
      case _: UserDefinedExpression if !checkDeterministicOptions.allowDeterministicUdf =>
        Some(Right(child))
      case p =>
        collectFirst[Expression, PlanOrExpression](
          p.children,
          c => findFirstNonDeterministicNode(c, checkDeterministicOptions)) orElse {
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

  /** Returns whether the read predicates of a transaction contain any deterministic UDFs. */
  def containsDeterministicUDF(
      predicates: Seq[DeltaTableReadPredicate], partitionedOnly: Boolean): Boolean = {
    if (partitionedOnly) {
      predicates.exists {
        _.partitionPredicates.exists(containsDeterministicUDF)
      }
    } else {
      predicates.exists { p =>
        p.dataPredicates.exists(containsDeterministicUDF) ||
          p.partitionPredicates.exists(containsDeterministicUDF)
      }
    }
  }

  /** Returns whether an expression contains any deterministic UDFs. */
  def containsDeterministicUDF(expr: Expression): Boolean = expr.exists {
    case udf: UserDefinedExpression => udf.deterministic
    case _ => false
  }
}


object DeltaSparkPlanUtils {
  /**
   * Options for deciding whether plans contain non-deterministic nodes and expressions.
   *
   * @param allowDeterministicUdf If true, allow UDFs that are marked by users as deterministic.
   *                              If false, always treat them as non-deterministic to be more
   *                              defensive against user bugs.
   */
  case class CheckDeterministicOptions(
    allowDeterministicUdf: Boolean
  )
}
