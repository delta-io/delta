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
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Filter, LeafNode, LogicalPlan, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation


trait DeltaSparkPlanUtils {
  protected def planContainsOnlyDeltaScans(source: LogicalPlan): Boolean = {
    !source.exists {
      case l: LogicalRelation =>
        l match {
          case DeltaTable(_) => false
          case _ => true
        }
      case _: LeafNode => true // Any other LeafNode is a non Delta scan.
      case _ => false
    }
  }

  /**
   * Returns `true` if `plan` has a safe level of determinism. This is a conservative approximation
   * of `plan` being a truly deterministic query.
   */
  protected def planIsDeterministic(plan: LogicalPlan): Boolean = plan match {
    // This is very restrictive, allowing only deterministic filters and projections directly
    // on top of a Delta Table.
    case Distinct(child) => planIsDeterministic(child)
    case Project(projectList, child) if projectList.forall(_.deterministic) =>
      planIsDeterministic(child)
    case Filter(cond, child) if cond.deterministic => planIsDeterministic(child)
    case Union(children, _, _) => children.forall(planIsDeterministic)
    case SubqueryAlias(_, child) => planIsDeterministic(child)
    case DeltaTable(_) => true
    case _ => false
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
