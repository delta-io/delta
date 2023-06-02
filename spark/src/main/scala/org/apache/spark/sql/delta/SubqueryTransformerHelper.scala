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

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery, SupportsSubquery}

/**
 * Trait to allow processing a special transformation of [[SubqueryExpression]]
 * instances in a query plan.
 */
trait SubqueryTransformerHelper {

  /**
   * Transform all nodes matched by the rule in the query plan rooted at given `plan`.
   * It traverses the tree starting from the leaves, whenever a [[SubqueryExpression]]
   * expression is encountered, given [[rule]] is applied to the subquery plan `plan`
   * in [[SubqueryExpression]] starting from the `plan` root until leaves.
   *
   * This is slightly different behavior compared to [[QueryPlan.transformUpWithSubqueries]]
   * or [[QueryPlan.transformDownWithSubqueries]]
   *
   * It requires that the given plan already gone through [[OptimizeSubqueries]] and the
   * root node denoting a subquery is removed and optimized appropriately.
   */
  def transformWithSubqueries(plan: LogicalPlan)
      (rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    require(!isSubqueryRoot(plan))
    transformSubqueries(plan, rule) transform (rule)
  }

  /** Is the give plan a subquery root. */
  def isSubqueryRoot(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[Subquery] || plan.isInstanceOf[SupportsSubquery]
  }

  private def transformSubqueries(
      plan: LogicalPlan,
      rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    import org.apache.spark.sql.delta.implicits._

    plan transformAllExpressionsUp {
      case subquery: SubqueryExpression =>
        subquery.withNewPlan(transformWithSubqueries(subquery.plan)(rule))
    }
  }
}
