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

package org.apache.spark.sql.delta.constraints

import org.apache.spark.sql.catalyst.optimizer.{ReplaceExpressions, RewriteWithExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

trait DeltaInvariantCheckerOptimizerShims { self: RuleExecutor[LogicalPlan] =>
  val DELTA_INVARIANT_CHECKER_OPTIMIZER_BATCHES = Seq(
    Batch("Finish Analysis", Once, ReplaceExpressions),
    Batch("Rewrite With expression", Once, RewriteWithExpression)
  )
}
