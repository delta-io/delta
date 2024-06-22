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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf

/**
 * Post-hoc resolution rules [[PreprocessTableMerge]] and [[PreprocessTableUpdate]] may introduce
 * new unresolved UpCast expressions that won't be resolved by [[ResolveUpCast]] that ran in the
 * previous resolution phase. This rule ensures these UpCast expressions get resolved in the
 * Post-hoc resolution phase.
 *
 * Note: we can't inject [[ResolveUpCast]] directly because we need an initialized analyzer instance
 * for that which is not available at the time Delta rules are injected. [[PostHocResolveUpCast]] is
 * delaying the access to the analyzer until after it's initialized.
 */
case class PostHocResolveUpCast(spark: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!plan.resolved) PostHocUpCastResolver.execute(plan) else plan

  /**
   * A rule executor that runs [[ResolveUpCast]] until all UpCast expressions have been resolved.
   */
  object PostHocUpCastResolver extends RuleExecutor[LogicalPlan] {
    final override protected def batches: Seq[Batch] = Seq(
      Batch(
        "Post-hoc UpCast Resolution",
        FixedPoint(
          conf.analyzerMaxIterations,
          errorOnExceed = true,
          maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key),
        spark.sessionState.analyzer.ResolveUpCast)
    )
  }
}
