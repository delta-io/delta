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
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Post-hoc resolution rules [[PreprocessTableMerge]] and [[PreprocessTableUpdate]] may introduce
 * new unresolved UpCast expressions that won't be resolved by [[ResolveUpCast]] that ran in the
 * previous resolution phase. This rule ensures these UpCast expressions get resolved in the
 * Post-hoc resolution phase.
 */
case class PostHocResolveUpCast(spark: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    spark.sessionState.analyzer.ResolveUpCast.apply(plan)
}
