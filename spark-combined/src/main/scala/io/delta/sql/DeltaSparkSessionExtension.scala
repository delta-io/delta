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

package io.delta.sql

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Delta Spark Session Extension that can register both V1 and V2 implementations.
 * This class sits in delta-spark (combined) module and can access:
 * - V1: org.apache.spark.sql.delta.* (full version with DeltaLog)
 * - V2: io.delta.kernel.spark.*
 */
class DeltaSparkSessionExtension extends AbstractSparkSessionExtension {

  override def preDeltaAnalysisRule(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(
      session => new MaybeFallbackV1Connector(session)
    )
  }

  /**
   * NoOpRule for binary compatibility with Delta 3.3.0
   * This class must remain here to satisfy MiMa checks
   */
  class NoOpRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }
}
