/*
 * Copyright (2025) The Delta Lake Project Authors.
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
 * An extension for Spark SQL to activate Delta SQL parser to support Delta SQL grammar
 * and enable hybrid V1/V2 connector behavior via analyzer rules.
 *
 * Scala example to create a `SparkSession` with the Delta SQL parser and hybrid catalog:
 * {{{
 *    import org.apache.spark.sql.SparkSession
 *
 *    val spark = SparkSession
 *       .builder()
 *       .appName("...")
 *       .master("...")
 *       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *       .config("spark.sql.catalog.spark_catalog", "io.delta.sql.DeltaHybridCatalog")
 *       .getOrCreate()
 * }}}
 *
 * @since 0.4.0
 */
class DeltaSparkSessionExtension extends AbstractDeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // First apply the base extensions from AbstractDeltaSparkSessionExtension
    super.apply(extensions)
    // Register the analyzer rule for kernel-based streaming
    // This rule wraps HybridDeltaTable with context hint for streaming queries
    extensions.injectResolutionRule { session =>
      new UseKernelForStreamingRule(session)
    }
  }

  /**
   * NoOpRule for binary compatibility with Delta 3.3.0
   * This class must remain here to satisfy MiMa checks
   */
  class NoOpRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }
}
