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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, StreamingRelationV2}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Analyzer rule that manages HybridDeltaTable behavior based on query type.
 *
 * This rule enables Delta to use:
 * - V2 (Kernel-based) SparkTable for streaming reads (sources with MicroBatchStream)
 * - V1 (DeltaLog-based) DeltaTableV2 for streaming writes, batch reads, and all writes
 *
 * The rule has two transformation cases:
 * 1. StreamingRelationV2 with HybridDeltaTable → Replace with SparkTable (V2)
 * 2. DataSourceV2Relation with HybridDeltaTable → Replace with DeltaTableV2 (V1)
 *
 * Case 2 is critical: It ensures that batch operations get plain DeltaTableV2,
 * which allows DeltaAnalysis's FallbackToV1DeltaRelation to match and convert to LogicalRelation.
 *
 * IMPORTANT: This rule must be registered BEFORE DeltaAnalysis in the analyzer pipeline
 * to ensure batch unwrapping happens before FallbackToV1DeltaRelation runs.
 */
class UseKernelForStreamingRule(spark: SparkSession) extends Rule[LogicalPlan] {

  // Check if Kernel streaming is enabled via configuration
  private def isKernelStreamingEnabled: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_KERNEL_STREAMING_ENABLED)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isKernelStreamingEnabled) {
      return plan
    }

    // Transform the plan with two cases:
    // Case 1: Streaming reads → Replace with SparkTable (V2)
    // Case 2: Batch/write operations → Replace with DeltaTableV2 (V1)
    plan.transformUp {
      // Case 1: Streaming read source → Replace HybridDeltaTable with SparkTable (V2)
      // This pattern matches ONLY streaming sources (readStream), not sinks (writeStream)
      case streamingRel @ StreamingRelationV2(
          source,
          sourceName,
          table @ ResolvedTable(catalog, identifier, hybridTable: HybridDeltaTable, attrs),
          extraOptions,
          output,
          v1Relation) =>

        try {
          logInfo(s"Replacing HybridDeltaTable with SparkTable for streaming source: $identifier")

          // Get the SparkTable (V2) from hybrid
          val sparkTable = hybridTable.getSparkTable()
          val newResolvedTable = ResolvedTable(catalog, identifier, sparkTable, attrs)

          // Return updated StreamingRelationV2 with SparkTable
          StreamingRelationV2(
            source, sourceName, newResolvedTable, extraOptions, output, v1Relation)

        } catch {
          case e: Exception =>
            // If replacement fails, log warning and fall back to default (V1)
            logWarning(
              s"Failed to replace HybridDeltaTable with SparkTable for streaming source " +
              s"$identifier, falling back to V1: ${e.getMessage}", e)
            streamingRel
        }

      // Case 2: Batch/write operations - Replace HybridDeltaTable with DeltaTableV2 (V1)
      // This matches DataSourceV2Relation which is what batch reads create
      case dsv2 @ DataSourceV2Relation(
          hybridTable: HybridDeltaTable, output, catalog, identifier, options) =>
        try {
          logInfo(
            s"Replacing HybridDeltaTable with DeltaTableV2 for batch/write operation: " +
            s"$identifier")

          // Get the DeltaTableV2 (V1) from hybrid
          val v1Table = hybridTable.getUnderlyingDeltaTableV2()

          // Return DataSourceV2Relation with plain DeltaTableV2
          // This allows DeltaAnalysis's FallbackToV1DeltaRelation to match and convert
          // to LogicalRelation (V1)
          DataSourceV2Relation(v1Table, output, catalog, identifier, options)

        } catch {
          case e: Exception =>
            // If replacement fails, log warning and keep hybrid (will default to V1)
            logWarning(
              s"Failed to replace HybridDeltaTable with DeltaTableV2 for $identifier, " +
              s"keeping hybrid: ${e.getMessage}", e)
            dsv2
        }

      // Don't transform anything else - all other node types pass through unchanged
    }
  }
}
