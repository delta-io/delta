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

/**
 * Analyzer rule that wraps HybridDeltaTable with context hint for streaming reads only.
 *
 * This rule enables Delta to use:
 * - V2 (Kernel-based) implementation for streaming reads (sources with MicroBatchStream)
 * - V1 (DeltaLog-based) implementation for streaming writes, batch reads, and all writes
 *
 * The rule works by:
 * 1. Pattern matching on StreamingRelationV2 nodes (which only exist for streaming sources)
 * 2. Extracting the ResolvedTable from within StreamingRelationV2
 * 3. Wrapping HybridDeltaTable with HybridDeltaTableWithContext(useV2=true)
 *
 * This precise matching ensures:
 * - Streaming writes (sinks) continue to use V1 (no wrapper, defaults to V1)
 * - Batch operations continue to use V1 (no wrapper, defaults to V1)
 * - Only streaming reads benefit from V2's MicroBatchStream implementation
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

    // Transform only StreamingRelationV2 nodes (streaming sources)
    // This pattern match is precise: it only matches tables used as streaming sources,
    // not streaming sinks, batch reads, or batch writes
    plan.transformUp {
      case streamingRel @ StreamingRelationV2(
          source,
          sourceName,
          table @ ResolvedTable(catalog, identifier, hybridTable: HybridDeltaTable, attrs),
          extraOptions,
          output,
          v1Relation) =>

        try {
          logInfo(s"Wrapping HybridDeltaTable with V2 context for streaming source: $identifier")

          // Wrap the hybrid table with context indicating V2 should be used
          val withContext = new HybridDeltaTableWithContext(hybridTable, useV2 = true)
          val newResolvedTable = ResolvedTable(catalog, identifier, withContext, attrs)

          // Return updated StreamingRelationV2 with wrapped table
          StreamingRelationV2(source, sourceName, newResolvedTable, extraOptions, output, v1Relation)

        } catch {
          case e: Exception =>
            // If wrapping fails, log warning and fall back to default (V1)
            logWarning(s"Failed to wrap HybridDeltaTable with V2 context for streaming source $identifier, " +
              s"falling back to V1: ${e.getMessage}", e)
            streamingRel
        }

      // Don't transform anything else - this preserves:
      // - Streaming writes (no StreamingRelationV2 wrapper) → defaults to V1
      // - Batch reads (no StreamingRelationV2 wrapper) → defaults to V1
      // - Batch writes (no StreamingRelationV2 wrapper) → defaults to V1
    }
  }
}
