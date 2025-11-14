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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.kernel.spark.table.SparkTable

/**
 * Analyzer rule that replaces V1 Delta tables with V2 Kernel-based tables for streaming reads only.
 *
 * This rule enables Delta to use:
 * - V2 (Kernel-based) implementation for streaming reads (sources with MicroBatchStream)
 * - V1 (DeltaLog-based) implementation for streaming writes, batch reads, and all writes
 *
 * The rule works by:
 * 1. Pattern matching on StreamingRelationV2 nodes (which only exist for streaming sources)
 * 2. Extracting the ResolvedTable from within StreamingRelationV2
 * 3. Replacing DeltaTableV2 with SparkTable (V2) only for those specific tables
 *
 * This precise matching ensures:
 * - Streaming writes (sinks) continue to use V1
 * - Batch operations continue to use V1
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
          table @ ResolvedTable(catalog, identifier, deltaTable: DeltaTableV2, attrs),
          extraOptions,
          output,
          v1Relation) =>

        try {
          logInfo(s"Replacing DeltaTableV2 with Kernel-based SparkTable for streaming source: $identifier")

          // Create V2 table for streaming read
          val v2Table = new SparkTable(
            Identifier.of(identifier.namespace(), identifier.name()),
            deltaTable.path.toString,
            deltaTable.options.asJava
          )

          val newResolvedTable = ResolvedTable(catalog, identifier, v2Table, attrs)

          // Return updated StreamingRelationV2 with V2 table
          StreamingRelationV2(source, sourceName, newResolvedTable, extraOptions, output, v1Relation)

        } catch {
          case e: Exception =>
            // If V2 table creation fails, log warning and fall back to V1
            logWarning(s"Failed to create Kernel V2 table for streaming source $identifier, " +
              s"falling back to V1: ${e.getMessage}", e)
            streamingRel
        }

      // Don't transform anything else - this preserves:
      // - Streaming writes (no StreamingRelationV2 wrapper)
      // - Batch reads (no StreamingRelationV2 wrapper)
      // - Batch writes (no StreamingRelationV2 wrapper)
    }
  }
}

/**
 * Configuration for Delta Kernel streaming support.
 */
object DeltaKernelStreamingConfig {

  /**
   * SQL configuration key to enable/disable Kernel-based streaming.
   * When enabled, streaming read queries will use the Kernel V2 implementation.
   * When disabled, all queries use the traditional V1 implementation.
   *
   * Default: false (disabled) for gradual rollout
   */
  val DELTA_KERNEL_STREAMING_ENABLED_KEY = "spark.databricks.delta.kernel.streaming.enabled"
}

