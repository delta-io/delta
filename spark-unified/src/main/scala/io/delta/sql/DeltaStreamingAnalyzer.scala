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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import io.delta.kernel.spark.table.SparkTable

/**
 * Analyzer rule that converts V1 StreamingRelation to V2 StreamingRelationV2 with Kernel-based
 * tables for catalog-managed Delta tables only.
 *
 * This rule enables Delta to use:
 * - V2 (Kernel-based) implementation for streaming reads of catalog-managed tables
 * - V1 (DeltaLog-based) implementation for path-based tables, streaming writes, and batch ops
 *
 * The rule works by:
 * 1. Pattern matching on StreamingRelation nodes (V1 streaming sources)
 * 2. Checking if the source is a catalog-managed Delta table
 * 3. Converting to StreamingRelationV2 with SparkTable (V2)
 *
 * This approach ensures:
 * - Only catalog-managed tables get V2 streaming
 * - Path-based tables continue using V1
 * - Streaming writes (sinks) continue to use V1
 * - Batch operations continue to use V1
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

    // Transform StreamingRelation (V1) to StreamingRelationV2 (V2) for catalog-managed tables
    plan.transformUp {
      case streamingRel @ StreamingRelation(dataSource, sourceName, output)
          if isCatalogManagedDeltaTable(dataSource) =>

        val catalogTable = dataSource.catalogTable.get
        val tableIdent = catalogTable.identifier

        logInfo(
          s"Converting StreamingRelation to StreamingRelationV2 with SparkTable for " +
          s"catalog-managed table: ${tableIdent.unquotedString}")

        // Get catalog and identifier
        val catalog = spark.sessionState.catalogManager.currentCatalog
        val identifier = Identifier.of(
          tableIdent.database.toArray,
          tableIdent.table
        )

        // Create V2 table for streaming read
        val v2Table = new SparkTable(
          identifier,
          catalogTable.location.toString,
          dataSource.options.asJava
        )

        // Create ResolvedTable
        val resolvedTable = ResolvedTable(
          catalog,
          identifier,
          v2Table,
          v2Table.columns.toAttributes
        )

        // Return StreamingRelationV2 with V1 fallback
        // Note: v1Relation allows Spark to fall back to V1 if V2 execution fails
        StreamingRelationV2(
          source = Some(dataSource),
          sourceName = sourceName,
          table = resolvedTable,
          extraOptions = dataSource.options,
          output = output,
          catalog = Some(catalog),
          identifier = Some(identifier),
          v1Relation = Some(streamingRel)
        )

      // Don't transform anything else - this preserves:
      // - Path-based Delta tables (no catalogTable)
      // - Streaming writes (no StreamingRelation wrapper)
      // - Batch reads (no StreamingRelation wrapper)
      // - Batch writes (no StreamingRelation wrapper)
    }
  }

  /**
   * Check if the DataSource is a catalog-managed Delta table.
   * We only convert catalog-managed tables to V2, not path-based tables.
   */
  private def isCatalogManagedDeltaTable(dataSource: DataSource): Boolean = {
    dataSource.catalogTable.exists { catalogTable =>
      // Check if it's a Delta table by looking at the provider
      catalogTable.provider.exists(_.equalsIgnoreCase("delta"))
    }
  }
}
