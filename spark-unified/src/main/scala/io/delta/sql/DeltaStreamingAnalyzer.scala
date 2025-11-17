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

import scala.collection.JavaConverters._

import io.delta.kernel.spark.table.SparkTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Analyzer rule that converts V1 StreamingRelation to V2 StreamingRelationV2 with V2-based
 * tables for catalog-managed Delta tables only.
 *
 * This rule enables Delta to use:
 * - V2 (DataSource V2) implementation for streaming reads of catalog-managed tables
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
class UseV2ForStreamingRule(spark: SparkSession) extends Rule[LogicalPlan] {

  // Check if V2 streaming is enabled via configuration
  private def isV2StreamingEnabled: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_V2_STREAMING_ENABLED)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isV2StreamingEnabled) {
      return plan
    }

    // Transform StreamingRelation (V1) to StreamingRelationV2 (V2) for catalog-managed tables
    plan.resolveOperatorsDown {
      case streamingRel @ StreamingRelation(dataSource, sourceName, output)
          if isCatalogManagedDeltaTable(dataSource) &&
             isCatalogOwnedTable(spark, dataSource.catalogTable.get) =>

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

        // Get output attributes from the table schema
        val outputAttributes = DataTypeUtils.toAttributes(v2Table.schema())

        // Return StreamingRelationV2 with V1 fallback
        // Note: v1Relation allows Spark to fall back to V1 if V2 execution fails
        StreamingRelationV2(
          None, // source: no TableProvider, just the table
          sourceName,
          v2Table, // table: directly pass SparkTable
          new CaseInsensitiveStringMap(dataSource.options.asJava),
          outputAttributes, // output attributes
          Some(catalog),
          Some(identifier),
          Some(streamingRel) // v1Relation fallback
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

  /**
   * Check if the table is catalog-owned (CCV2).
   */
  private def isCatalogOwnedTable(
      spark: SparkSession,
      catalogTable: CatalogTable): Boolean = {
    // TODO: Implement actual check for catalog-owned tables
    // Currently returns true to enable V2 streaming for all catalog-managed tables
    true
  }
}
