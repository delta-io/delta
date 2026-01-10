/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.util.CatalogTableUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * Centralized decision logic for Delta connector selection (sparkV2 vs sparkV1).
 *
 * This class encapsulates all configuration checking for `spark.databricks.delta.v2.enableMode`
 * so that the rest of the codebase doesn't need to directly inspect configuration values.
 *
 * Configuration modes:
 * - NONE (default): sparkV1 connector for all operations
 * - AUTO: sparkV2 connector only for Unity Catalog managed tables
 * - STRICT: sparkV2 connector for all tables (testing mode)
 *
 * @param sqlConf Spark SQL configuration to read the v2.enableMode setting
 */
class DeltaV2Mode(sqlConf: SQLConf) {

  private def mode: String = {
    sqlConf.getConf(DeltaSQLConf.V2_ENABLE_MODE)
  }

  /**
   * Determines if streaming reads should use the sparkV2 connector.
   *
   * @param catalogTable Optional catalog table metadata
   * @return true if sparkV2 streaming reads should be used
   */
  def isStreamingReadsEnabled(catalogTable: Option[CatalogTable]): Boolean = {
    mode match {
      case "STRICT" =>
        // Always use sparkV2 connector for all catalog tables
        true
      case "AUTO" =>
        // Only use sparkV2 connector for Unity Catalog managed tables
        catalogTable.exists(CatalogTableUtils.isUnityCatalogManagedTable)
      case _ =>
        // NONE or unknown: use sparkV1 streaming
        false
    }
  }

  /**
   * Determines if catalog should return sparkV2 (SparkTable) or sparkV1 (DeltaTableV2) tables.
   *
   * @return true if catalog should return sparkV2 tables
   */
  def shouldCatalogReturnV2Tables: Boolean = {
    mode match {
      case "STRICT" =>
        // STRICT mode: always return sparkV2 tables
        true
      case _ =>
        // NONE (default) or AUTO: return sparkV1 tables
        // Note: AUTO mode uses sparkV2 connector only for streaming via ApplyV2Streaming rule,
        // not at catalog level
        false
    }
  }

  /**
   * Determines if the provided schema should be trusted without validation for streaming reads.
   * This is used to bypass DeltaLog schema loading for Unity Catalog tables where the catalog
   * already provides the correct schema.
   *
   * This checks the parameters map for UC markers to determine if the table is UC-managed.
   *
   * @param parameters DataSource parameters map containing table storage properties
   * @return true if provided schema should be used without validation
   */
  def shouldBypassSchemaValidationForStreaming(
      parameters: Map[String, String]): Boolean = {
    mode match {
      case "STRICT" | "AUTO" =>
        // In sparkV2 modes, trust the schema for Unity Catalog managed tables
        CatalogTableUtils.isUnityCatalogManagedTableFromProperties(parameters)
      case _ =>
        // NONE or unknown: always validate schema via DeltaLog
        false
    }
  }

  /**
   * Gets the current mode string (for logging/debugging).
   */
  def getMode: String = mode
}

object DeltaV2Mode {
  /**
   * Creates a DeltaV2Mode instance from SQLConf.
   */
  def apply(sqlConf: SQLConf): DeltaV2Mode = new DeltaV2Mode(sqlConf)
}
