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

package org.apache.spark.sql.delta

import java.util.{Map => JMap, Optional}

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.CatalogTableUtils

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf

/**
 * Centralized decision logic for Delta connector selection (sparkV2 vs sparkV1).
 *
 * <p>This class encapsulates all configuration checking for
 * <code>spark.databricks.delta.v2.enableMode</code> so that the rest of the codebase doesn't need
 * to directly inspect configuration values.
 *
 * <p>The mode is read once at construction. Create a new instance to read updated settings.
 *
 * <p>Configuration modes:
 * <ul>
 *   <li>NONE (default): sparkV1 connector for all operations</li>
 *   <li>AUTO: sparkV2 connector only for Unity Catalog managed tables</li>
 *   <li>STRICT: sparkV2 connector for all tables (testing mode)</li>
 * </ul>
 */
class DeltaV2Mode private (mode: String) {
  private final val NONE = "NONE"
  private final val STRICT = "STRICT"
  private final val AUTO = "AUTO"

  /** Captures the mode from the supplied SQL configuration. */
  def this(sqlConf: SQLConf) = this(sqlConf.getConf(DeltaSQLConf.V2_ENABLE_MODE))

  /** Captures the mode from the supplied session configuration. */
  def this(runtimeConfig: RuntimeConfig) = this(runtimeConfig.get(DeltaSQLConf.V2_ENABLE_MODE))

  /**
   * Determines if streaming reads should use the sparkV2 connector.
   *
   * @param catalogTable Optional catalog table metadata
   * @return true if sparkV2 streaming reads should be used
   */
  def isStreamingReadsEnabled(catalogTable: Optional[CatalogTable]): Boolean = mode match {
    case STRICT =>
      // Always use sparkV2 connector for all catalog tables
      true
    case AUTO =>
      // Only use sparkV2 connector for Unity Catalog managed tables
      catalogTable.isPresent && CatalogTableUtils.isUnityCatalogManagedTable(catalogTable.get)
    case _ =>
      // NONE or unknown: use sparkV1 streaming
      false
  }

  /**
   * Determines if catalog should return sparkV2 (DeltaV2Table) or sparkV1 (DeltaTableV2) tables.
   *
   * @return true if catalog should return sparkV2 tables
   */
  def shouldCatalogReturnV2Tables(): Boolean = mode match {
    case STRICT =>
      // STRICT mode: always return sparkV2 tables
      true
    case _ =>
      // NONE (default) or AUTO: return sparkV1 tables
      // Note: AUTO mode uses sparkV2 connector only for streaming via ApplyV2Streaming rule,
      // not at catalog level
      false
  }

  /**
   * Determines if catalog-driven Auto-CDF (CHANGES) reads should route to the sparkV2 connector.
   *
   * @return true if CHANGES reads should use the sparkV2 connector
   */
  def shouldRouteChangelogToV2(): Boolean = mode match {
    case STRICT | AUTO => true
    // NONE or unknown: the V2 Auto-CDF path is not available.
    case _ => false
  }

  /** Whether pre-resolution may apply Delta's V2 schema-evolution shims. */
  def allowsV2SchemaEvolutionShims(): Boolean = NONE != mode

  /**
   * Determines if the provided schema should be trusted without validation for streaming reads.
   * This is used to bypass DeltaLog schema loading for Unity Catalog tables where the catalog
   * already provides the correct schema.
   *
   * <p>If we don't bypass, we will load schema from DeltaLog and validate against the provided
   * schema. For UC-managed tables this extra DeltaLog access can be unnecessary and may fail when
   * the client doesn't have direct storage access to the managed location, even though the UC
   * schema is authoritative. For UC-managed tables, the DeltaLog schema should always match the
   * catalog schema, so re-validating provides no additional correctness guarantees.
   *
   * <p>This checks the parameters map for UC markers to determine if the table is UC-managed.
   *
   * @param parameters DataSource parameters map containing table storage properties
   * @return true if provided schema should be used without validation
   */
  def shouldBypassSchemaValidationForStreaming(parameters: JMap[String, String]): Boolean = {
    mode match {
      case STRICT | AUTO =>
        // In sparkV2 modes, trust the schema for Unity Catalog managed tables
        CatalogTableUtils.isUnityCatalogManagedTableFromProperties(parameters)
      case _ =>
        // NONE or unknown: always validate schema via DeltaLog
        false
    }
  }

  /** Gets the captured mode string (for logging/debugging). */
  def getMode: String = mode
}

object DeltaV2Mode {
  /** Captures the mode from the supplied session's SQLConf, not its RuntimeConfig. */
  def apply(sparkSession: SparkSession): DeltaV2Mode = apply(sparkSession.sessionState.conf)

  /** Captures the mode from the supplied SQL configuration. */
  def apply(sqlConf: SQLConf): DeltaV2Mode = new DeltaV2Mode(sqlConf)
}
