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

package org.apache.spark.sql.delta;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.sources.DeltaSQLConf$;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
import org.apache.spark.sql.internal.SQLConf;

/**
 * Centralized decision logic for Delta connector selection (sparkV2 vs sparkV1).
 *
 * <p>This class encapsulates all configuration checking for
 * {@code spark.databricks.delta.v2.enableMode} so that the rest of the codebase doesn't need to
 * directly inspect configuration values.
 *
 * <p>Configuration modes:
 * <ul>
 *   <li>NONE (default): sparkV1 connector for all operations</li>
 *   <li>AUTO: sparkV2 connector only for Unity Catalog managed tables</li>
 *   <li>STRICT: sparkV2 connector for all tables (testing mode)</li>
 * </ul>
 */
public class DeltaV2Mode {
  private static final String STRICT = "STRICT";
  private static final String AUTO = "AUTO";

  private final SQLConf sqlConf;

  public DeltaV2Mode(SQLConf sqlConf) {
    this.sqlConf = sqlConf;
  }

  private String mode() {
    return sqlConf.getConf(DeltaSQLConf$.MODULE$.V2_ENABLE_MODE());
  }

  /**
   * Determines if streaming reads should use the sparkV2 connector.
   *
   * @param catalogTable Optional catalog table metadata
   * @return true if sparkV2 streaming reads should be used
   */
  public boolean isStreamingReadsEnabled(Optional<CatalogTable> catalogTable) {
    switch (mode()) {
      case STRICT:
        // Always use sparkV2 connector for all catalog tables
        return true;
      case AUTO:
        // Only use sparkV2 connector for Unity Catalog managed tables
        return catalogTable.map(CatalogTableUtils::isUnityCatalogManagedTable).orElse(false);
      default:
        // NONE or unknown: use sparkV1 streaming
        return false;
    }
  }

  /**
   * Determines if catalog should return sparkV2 (SparkTable) or sparkV1 (DeltaTableV2) tables.
   *
   * @return true if catalog should return sparkV2 tables
   */
  public boolean shouldCatalogReturnV2Tables() {
    switch (mode()) {
      case STRICT:
        // STRICT mode: always return sparkV2 tables
        return true;
      default:
        // NONE (default) or AUTO: return sparkV1 tables
        // Note: AUTO mode uses sparkV2 connector only for streaming via ApplyV2Streaming rule,
        // not at catalog level
        return false;
    }
  }

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
  public boolean shouldBypassSchemaValidationForStreaming(Map<String, String> parameters) {
    switch (mode()) {
      case STRICT:
      case AUTO:
        // In sparkV2 modes, trust the schema for Unity Catalog managed tables
        return CatalogTableUtils.isUnityCatalogManagedTableFromProperties(parameters);
      default:
        // NONE or unknown: always validate schema via DeltaLog
        return false;
    }
  }

  /**
   * Determines if metadata-only CREATE TABLE should use the Kernel-based commit path.
   *
   * <p>Mode behavior:
   *
   * <ul>
   *   <li>STRICT: enabled for all tables
   *   <li>AUTO: enabled only for Unity Catalog managed tables
   *   <li>NONE (default): disabled
   * </ul>
   *
   * @param properties CREATE TABLE properties map
   * @return true if metadata-only CREATE should use Kernel commit path
   */
  public boolean shouldUseKernelMetadataOnlyCreate(Map<String, String> properties) {
    switch (mode()) {
      case STRICT:
        return true;
      case AUTO:
        return CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties);
      default:
        return false;
    }
  }

  /**
   * Gets the current mode string (for logging/debugging).
   */
  public String getMode() {
    return mode();
  }
}
