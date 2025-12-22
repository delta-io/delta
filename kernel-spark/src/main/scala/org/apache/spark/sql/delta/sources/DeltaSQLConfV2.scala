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

package org.apache.spark.sql.delta.sources

/**
 * SQL configurations for Delta V2 connector (Kernel-based connector).
 */
object DeltaSQLConfV2 extends DeltaSQLConfUtils {

  /**
   * Controls which connector implementation to use for Delta table operations.
   *
   * Valid values:
   * - NONE: V2 connector is disabled, always use V1 connector (DeltaTableV2) - default
   * - STRICT: V2 connector is strictly enforced, always use V2 connector (Kernel SparkTable).
   *           Intended for testing V2 connector capabilities
   *
   * V1 vs V2 Connectors:
   * - V1 Connector (DeltaTableV2): Legacy Delta connector with full read/write support,
   *   uses DeltaLog for metadata management
   * - V2 Connector (SparkTable): New Kernel-based connector with read-only support,
   *   uses Kernel's Table API for metadata management
   */
  val V2_ENABLE_MODE =
    buildConf("v2.enableMode")
      .doc(
        "Controls the Delta V2 connector enable mode. " +
        "Valid values: NONE (disabled, default), STRICT (should ONLY be enabled for testing).")
      .stringConf
      .checkValues(Set("NONE", "STRICT"))
      .createWithDefault("NONE")
}

