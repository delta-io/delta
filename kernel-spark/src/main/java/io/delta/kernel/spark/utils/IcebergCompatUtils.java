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
package io.delta.kernel.spark.utils;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import java.util.Optional;

/**
 * Utility methods for Iceberg compatibility in Kernel based connector. This class provides Iceberg
 * compatibility utility methods that align with Spark V1's IcebergCompat APIs.
 */
public class IcebergCompatUtils {

  /** Configuration key for Iceberg Compat V1 (matching Spark V1's DeltaConfigs). */
  private static final String ICEBERG_COMPAT_V1_ENABLED_KEY = "delta.enableIcebergCompatV1";

  private IcebergCompatUtils() {}

  /**
   * Returns whether any version of Iceberg compatibility is enabled. This method follows the same
   * logic as Spark V1's IcebergCompat.isAnyEnabled.
   *
   * <p>Note: This only checks V1 and V2, matching Spark V1's support. Kernel supports V2 and V3,
   * but we only check V1 and V2 for API compatibility with Spark V1.
   *
   * @param metadata The table metadata to check.
   * @return true if any version of Iceberg compatibility (V1 or V2) is enabled; false otherwise.
   */
  public static boolean isAnyEnabled(Metadata metadata) {
    return getEnabledVersion(metadata).isPresent();
  }

  /**
   * Returns whether an Iceberg compatibility version greater than or equal to the required version
   * is enabled. This method follows the same logic as Spark V1's IcebergCompat.isGeqEnabled: it
   * finds the enabled version (if any) and checks if it is >= requiredVersion.
   *
   * <p>Note: This only checks V1 and V2, matching Spark V1's support. Kernel supports V2 and V3,
   * but we only check V1 and V2 for API compatibility with Spark V1.
   *
   * @param metadata The table metadata to check.
   * @param requiredVersion The minimum required Iceberg compatibility version.
   * @return true if an Iceberg compatibility version >= requiredVersion is enabled; false
   *     otherwise.
   */
  public static boolean isVersionGeqEnabled(Metadata metadata, int requiredVersion) {
    return getEnabledVersion(metadata).map(version -> version >= requiredVersion).orElse(false);
  }

  /**
   * Helper method to get the enabled Iceberg compatibility version.
   *
   * @param metadata The table metadata to check.
   * @return Optional containing the enabled version number (1 or 2), or empty if no version is
   *     enabled.
   */
  private static Optional<Integer> getEnabledVersion(Metadata metadata) {
    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata)) {
      return Optional.of(2);
    } else if (isIcebergCompatV1Enabled(metadata)) {
      return Optional.of(1);
    }
    return Optional.empty();
  }

  /**
   * Manually check if Iceberg Compat V1 is enabled by reading the configuration directly.
   *
   * @param metadata The table metadata to check.
   * @return true if V1 is enabled; false otherwise.
   */
  private static boolean isIcebergCompatV1Enabled(Metadata metadata) {
    String value = metadata.getConfiguration().get(ICEBERG_COMPAT_V1_ENABLED_KEY);
    return value != null && value.equalsIgnoreCase("true");
  }
}
