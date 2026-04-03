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
package io.delta.spark.internal.v2.catalog;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.util.CatalogTableUtils;

/** Shared property handling for DSv2 CREATE TABLE planning and publication. */
public final class CreateTablePropertySupport {
  private static final String DEFAULT_TABLE_PROPERTY_PREFIX =
      "spark.databricks.delta.properties.defaults.";
  private static final String CHECKPOINT_POLICY_KEY = "delta.checkpointPolicy";
  private static final String CHECKPOINT_POLICY_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "checkpointPolicy";
  private static final String ENABLE_DELETION_VECTORS_KEY = "delta.enableDeletionVectors";
  private static final String ENABLE_DELETION_VECTORS_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "enableDeletionVectors";
  private static final String ENABLE_ROW_TRACKING_KEY = "delta.enableRowTracking";
  private static final String ENABLE_ROW_TRACKING_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "enableRowTracking";

  private CreateTablePropertySupport() {}

  /** Filters CREATE TABLE input down to the properties that should be committed to Delta. */
  public static Map<String, String> filterCreateTableProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.remove(TableCatalog.PROP_LOCATION);
    filtered.remove(TableCatalog.PROP_PROVIDER);
    filtered.remove(TableCatalog.PROP_COMMENT);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_EXTERNAL);
    filtered.remove("path");
    filtered.remove("option.path");
    filtered.remove("test.simulateUC");
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    normalizeUcTableIdProperty(filtered);
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    return filtered;
  }

  /** Removes filesystem credential keys while preserving catalog registration metadata. */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    normalizeUcTableIdProperty(filtered);
    return filtered;
  }

  public static void addCatalogManagedQoLDefaults(
      SparkSession spark, Map<String, String> properties) {
    if (!CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties)) {
      return;
    }

    putIfUnsetByTableOrSession(
        spark,
        properties,
        ENABLE_DELETION_VECTORS_KEY,
        ENABLE_DELETION_VECTORS_DEFAULT_KEY,
        "true");
    putIfUnsetByTableOrSession(
        spark, properties, CHECKPOINT_POLICY_KEY, CHECKPOINT_POLICY_DEFAULT_KEY, "v2");
    putIfUnsetByTableOrSession(
        spark, properties, ENABLE_ROW_TRACKING_KEY, ENABLE_ROW_TRACKING_DEFAULT_KEY, "true");
  }

  public static void normalizeUcTableIdProperty(Map<String, String> properties) {
    String oldTableId = properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldTableId != null) {
      properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldTableId);
    }
  }

  public static boolean isHadoopOption(String key) {
    String effectiveKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
    return DeltaTableUtils.validDeltaTableHadoopPrefixes().exists(effectiveKey::startsWith);
  }

  private static void putIfUnsetByTableOrSession(
      SparkSession spark,
      Map<String, String> properties,
      String tablePropertyKey,
      String sessionDefaultKey,
      String value) {
    if (!properties.containsKey(tablePropertyKey)
        && spark.conf().get(sessionDefaultKey, null) == null) {
      properties.put(tablePropertyKey, value);
    }
  }
}
