package io.delta.kernel.spark.utils;

import static java.util.Objects.requireNonNull;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Utility helpers for inspecting Delta-related metadata persisted on Spark {@link CatalogTable}
 * instances by Unity Catalog.
 *
 * <p>Unity Catalog marks catalog-managed tables via feature flags stored in table storage
 * properties. This helper centralises the logic for interpreting those properties so the Kernel
 * connector can decide when to use catalog-owned (CCv2) behaviour.
 *
 * <ul>
 *   <li>{@link #isCatalogManaged(CatalogTable)} checks whether either {@code
 *       delta.feature.catalogManaged} or {@code delta.feature.catalogOwned-preview} is set to
 *       {@code supported}, signalling that a catalog manages the table.
 *   <li>{@link #isUnityCatalogManagedTable(CatalogTable)} additionally verifies the presence of the
 *       Unity Catalog table identifier ({@link UCCommitCoordinatorClient#UC_TABLE_ID_KEY}) to
 *       confirm that the table is backed by Unity Catalog.
 * </ul>
 */
public final class CatalogTableUtils {
  static final String FEATURE_CATALOG_MANAGED = "delta.feature.catalogManaged";
  static final String FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview";
  private static final String SUPPORTED = "supported";

  private CatalogTableUtils() {}

  // Checks whether *any* catalog manages this table via CCv2 semantics by checking
  // if the catalogManaged/catalogOwned-preview flags are 'supported'
  public static boolean isCatalogManaged(CatalogTable table) {
    requireNonNull(table, "table is null");
    Map<String, String> storageProperties = getStorageProperties(table);
    return isCatalogManagedFeatureEnabled(storageProperties, FEATURE_CATALOG_MANAGED)
        || isCatalogManagedFeatureEnabled(storageProperties, FEATURE_CATALOG_OWNED_PREVIEW);
  }

  // Checks if table is *Unity Catalog* managed - meaning it isCatalogManaged, and it contains
  // a UC identifier (UC_TABLE_ID_KEY)
  public static boolean isUnityCatalogManagedTable(CatalogTable table) {
    requireNonNull(table, "table is null");
    Map<String, String> storageProperties = getStorageProperties(table);
    boolean isUCBacked = storageProperties.containsKey(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    return isUCBacked && isCatalogManaged(table);
  }

  // We merge the storage and logical properties into a single map because we want to be able to
  // access both sets of properties in a single method.
  public static Map<String, String> getStorageProperties(CatalogTable table) {
    requireNonNull(table, "table is null");
    Map<String, String> merged = new HashMap<>();
    merged.putAll(ScalaUtils.toJavaMap(table.storage().properties()));
    merged.putAll(ScalaUtils.toJavaMap(table.properties()));
    return merged;
  }

  public static boolean isCatalogManagedFeatureEnabled(
      Map<String, String> tableProperties, String featureKey) {
    requireNonNull(tableProperties, "tableProperties is null");
    requireNonNull(featureKey, "featureKey is null");
    String featureValue = tableProperties.get(featureKey);
    if (featureValue == null) {
      return false;
    }
    return featureValue.equalsIgnoreCase(SUPPORTED);
  }
}
