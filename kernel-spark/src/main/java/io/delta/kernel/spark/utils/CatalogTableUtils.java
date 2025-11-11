package io.delta.kernel.spark.utils;

import static java.util.Objects.requireNonNull;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * Unity Catalog persists Unity-specific metadata onto Spark {@link Table} instances when they are
 * resolved through UCSingleCatalog. This helper centralises the logic for interpreting those
 * properties so the Kernel connector can decide when to use catalog-owned (CCv2) behaviour.
 *
 * <p> These constants mirror the property keys by the UC <> Spark connector
 * <ul>
 *  <li>delta.unityCatalog.* (e.g., tableId) flags a table as catalog-managed
 *  <li>delta.feature.catalogOwned[-preview] signals that CCv2 (catalog-owned commit coordination)
 *    is enabled. Both map to the value "supported" when active
 * </ul>
 *
 * <p> See {@code connectors/spark/.../UCSingleCatalog.scala} for the producer side of these props
 */
public final class CatalogTableUtils {
  static final String UNITY_CATALOG_PROPERTY_PREFIX = "delta.unityCatalog.";
  static final String UNITY_CATALOG_TABLE_ID_PROP = UNITY_CATALOG_PROPERTY_PREFIX + "tableId";
  static final String FEATURE_CATALOG_OWNED = "delta.feature.catalogOwned";
  static final String FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview";
  private static final String SUPPORTED = "supported";

  private CatalogTableUtils() {}

  public static boolean isCCv2Table(Table table) {
    requireNonNull(table, "table is null");
    Map<String, String> tableProperties = table.properties();
    if (!isCatalogManagedTable(tableProperties)) {
      return false;
    }

    return isCatalogOwnedFeatureSupported(tableProperties, FEATURE_CATALOG_OWNED)
        || isCatalogOwnedFeatureSupported(tableProperties, FEATURE_CATALOG_OWNED_PREVIEW);
  }

  public static boolean isCatalogManagedTable(Table table) {
    requireNonNull(table, "table is null");
    return isCatalogManagedTable(table.properties());
  }

  static boolean isCatalogManagedTable(Map<String, String> tableProperties) {
    if (tableProperties == null) {
      return false;
    }

    String tableId = tableProperties.get(UNITY_CATALOG_TABLE_ID_PROP);
    if (tableId != null && !tableId.trim().isEmpty()) {
      return true;
    }

    return tableProperties.keySet().stream()
        .filter(Objects::nonNull)
        .anyMatch(key -> key.startsWith(UNITY_CATALOG_PROPERTY_PREFIX));
  }

  private static boolean isCatalogOwnedFeatureSupported(
      Map<String, String> tableProperties, String featureKey) {
    if (tableProperties == null) {
      return false;
    }
    String value = tableProperties.get(featureKey);
    if (value == null) {
      return false;
    }
    return SUPPORTED.equalsIgnoreCase(value.trim());
  }
}
