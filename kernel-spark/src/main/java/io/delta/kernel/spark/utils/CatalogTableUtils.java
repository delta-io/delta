package io.delta.kernel.spark.utils;

import static java.util.Objects.requireNonNull;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.connector.catalog.Table;

public class CatalogTableUtils {

  // UC persists markers and feature flags into Spark Table properties during resolution.
  // - Keys under delta.unityCatalog.* (e.g. tableId) identify the table as Unity Catalog managed,
  //   which tells the connector to use UC-aware code paths instead of path-only logic.
  // - delta.feature.catalogOwned[-preview] advertise CCv2 support; when their value is
  //   "supported" the table should opt into catalog-owned commit coordination.
  static final String UNITY_CATALOG_PROPERTY_PREFIX = "delta.unityCatalog.";
  static final String UNITY_CATALOG_TABLE_ID_PROP = UNITY_CATALOG_PROPERTY_PREFIX + "tableId";
  static final String FEATURE_CATALOG_OWNED = "delta.feature.catalogOwned";
  static final String FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview";
  private static final String SUPPORTED = "supported";

  private CatalogTableUtils() {}

  /**
   * A table is a CCv2 table if it is catalog managed and has catalogOwned feature enabled.
   * Catalog managed tables are identified by the presence of delta.unityCatalog.* properties.
   * CatalogOwned feature is identified by the presence of delta.feature.catalogOwned[-preview] properties.
   *
   * @param table the table
   * @return true if the table is a CCv2 table, false otherwise
   */
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

  /**
   * A table is catalog managed if it has a non-empty tableId under delta.unityCatalog.* properties.
   * Refer to connectors/spark/src/main/scala/io/unitycatalog/spark/UCSingleCatalog.scala for more details.
   *
   * @param tableProperties the table properties
   * @return true if the table is catalog managed, false otherwise
   */
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

  /**
   * A table has catalogOwned feature enabled if table's
   * properties["delta.feature.catalogOwned"] is "supported"
   * We currently use both preview and stable version of the feature but may only support stable version in the future.
   *
   * @param tableProperties the table properties
   * @param featureKey the feature key
   * @return true if the feature is supported, false otherwise
   */
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
