package io.delta.kernel.spark.unity;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Factory for constructing Unity Catalog clients from Spark session catalog configuration.
 *
 * <p>The logic mirrors the config resolution performed by {@code
 * org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder}, ensuring the connector
 * honours the same semantics across V1 and Kernel-backed paths.
 */
public final class UnityCatalogClientFactory {

  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String URI_SUFFIX = "uri";
  private static final String TOKEN_SUFFIX = "token";
  private static final String WAREHOUSE_SUFFIX = "warehouse";
  private static final String UNITY_CATALOG_CONNECTOR_CLASS =
      "io.unitycatalog.spark.UCSingleCatalog";

  private UnityCatalogClientFactory() {}

  private static volatile BiFunction<String, String, UCClient> clientBuilder =
      UnityCatalogClientFactory::defaultClientBuilder;

  /**
   * Creates a Unity Catalog client for the provided catalog table if it is Unity Catalog managed.
   *
   * @param spark active Spark session
   * @param identifier table identifier supplied by Spark DSv2
   * @param catalogTable catalog metadata for the table
   * @return optional Unity Catalog client details; empty when the table is not UC managed
   */
  public static Optional<UnityCatalogClient> create(
      SparkSession spark, Identifier identifier, CatalogTable catalogTable) {
    requireNonNull(spark, "spark session is null");
    requireNonNull(identifier, "identifier is null");
    requireNonNull(catalogTable, "catalogTable is null");

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return Optional.empty();
    }

    Map<String, String> sparkConf = ScalaUtils.toJavaMap(spark.conf().getAll());

    List<CatalogConfig> unityCatalogs = collectUnityCatalogConfigs(sparkConf);
    if (unityCatalogs.isEmpty()) {
      throw new IllegalStateException(
          "Unity Catalog table detected but no Unity Catalog connectors are configured.");
    }

    Optional<String> requestedCatalogName = extractCatalogName(identifier);
    CatalogConfig selectedCatalog =
        selectCatalogConfig(requestedCatalogName, unityCatalogs, identifier.name());

    UCClient ucClient = clientBuilder.apply(selectedCatalog.uri, selectedCatalog.token);
    UnityCatalogClient clientHandle =
        new UnityCatalogClient(selectedCatalog.name, ucClient, selectedCatalog.warehouse);
    return Optional.of(clientHandle);
  }

  private static CatalogConfig selectCatalogConfig(
      Optional<String> requestedCatalogName, List<CatalogConfig> unityCatalogs, String tableName) {
    if (requestedCatalogName.isPresent()) {
      Optional<CatalogConfig> match =
          unityCatalogs.stream()
              .filter(config -> config.name.equalsIgnoreCase(requestedCatalogName.get()))
              .findFirst();
      if (match.isPresent()) {
        return match.get();
      }
      if (unityCatalogs.size() == 1) {
        return unityCatalogs.get(0);
      }
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Unable to locate Unity Catalog connector '%s' for table '%s'.",
              requestedCatalogName.get(),
              tableName));
    }

    if (unityCatalogs.size() == 1) {
      return unityCatalogs.get(0);
    }

    throw new IllegalStateException(
        String.format(
            Locale.ROOT,
            "Multiple Unity Catalog connectors configured (%s) but table '%s' does not carry a "
                + "catalog-qualified identifier.",
            listCatalogNames(unityCatalogs),
            tableName));
  }

  private static List<CatalogConfig> collectUnityCatalogConfigs(Map<String, String> sparkConf) {
    Map<String, CatalogProperties> catalogProperties = new HashMap<>();

    for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(SPARK_SQL_CATALOG_PREFIX)) {
        continue;
      }
      String remainder = key.substring(SPARK_SQL_CATALOG_PREFIX.length());
      int dotIndex = remainder.indexOf('.');
      String catalogName;
      String propertyKey = null;
      if (dotIndex == -1) {
        catalogName = remainder;
      } else {
        catalogName = remainder.substring(0, dotIndex);
        propertyKey = remainder.substring(dotIndex + 1);
      }

      CatalogProperties properties =
          catalogProperties.computeIfAbsent(catalogName, CatalogProperties::new);
      if (propertyKey == null) {
        properties.connectorClass = entry.getValue();
      } else if (URI_SUFFIX.equals(propertyKey)) {
        properties.uri = entry.getValue();
      } else if (TOKEN_SUFFIX.equals(propertyKey)) {
        properties.token = entry.getValue();
      } else if (WAREHOUSE_SUFFIX.equals(propertyKey)) {
        properties.warehouse = Optional.ofNullable(entry.getValue()).filter(v -> !v.isEmpty());
      }
    }

    List<CatalogConfig> unityCatalogs = new ArrayList<>();
    for (CatalogProperties properties : catalogProperties.values()) {
      if (!UNITY_CATALOG_CONNECTOR_CLASS.equals(properties.connectorClass)) {
        continue;
      }

      String uri = requireTrimmed(properties.uri, properties.name, URI_SUFFIX);
      try {
        new URI(uri);
      } catch (URISyntaxException e) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Invalid Unity Catalog URI '%s' configured for catalog '%s'.",
                uri,
                properties.name),
            e);
      }

      String token = requireTrimmed(properties.token, properties.name, TOKEN_SUFFIX);
      unityCatalogs.add(new CatalogConfig(properties.name, uri, token, properties.warehouse));
    }

    return unityCatalogs;
  }

  private static String listCatalogNames(List<CatalogConfig> configs) {
    List<String> names = new ArrayList<>(configs.size());
    for (CatalogConfig config : configs) {
      names.add(config.name);
    }
    Collections.sort(names, String.CASE_INSENSITIVE_ORDER);
    return String.join(",", names);
  }

  private static String requireTrimmed(String value, String catalogName, String propertySuffix) {
    if (value == null) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Missing Unity Catalog configuration '%s%s%s'.",
              SPARK_SQL_CATALOG_PREFIX,
              catalogName,
              propertySuffix.isEmpty() ? "" : "." + propertySuffix));
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Unity Catalog configuration '%s%s.%s' cannot be empty.",
              SPARK_SQL_CATALOG_PREFIX,
              catalogName,
              propertySuffix));
    }
    return trimmed;
  }

  private static Optional<String> extractCatalogName(Identifier identifier) {
    String[] namespace = identifier.namespace();
    if (namespace != null && namespace.length > 0) {
      return Optional.of(namespace[0]);
    }
    return Optional.empty();
  }

  /** Unity Catalog client handle containing additional metadata required by the connector. */
  public static final class UnityCatalogClient implements AutoCloseable {
    private final String catalogName;
    private final UCClient ucClient;
    private final Optional<String> warehouse;

    UnityCatalogClient(String catalogName, UCClient ucClient, Optional<String> warehouse) {
      this.catalogName = requireNonNull(catalogName, "catalogName is null");
      this.ucClient = requireNonNull(ucClient, "ucClient is null");
      this.warehouse = Objects.requireNonNullElseGet(warehouse, Optional::empty);
    }

    public String getCatalogName() {
      return catalogName;
    }

    public UCClient getUcClient() {
      return ucClient;
    }

    public Optional<String> getWarehouse() {
      return warehouse;
    }

    @Override
    public void close() throws Exception {
      ucClient.close();
    }
  }

  private static final class CatalogProperties {
    private final String name;
    private String connectorClass;
    private String uri;
    private String token;
    private Optional<String> warehouse = Optional.empty();

    private CatalogProperties(String name) {
      this.name = name;
    }
  }

  private static final class CatalogConfig {
    private final String name;
    private final String uri;
    private final String token;
    private final Optional<String> warehouse;

    private CatalogConfig(String name, String uri, String token, Optional<String> warehouse) {
      this.name = name;
      this.uri = uri;
      this.token = token;
      this.warehouse = warehouse;
    }
  }

  /** Visible for testing: override the UC client builder. */
  public static void setClientBuilderForTesting(BiFunction<String, String, UCClient> builder) {
    clientBuilder = requireNonNull(builder, "builder is null");
  }

  /** Visible for testing: reset the UC client builder to the default implementation. */
  public static void resetClientBuilderForTesting() {
    clientBuilder = UnityCatalogClientFactory::defaultClientBuilder;
  }

  private static UCClient defaultClientBuilder(String uri, String token) {
    try {
      return new UCTokenBasedRestClient(uri, token);
    } catch (NoClassDefFoundError e) {
      throw new IllegalStateException(
          "Unity Catalog client requires Apache HttpClient dependencies on the classpath.", e);
    }
  }
}
