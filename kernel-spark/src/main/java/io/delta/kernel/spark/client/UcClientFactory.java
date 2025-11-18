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
package io.delta.kernel.spark.client;

import static io.delta.kernel.spark.utils.SparkConfigUtils.*;
import static java.util.Objects.requireNonNull;

import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Factory for creating Unity Catalog clients from Spark session configurations.
 *
 * <p>This factory constructs {@link UCClient} instances for querying Unity Catalog metadata and
 * features, needed for CCv2 (Catalog Commit Coordinator v2) detection and operations on
 * catalog-managed Delta tables.
 *
 * <p>The factory extracts Unity Catalog connection parameters (URI, authentication token) from
 * Spark's catalog configuration namespace and creates {@link UCTokenBasedRestClient} instances.
 *
 * <p><b>Configuration Requirements:</b>
 *
 * <p>The factory requires the following Spark configurations for the target catalog:
 *
 * <ul>
 *   <li>{@code spark.sql.catalog.<catalog_name>.uri} - Unity Catalog service URI
 *   <li>{@code spark.sql.catalog.<catalog_name>.token} - Authentication token for UC service
 * </ul>
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * SparkSession spark = SparkSession.active();
 * String catalogName = "my_catalog";
 *
 * try (UCClient client = UcClientFactory.buildForCatalog(spark, catalogName)) {
 *   String metastoreId = client.getMetastoreId();
 *   // Query commit metadata, table features, etc.
 * }
 * }</pre>
 *
 * <p><b>Design Notes:</b>
 *
 * <ul>
 *   <li>Reuses {@link UCTokenBasedRestClient} from storage module - no code duplication
 *   <li>Stateless factory pattern - no internal state or caching
 *   <li>Caller responsible for client lifecycle management (closing resources)
 *   <li>Decoupled from SparkTable - testable independently with mock SparkSession
 * </ul>
 *
 * @see UCClient
 * @see UCTokenBasedRestClient
 */
public final class UcClientFactory {

  private static final String URI_CONFIG_KEY = "uri";
  private static final String TOKEN_CONFIG_KEY = "token";

  private UcClientFactory() {}

  /**
   * Builds a Unity Catalog client for the specified catalog.
   *
   * <p>Extracts the Unity Catalog URI and authentication token from Spark session configuration and
   * constructs a {@link UCTokenBasedRestClient}. This method follows the same pattern as {@code
   * UCCommitCoordinatorBuilder.buildForCatalog} in the delta-spark module.
   *
   * <p>Required Spark configurations:
   *
   * <ul>
   *   <li>{@code spark.sql.catalog.<catalogName>.uri} - Must be a valid URI
   *   <li>{@code spark.sql.catalog.<catalogName>.token} - Must be non-empty
   * </ul>
   *
   * <p><b>Error Handling:</b>
   *
   * <p>This method throws {@link IllegalArgumentException} if:
   *
   * <ul>
   *   <li>The catalog is not configured in Spark session
   *   <li>Required configuration (uri or token) is missing
   *   <li>Configuration values are empty or contain only whitespace
   * </ul>
   *
   * <p><b>Resource Management:</b>
   *
   * <p>The returned {@link UCClient} implements {@link AutoCloseable}. Callers are responsible for
   * closing the client when done to release HTTP connections and other resources. Use
   * try-with-resources for automatic cleanup.
   *
   * @param spark the Spark session containing catalog configuration
   * @param catalogName name of the Unity Catalog to create client for
   * @return configured UCClient instance ready for use
   * @throws NullPointerException if spark or catalogName is null
   * @throws IllegalArgumentException if required configuration is missing or invalid
   */
  public static UCClient buildForCatalog(SparkSession spark, String catalogName) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogName, "catalogName is null");

    // Extract all catalog-specific configuration from Spark session
    Map<String, String> catalogConfig = extractCatalogConfig(spark, catalogName);

    // Validate that catalog exists and has configuration
    if (catalogConfig.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Catalog '%s' not found in Spark session configurations. "
                  + "Please ensure the catalog is configured with "
                  + "'spark.sql.catalog.%s.uri' and 'spark.sql.catalog.%s.token'.",
              catalogName, catalogName, catalogName));
    }

    // Extract required URI and token - will throw IllegalArgumentException if missing
    String uri = getRequiredConfig(catalogConfig, URI_CONFIG_KEY);
    String token = getRequiredConfig(catalogConfig, TOKEN_CONFIG_KEY);

    // Delegate to UCTokenBasedRestClient - reuse existing implementation
    return new UCTokenBasedRestClient(uri, token);
  }
}
