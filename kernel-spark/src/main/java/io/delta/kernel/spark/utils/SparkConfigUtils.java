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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Utility helpers for extracting Unity Catalog client configuration from Spark session
 * configurations.
 *
 * <p>Unity Catalog clients require connection parameters (URI, authentication tokens) that are
 * stored in Spark's catalog configuration namespace. This helper centralizes the logic for
 * extracting and validating those configurations so the UcClientFactory can construct properly
 * configured clients.
 *
 * <p>Configuration keys follow the pattern: {@code spark.sql.catalog.<catalog_name>.<key>}
 *
 * <p>Example configurations:
 *
 * <ul>
 *   <li>{@code spark.sql.catalog.my_catalog.uri} - Base URI for Unity Catalog service
 *   <li>{@code spark.sql.catalog.my_catalog.token} - Authentication token
 *   <li>{@code spark.sql.catalog.my_catalog.warehouse} - Default warehouse/catalog context
 * </ul>
 */
public final class SparkConfigUtils {

  private static final String CATALOG_CONFIG_PREFIX = "spark.sql.catalog.";
  private static final String CONFIG_SEPARATOR = ".";

  private SparkConfigUtils() {}

  /**
   * Extracts all Unity Catalog configuration properties for the specified catalog from the Spark
   * session.
   *
   * <p>Searches for all configuration keys matching the pattern {@code
   * spark.sql.catalog.<catalogName>.*} and returns them in a map with the prefix stripped.
   *
   * <p>For example, if the catalog name is "my_catalog" and the config contains: {@code
   * spark.sql.catalog.my_catalog.uri=https://uc-server.com
   * spark.sql.catalog.my_catalog.token=abc123}
   *
   * <p>This method will return a map: {@code {"uri": "https://uc-server.com", "token": "abc123"}}
   *
   * @param spark the Spark session to extract configuration from
   * @param catalogName name of the catalog to extract configuration for
   * @return map of configuration keys (without catalog prefix) to their values
   * @throws NullPointerException if spark or catalogName is null
   */
  public static Map<String, String> extractCatalogConfig(SparkSession spark, String catalogName) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogName, "catalogName is null");

    String prefix = CATALOG_CONFIG_PREFIX + catalogName + CONFIG_SEPARATOR;
    Map<String, String> catalogConfig = new HashMap<>();

    // Iterate through all Spark configurations and filter by catalog prefix
    scala.collection.immutable.Map<String, String> allConfigs = spark.conf().getAll();
    scala.collection.Iterator<scala.Tuple2<String, String>> iterator = allConfigs.iterator();

    while (iterator.hasNext()) {
      scala.Tuple2<String, String> entry = iterator.next();
      String key = entry._1();
      String value = entry._2();

      if (key.startsWith(prefix)) {
        // Strip the prefix and store the remaining key
        String configKey = key.substring(prefix.length());
        catalogConfig.put(configKey, value);
      }
    }

    return catalogConfig;
  }

  /**
   * Retrieves a required configuration value from the config map.
   *
   * <p>This method enforces that required configurations are present and non-empty.
   *
   * @param config map of configuration properties
   * @param key configuration key to retrieve
   * @return the configuration value
   * @throws NullPointerException if config or key is null
   * @throws IllegalArgumentException if the key is missing or has an empty value
   */
  public static String getRequiredConfig(Map<String, String> config, String key) {
    requireNonNull(config, "config is null");
    requireNonNull(key, "key is null");

    String value = config.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Required configuration key '%s' is missing or empty", key));
    }
    return value;
  }

  /**
   * Retrieves an optional configuration value from the config map, returning a default if not
   * present.
   *
   * @param config map of configuration properties
   * @param key configuration key to retrieve
   * @param defaultValue default value to return if key is not present or has empty value
   * @return the configuration value or default if not present/empty
   * @throws NullPointerException if config or key is null (defaultValue may be null)
   */
  public static String getOptionalConfig(
      Map<String, String> config, String key, String defaultValue) {
    requireNonNull(config, "config is null");
    requireNonNull(key, "key is null");

    String value = config.get(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    return value;
  }
}
