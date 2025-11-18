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
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import scala.Tuple3;
import scala.collection.JavaConverters;

/**
 * Utility class for extracting Unity Catalog configuration from Spark session.
 *
 * <p>This is a thin wrapper around {@link
 * org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder#getCatalogConfigs}
 * that provides a Java-friendly API for accessing catalog configurations.
 *
 * <p>Delegates to UCCommitCoordinatorBuilder for all validation logic (URI parsing, connector
 * class filtering, etc.) to avoid code duplication.
 */
public final class SparkConfigUtils {

  private SparkConfigUtils() {}

  /**
   * Extracts Unity Catalog configuration for the specified catalog from Spark session.
   *
   * <p>Delegates to {@link
   * org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder#getCatalogConfigs}
   * which filters for Unity Catalog connectors and validates URIs.
   *
   * @param spark the Spark session containing catalog configuration
   * @param catalogName the catalog name to extract configuration for
   * @return map of configuration keys and values (uri, token, etc.)
   * @throws NullPointerException if spark or catalogName is null
   */
  public static Map<String, String> getCatalogConfig(SparkSession spark, String catalogName) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogName, "catalogName is null");

    // Delegate to UCCommitCoordinatorBuilder for catalog config extraction
    scala.collection.immutable.List<Tuple3<String, String, String>> catalogConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigs(spark);

    // Convert Scala List to Java and find matching catalog
    List<Tuple3<String, String, String>> javaCatalogConfigs =
        JavaConverters.seqAsJavaList(catalogConfigs);

    for (Tuple3<String, String, String> config : javaCatalogConfigs) {
      if (config._1().equals(catalogName)) {
        Map<String, String> result = new HashMap<>();
        result.put("uri", config._2());
        result.put("token", config._3());
        return result;
      }
    }

    // Catalog not found
    return new HashMap<>();
  }
}
