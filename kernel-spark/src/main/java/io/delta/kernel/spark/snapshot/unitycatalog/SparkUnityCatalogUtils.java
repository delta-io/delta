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
package io.delta.kernel.spark.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;

/**
 * Utility class for extracting Unity Catalog connection information from Spark catalog metadata.
 *
 * <p>This class isolates Spark dependencies, allowing {@link UnityCatalogAdapter} to be created
 * without Spark if connection info is provided directly via {@link UnityCatalogConnectionInfo}.
 */
public final class SparkUnityCatalogUtils {

  // Utility class - no instances
  private SparkUnityCatalogUtils() {}

  /**
   * Extracts Unity Catalog connection information from Spark catalog table metadata.
   *
   * @param catalogTable Spark catalog table metadata
   * @param spark SparkSession for resolving Unity Catalog configurations
   * @return connection info if table is UC-managed, empty otherwise
   * @throws IllegalArgumentException if table is UC-managed but configuration is invalid
   */
  public static Optional<UnityCatalogConnectionInfo> extractConnectionInfo(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return Optional.empty();
    }

    String tableId = extractUCTableId(catalogTable);
    String tablePath = extractTablePath(catalogTable);

    // Get catalog name
    scala.Option<String> catalogOption = catalogTable.identifier().catalog();
    String catalogName =
        catalogOption.isDefined()
            ? catalogOption.get()
            : spark.sessionState().catalogManager().currentCatalog().name();

    // Get UC endpoint and token from Spark configs
    List<UCCatalogConfig> ucConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigsJava(spark);

    Optional<UCCatalogConfig> config =
        ucConfigs.stream().filter(c -> c.catalogName().equals(catalogName)).findFirst();

    if (!config.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot create UC client: Unity Catalog configuration not found for catalog '"
              + catalogName
              + "'.");
    }

    String endpoint = config.get().uri();
    String token = config.get().token();

    return Optional.of(new UnityCatalogConnectionInfo(tableId, tablePath, endpoint, token));
  }

  private static String extractUCTableId(CatalogTable catalogTable) {
    Map<String, String> storageProperties =
        scala.jdk.javaapi.CollectionConverters.asJava(catalogTable.storage().properties());

    String ucTableId = storageProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId == null || ucTableId.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot extract ucTableId from table " + catalogTable.identifier());
    }
    return ucTableId;
  }

  private static String extractTablePath(CatalogTable catalogTable) {
    if (catalogTable.location() == null) {
      throw new IllegalArgumentException(
          "Cannot extract table path: location is null for table " + catalogTable.identifier());
    }
    return catalogTable.location().toString();
  }
}
