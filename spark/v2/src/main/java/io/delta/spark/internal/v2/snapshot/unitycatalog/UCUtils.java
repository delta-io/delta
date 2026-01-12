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
package io.delta.spark.internal.v2.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import io.delta.spark.internal.v2.utils.CatalogTableUtils;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;

/**
 * Utility class for extracting Unity Catalog table information from Spark catalog metadata.
 *
 * <p>This class isolates Spark dependencies, allowing {@link UCManagedTableSnapshotManager} to be
 * created without Spark if table info is provided directly via {@link UCTableInfo}.
 */
public final class UCUtils {

  // Utility class - no instances
  private UCUtils() {}

  /**
   * Extracts Unity Catalog table information from Spark catalog table metadata.
   *
   * @param catalogTable Spark catalog table metadata
   * @param spark SparkSession for resolving Unity Catalog configurations
   * @return table info if table is UC-managed, empty otherwise
   * @throws IllegalArgumentException if table is UC-managed but configuration is invalid
   */
  public static Optional<UCTableInfo> extractTableInfo(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return Optional.empty();
    }

    String tableId = extractUCTableId(catalogTable);
    String tablePath = extractTablePath(catalogTable);

    // Get catalog name - require explicit catalog in identifier
    scala.Option<String> catalogOption = catalogTable.identifier().catalog();
    if (catalogOption.isEmpty()) {
      throw new IllegalArgumentException(
          "Unable to determine Unity Catalog for table "
              + catalogTable.identifier()
              + ": catalog name is missing. Use a fully-qualified table name with an explicit "
              + "catalog (e.g., catalog.schema.table).");
    }
    String catalogName = catalogOption.get();

    // Get UC endpoint and token from Spark configs
    scala.collection.immutable.Map<String, UCCatalogConfig> ucConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);

    scala.Option<UCCatalogConfig> configOpt = ucConfigs.get(catalogName);

    if (configOpt.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create UC client for table "
              + catalogTable.identifier()
              + ": Unity Catalog configuration not found for catalog '"
              + catalogName
              + "'.");
    }

    UCCatalogConfig config = configOpt.get();
    String ucUri = config.uri();

    return Optional.of(new UCTableInfo(tableId, tablePath, ucUri, asJava(config.authConfig())));
  }

  private static String extractUCTableId(CatalogTable catalogTable) {
    Map<String, String> storageProperties =
        scala.jdk.javaapi.CollectionConverters.asJava(catalogTable.storage().properties());

    // TODO: UC constants should be consolidated in a shared location (future PR)
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
