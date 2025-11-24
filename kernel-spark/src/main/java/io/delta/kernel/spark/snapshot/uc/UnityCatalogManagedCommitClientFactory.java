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
package io.delta.kernel.spark.snapshot.uc;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.spark.snapshot.ManagedCommitClient;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;

/** Factory for UC-backed {@link ManagedCommitClient} instances. */
public final class UnityCatalogManagedCommitClientFactory {

  private UnityCatalogManagedCommitClientFactory() {}

  public static Optional<ManagedCommitClient> create(CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return Optional.empty();
    }

    String tableId = extractUCTableId(catalogTable);
    String tablePath = extractTablePath(catalogTable);
    UCClient client = createUCClient(catalogTable, spark);
    return Optional.of(new UnityCatalogManagedCommitClient(tableId, tablePath, client));
  }

  private static String extractUCTableId(CatalogTable catalogTable) {
    Map<String, String> storageProperties =
        scala.collection.JavaConverters.mapAsJavaMap(catalogTable.storage().properties());

    String ucTableId =
        storageProperties.get(
            io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
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

  private static UCClient createUCClient(CatalogTable catalogTable, SparkSession spark) {
    scala.Option<String> catalogOption = catalogTable.identifier().catalog();
    String catalogName =
        catalogOption.isDefined()
            ? catalogOption.get()
            : spark.sessionState().catalogManager().currentCatalog().name();

    scala.collection.immutable.List<scala.Tuple3<String, String, String>> scalaConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigs(spark);

    Optional<scala.Tuple3<String, String, String>> configTuple =
        scala.jdk.javaapi.CollectionConverters.asJava(scalaConfigs).stream()
            .filter(tuple -> tuple._1().equals(catalogName))
            .findFirst();

    if (!configTuple.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot create UC client: Unity Catalog configuration not found for catalog '"
              + catalogName
              + "'.");
    }

    scala.Tuple3<String, String, String> config = configTuple.get();
    String uri = config._2();
    String token = config._3();

    return UCTokenBasedRestClientFactory$.MODULE$.createUCClient(uri, token);
  }
}
