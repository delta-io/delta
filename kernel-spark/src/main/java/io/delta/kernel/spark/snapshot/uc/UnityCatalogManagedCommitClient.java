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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.snapshot.ManagedCommitClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;

/** UC-backed implementation of {@link ManagedCommitClient}. */
public final class UnityCatalogManagedCommitClient implements ManagedCommitClient {

  private final String tableId;
  private final String tablePath;
  private final UCClient ucClient;
  private final UCCatalogManagedClient ucManagedClient;

  public UnityCatalogManagedCommitClient(
      String tableId, String tablePath, UCClient ucClient) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.ucManagedClient = new UCCatalogManagedClient(ucClient);
  }

  public static UnityCatalogManagedCommitClient fromCatalog(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    String tableId = extractUCTableId(catalogTable);
    String tablePath = extractTablePath(catalogTable);
    UCClient client = createUCClient(catalogTable, spark);
    return new UnityCatalogManagedCommitClient(tableId, tablePath, client);
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  @Override
  public String getTablePath() {
    return tablePath;
  }

  @Override
  public Snapshot loadSnapshot(
      Engine engine, Optional<Long> versionOpt, Optional<Long> timestampOpt) {
    return ucManagedClient.loadSnapshot(engine, tableId, tablePath, versionOpt, timestampOpt);
  }

  @Override
  public CommitRange loadCommitRange(
      Engine engine,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt) {
    return ucManagedClient.loadCommitRange(
        engine,
        tableId,
        tablePath,
        startVersionOpt,
        startTimestampOpt,
        endVersionOpt,
        endTimestampOpt);
  }

  @Override
  public void close() {
    try {
      ucClient.close();
    } catch (Exception e) {
      // Swallow close errors to avoid disrupting caller cleanup
    }
  }

  private static String extractUCTableId(CatalogTable catalogTable) {
    java.util.Map<String, String> storageProperties =
        scala.collection.JavaConverters.mapAsJavaMap(catalogTable.storage().properties());

    String ucTableId = storageProperties.get(io.delta.storage.commit.uccommitcoordinator
        .UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
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
    final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
    final String UC_CONNECTOR_CLASS = "io.unitycatalog.spark.UCSingleCatalog";

    scala.Option<String> catalogOption = catalogTable.identifier().catalog();
    String catalogName =
        catalogOption.isDefined()
            ? catalogOption.get()
            : spark.sessionState().catalogManager().currentCatalog().name();

    Map<String, CatalogEntry> entries = new HashMap<>();
    spark.conf()
        .getAll()
        .forEach(
            (k, v) -> {
              if (!k.startsWith(SPARK_SQL_CATALOG_PREFIX)) {
                return;
              }
              String remainder = k.substring(SPARK_SQL_CATALOG_PREFIX.length());
              int dotIdx = remainder.indexOf('.');
              String name = dotIdx == -1 ? remainder : remainder.substring(0, dotIdx);
              String keySuffix = dotIdx == -1 ? "" : remainder.substring(dotIdx + 1);
              CatalogEntry entry = entries.computeIfAbsent(name, CatalogEntry::new);
              if (keySuffix.isEmpty()) {
                entry.connectorClass = v;
              } else if ("uri".equals(keySuffix)) {
                entry.uri = v;
              } else if ("token".equals(keySuffix)) {
                entry.token = v;
              }
            });

    Optional<CatalogEntry> configTuple =
        entries.values().stream()
            .filter(e -> catalogName.equals(e.name))
            .filter(e -> UC_CONNECTOR_CLASS.equals(e.connectorClass))
            .filter(e -> e.uri != null && !e.uri.isEmpty())
            .filter(e -> e.token != null && !e.token.isEmpty())
            .findFirst();

    if (!configTuple.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot create UC client: Unity Catalog configuration not found for catalog '"
              + catalogName
              + "'.");
    }

    CatalogEntry config = configTuple.get();
    String uri = config.uri;
    String token = config.token;

    return UCTokenBasedRestClientFactory$.MODULE$.createUCClient(uri, token);
  }

  private static final class CatalogEntry {
    final String name;
    String connectorClass;
    String uri;
    String token;

    CatalogEntry(String name) {
      this.name = name;
    }
  }
}
