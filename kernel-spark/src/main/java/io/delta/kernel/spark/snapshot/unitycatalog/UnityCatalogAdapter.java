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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.snapshot.ManagedCatalogAdapter;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;

/** UC-backed implementation of {@link ManagedCatalogAdapter}. */
public final class UnityCatalogAdapter implements ManagedCatalogAdapter {

  private final String tableId;
  private final String tablePath;
  private final UCClient ucClient;
  private final UCCatalogManagedClient ucManagedClient;

  public UnityCatalogAdapter(String tableId, String tablePath, UCClient ucClient) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.ucManagedClient = new UCCatalogManagedClient(ucClient);
  }

  /**
   * Builds a UC-backed {@link ManagedCatalogAdapter} for a UC-managed table.
   *
   * @throws IllegalArgumentException if the table lacks UC identifiers or catalog config is missing
   */
  public static Optional<ManagedCatalogAdapter> fromCatalog(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return Optional.empty();
    }

    String tableId = extractUCTableId(catalogTable);
    String tablePath = extractTablePath(catalogTable);
    UCClient client = createUCClient(catalogTable, spark);
    return Optional.of(new UnityCatalogAdapter(tableId, tablePath, client));
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
  public List<ParsedLogData> getRatifiedCommits(Optional<Long> endVersionOpt) {
    GetCommitsResponse response = getCommitsFromUC(endVersionOpt);
    return response.getCommits().stream()
        .sorted(Comparator.comparingLong(Commit::getVersion))
        .map(
            commit ->
                ParsedCatalogCommitData.forFileStatus(
                    hadoopFileStatusToKernelFileStatus(commit.getFileStatus())))
        .collect(Collectors.toList());
  }

  @Override
  public long getLatestRatifiedVersion() {
    GetCommitsResponse response = getCommitsFromUC(Optional.empty());
    long maxRatified = response.getLatestTableVersion();
    // UC returns -1 when only 0.json exists (CREATE not yet registered with UC)
    return maxRatified == -1 ? 0 : maxRatified;
  }

  @Override
  public void close() {
    try {
      ucClient.close();
    } catch (Exception e) {
      // Swallow close errors to avoid disrupting caller cleanup
    }
  }

  private GetCommitsResponse getCommitsFromUC(Optional<Long> endVersionOpt) {
    try {
      return ucClient.getCommits(
          tableId,
          new Path(tablePath).toUri(),
          Optional.empty(), // startVersion
          endVersionOpt);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (UCCommitCoordinatorException e) {
      throw new RuntimeException(e);
    }
  }

  private static io.delta.kernel.utils.FileStatus hadoopFileStatusToKernelFileStatus(
      org.apache.hadoop.fs.FileStatus hadoopFS) {
    return io.delta.kernel.utils.FileStatus.of(
        hadoopFS.getPath().toString(), hadoopFS.getLen(), hadoopFS.getModificationTime());
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
