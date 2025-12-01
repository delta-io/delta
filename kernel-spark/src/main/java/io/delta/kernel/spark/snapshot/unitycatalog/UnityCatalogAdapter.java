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
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClientFactory$;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

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
   * Creates adapter from Spark catalog table (convenience method).
   *
   * <p>Extracts UC connection info from Spark metadata and delegates to {@link
   * #fromConnectionInfo}.
   *
   * @param catalogTable Spark catalog table metadata
   * @param spark SparkSession for resolving Unity Catalog configurations
   * @return adapter if table is UC-managed, empty otherwise
   * @throws IllegalArgumentException if table is UC-managed but configuration is invalid
   */
  public static Optional<ManagedCatalogAdapter> fromCatalog(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    return SparkUnityCatalogUtils.extractConnectionInfo(catalogTable, spark)
        .map(UnityCatalogAdapter::fromConnectionInfo);
  }

  /**
   * Creates adapter from connection info (no Spark dependency).
   *
   * <p>This method allows creating a UC adapter without Spark dependencies if you have connection
   * information directly.
   *
   * @param info Unity Catalog connection information
   * @return adapter instance
   */
  public static ManagedCatalogAdapter fromConnectionInfo(UnityCatalogConnectionInfo info) {
    requireNonNull(info, "info is null");
    UCClient client =
        UCTokenBasedRestClientFactory$.MODULE$.createUCClient(info.getEndpoint(), info.getToken());
    return new UnityCatalogAdapter(info.getTableId(), info.getTablePath(), client);
  }

  public String getTableId() {
    return tableId;
  }

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
}
