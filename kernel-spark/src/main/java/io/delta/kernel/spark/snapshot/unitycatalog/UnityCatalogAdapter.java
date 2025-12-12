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

import io.delta.kernel.spark.snapshot.ManagedCatalogAdapter;
import io.delta.storage.commit.GetCommitsResponse;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Unity Catalog implementation of {@link ManagedCatalogAdapter}.
 *
 * <p>This adapter is responsible only for fetching commit metadata from Unity Catalog's commit
 * coordinator API. It does not contain any Delta/Kernel snapshot building logic - that
 * responsibility belongs to the snapshot manager layer.
 *
 * <p>Methods are stubbed in this wireframe PR and will be implemented in a follow-up once UC
 * operations are enabled.
 */
public final class UnityCatalogAdapter implements ManagedCatalogAdapter {

  private final String tableId;
  private final String tablePath;
  private final String endpoint;
  private final String token;

  public UnityCatalogAdapter(String tableId, String tablePath, String endpoint, String token) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.endpoint = requireNonNull(endpoint, "endpoint is null");
    this.token = requireNonNull(token, "token is null");
  }

  /**
   * Creates adapter from Spark catalog table.
   *
   * <p>Extracts UC connection info from Spark metadata and creates the adapter.
   *
   * @param catalogTable the catalog table metadata
   * @param spark the active SparkSession
   * @return Optional containing the adapter if this is a UC-managed table, empty otherwise
   */
  public static Optional<ManagedCatalogAdapter> fromCatalog(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");

    return SparkUnityCatalogUtils.extractConnectionInfo(catalogTable, spark)
        .map(UnityCatalogAdapter::fromConnectionInfo);
  }

  /** Creates adapter from connection info (no Spark dependency). */
  public static ManagedCatalogAdapter fromConnectionInfo(UnityCatalogConnectionInfo info) {
    requireNonNull(info, "info is null");
    return new UnityCatalogAdapter(
        info.getTableId(), info.getTablePath(), info.getEndpoint(), info.getToken());
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  @Override
  public String getTablePath() {
    return tablePath;
  }

  /** Returns the UC endpoint URL. */
  public String getEndpoint() {
    return endpoint;
  }

  /** Returns the UC authentication token. */
  public String getToken() {
    return token;
  }

  @Override
  public GetCommitsResponse getCommits(long startVersion, Optional<Long> endVersion) {
    requireNonNull(endVersion, "endVersion is null");
    throw new UnsupportedOperationException("UC getCommits not implemented yet");
  }

  @Override
  public long getLatestRatifiedVersion() {
    throw new UnsupportedOperationException("UC getLatestRatifiedVersion not implemented yet");
  }

  @Override
  public void close() {
    // no-op in wireframe; will close UCClient in implementation
  }
}
