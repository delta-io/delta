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
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.snapshot.ManagedCatalogAdapter;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * UC-backed implementation shell of {@link ManagedCatalogAdapter}.
 *
 * <p>Methods are intentionally stubbed in this wireframe PR and will be implemented in a follow-up
 * once UC operations are enabled.
 */
public final class UnityCatalogAdapter implements ManagedCatalogAdapter {

  private final String tableId;
  private final String tablePath;

  public UnityCatalogAdapter(String tableId, String tablePath) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
  }

  /**
   * Creates adapter from Spark catalog table (convenience method).
   *
   * <p>Extracts UC connection info from Spark metadata and delegates to {@link
   * #fromConnectionInfo}.
   */
  public static Optional<ManagedCatalogAdapter> fromCatalog(
      CatalogTable catalogTable, SparkSession spark) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");
    throw new UnsupportedOperationException("UC wiring deferred to implementation PR");
  }

  /** Creates adapter from connection info (no Spark dependency). */
  public static ManagedCatalogAdapter fromConnectionInfo(UnityCatalogConnectionInfo info) {
    requireNonNull(info, "info is null");
    return new UnityCatalogAdapter(info.getTableId(), info.getTablePath());
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
    throw new UnsupportedOperationException("UC snapshot loading not implemented yet");
  }

  @Override
  public CommitRange loadCommitRange(
      Engine engine,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt) {
    throw new UnsupportedOperationException("UC commit range loading not implemented yet");
  }

  @Override
  public List<ParsedLogData> getRatifiedCommits(Optional<Long> endVersionOpt) {
    throw new UnsupportedOperationException("UC commit listing not implemented yet");
  }

  @Override
  public long getLatestRatifiedVersion() {
    throw new UnsupportedOperationException("UC ratified version lookup not implemented yet");
  }

  @Override
  public void close() {
    // no-op in wireframe
  }
}
