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
package io.delta.kernel.spark.snapshot;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import java.util.List;
import java.util.Optional;

/**
 * Adapter for catalog-managed tables that knows how to load snapshots and commit ranges for a
 * specific table.
 */
public interface ManagedCatalogAdapter extends AutoCloseable {

  /** @return catalog-managed table identifier (for logging/telemetry). */
  String getTableId();

  /** @return physical table path used by Delta Kernel. */
  String getTablePath();

  Snapshot loadSnapshot(Engine engine, Optional<Long> versionOpt, Optional<Long> timestampOpt);

  CommitRange loadCommitRange(
      Engine engine,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt);

  /**
   * Gets the ratified commits from the catalog up to the specified version.
   *
   * <p>The returned list contains {@link ParsedLogData} representing each ratified commit, sorted
   * by version in ascending order. These are typically {@code ParsedCatalogCommitData} instances
   * for catalog-managed tables.
   *
   * @param endVersionOpt optional end version (inclusive); if empty, returns commits up to latest
   * @return list of parsed log data representing ratified commits, sorted by version ascending
   */
  List<ParsedLogData> getRatifiedCommits(Optional<Long> endVersionOpt);

  /**
   * Gets the latest ratified table version from the catalog.
   *
   * <p>For catalog-managed tables, this is the highest version that has been ratified by the
   * catalog coordinator.
   *
   * @return the latest version ratified by the catalog, or 0 if only the initial commit exists
   */
  long getLatestRatifiedVersion();

  @Override
  void close();
}
