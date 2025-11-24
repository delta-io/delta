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
import java.util.Optional;

/** UC-backed implementation of {@link ManagedCommitClient}. */
public final class UnityCatalogManagedCommitClient implements ManagedCommitClient {

  private final String tableId;
  private final String tablePath;
  private final UCClient ucClient;
  private final UCCatalogManagedClient ucManagedClient;

  public UnityCatalogManagedCommitClient(String tableId, String tablePath, UCClient ucClient) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.ucManagedClient = new UCCatalogManagedClient(ucClient);
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

}
