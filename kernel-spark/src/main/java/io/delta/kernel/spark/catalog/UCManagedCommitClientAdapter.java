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

package io.delta.kernel.spark.catalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;

/**
 * Adapter that wraps Unity Catalog's UCCatalogManagedClient to implement the generic
 * ManagedCommitClient interface.
 *
 * <p>This adapter isolates all UC-specific code in the UC module, keeping kernel-spark
 * catalog-agnostic.
 */
public class UCManagedCommitClientAdapter implements ManagedCommitClient {

  private final UCCatalogManagedClient ucClient;
  private final UCClient rawUCClient;
  private final String tablePath;

  public UCManagedCommitClientAdapter(
      UCCatalogManagedClient ucClient, UCClient rawUCClient, String tablePath) {
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.rawUCClient = requireNonNull(rawUCClient, "rawUCClient is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
  }

  @Override
  public Snapshot getSnapshot(
      Engine engine,
      String tableId,
      String tablePath,
      Optional<Long> version,
      Optional<Long> timestampMillis) {

    // Delegate to UC-specific implementation
    return ucClient.loadSnapshot(engine, tableId, tablePath, version, timestampMillis);
  }

  @Override
  public boolean versionExists(String tableId, long version) {
    try {
      // Try to get commits for this specific version
      // If the version doesn't exist, UC will return an empty response or throw an exception
      GetCommitsResponse response =
          rawUCClient.getCommits(
              tableId, new Path(tablePath).toUri(), Optional.of(version), Optional.of(version));

      // If we get a response with commits, or if latestTableVersion >= version, it exists
      return !response.getCommits().isEmpty() || response.getLatestTableVersion() >= version;
    } catch (IOException | UCCommitCoordinatorException e) {
      // If there's an error, assume the version doesn't exist
      return false;
    }
  }

  @Override
  public long getLatestVersion(String tableId) {
    try {
      // Get commits without specifying an end version to get the latest table version
      GetCommitsResponse response =
          rawUCClient.getCommits(
              tableId, new Path(tablePath).toUri(), Optional.empty(), Optional.empty());

      long latestVersion = response.getLatestTableVersion();
      // UC returns -1 if only 0.json exists (table just created)
      return latestVersion == -1 ? 0 : latestVersion;
    } catch (IOException | UCCommitCoordinatorException e) {
      throw new RuntimeException("Failed to get latest version for table " + tableId, e);
    }
  }

  @Override
  public void close() throws Exception {
    rawUCClient.close();
  }
}
