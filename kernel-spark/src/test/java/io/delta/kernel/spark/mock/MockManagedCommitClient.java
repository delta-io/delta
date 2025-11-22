/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.kernel.spark.mock;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.catalog.ManagedCommitClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of ManagedCommitClient for testing.
 *
 * <p>This proves that the interface is complete and usable without any actual catalog dependencies.
 */
public class MockManagedCommitClient implements ManagedCommitClient {

  private final Map<String, Map<Long, Snapshot>> tableSnapshots = new ConcurrentHashMap<>();

  /**
   * Adds a snapshot to the mock client for testing.
   *
   * @param tableId the table identifier
   * @param version the version number
   * @param snapshot the snapshot to store
   */
  public void addSnapshot(String tableId, long version, Snapshot snapshot) {
    tableSnapshots.computeIfAbsent(tableId, k -> new HashMap<>()).put(version, snapshot);
  }

  @Override
  public Snapshot getSnapshot(
      Engine engine,
      String tableId,
      String tablePath,
      Optional<Long> version,
      Optional<Long> timestampMillis) {

    Map<Long, Snapshot> versions = tableSnapshots.get(tableId);
    if (versions == null) {
      throw new IllegalArgumentException("Table not found: " + tableId);
    }

    long targetVersion = version.orElse(getLatestVersion(tableId));
    Snapshot snapshot = versions.get(targetVersion);
    if (snapshot == null) {
      throw new IllegalArgumentException("Version not found: " + targetVersion);
    }

    return snapshot;
  }

  @Override
  public boolean versionExists(String tableId, long version) {
    Map<Long, Snapshot> versions = tableSnapshots.get(tableId);
    return versions != null && versions.containsKey(version);
  }

  @Override
  public long getLatestVersion(String tableId) {
    Map<Long, Snapshot> versions = tableSnapshots.get(tableId);
    if (versions == null || versions.isEmpty()) {
      return -1;
    }
    return versions.keySet().stream().max(Long::compareTo).orElse(-1L);
  }

  @Override
  public void close() {
    tableSnapshots.clear();
  }
}
