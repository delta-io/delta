/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics;

import io.delta.kernel.metrics.SnapshotReport;
import java.util.Optional;

/**
 * Stores the context for a given Snapshot query. This includes information about the query
 * parameters (i.e. table path, time travel parameters), updated state as the snapshot query
 * progresses (i.e. resolved version), and metrics.
 *
 * <p>This is used to generate a {@link SnapshotReport}. It exists from snapshot query initiation
 * until either successful snapshot construction or failure.
 */
public class SnapshotQueryContext {

  /** Creates a {@link SnapshotQueryContext} for a Snapshot created by a latest snapshot query */
  public static SnapshotQueryContext forLatestSnapshot(String tablePath) {
    return new SnapshotQueryContext(tablePath, Optional.empty(), Optional.empty());
  }

  /** Creates a {@link SnapshotQueryContext} for a Snapshot created by a AS OF VERSION query */
  public static SnapshotQueryContext forVersionSnapshot(String tablePath, long version) {
    return new SnapshotQueryContext(tablePath, Optional.of(version), Optional.empty());
  }

  /** Creates a {@link SnapshotQueryContext} for a Snapshot created by a AS OF TIMESTAMP query */
  public static SnapshotQueryContext forTimestampSnapshot(String tablePath, long timestamp) {
    return new SnapshotQueryContext(tablePath, Optional.empty(), Optional.of(timestamp));
  }

  private final String tablePath;
  private Optional<Long> version;
  private final Optional<Long> providedTimestamp;
  private final SnapshotMetrics snapshotMetrics = new SnapshotMetrics();

  /**
   * @param tablePath the table path for the table being queried
   * @param providedVersion the provided version for a time-travel-by-version query, empty if this
   *     is not a time-travel-by-version query
   * @param providedTimestamp the provided timestamp for a time-travel-by-timestamp query, empty if
   *     this is not a time-travel-by-timestamp query
   */
  private SnapshotQueryContext(
      String tablePath, Optional<Long> providedVersion, Optional<Long> providedTimestamp) {
    this.tablePath = tablePath;
    this.version = providedVersion;
    this.providedTimestamp = providedTimestamp;
  }

  public String getTablePath() {
    return tablePath;
  }

  public Optional<Long> getVersion() {
    return version;
  }

  public Optional<Long> getProvidedTimestamp() {
    return providedTimestamp;
  }

  public SnapshotMetrics getSnapshotMetrics() {
    return snapshotMetrics;
  }

  /**
   * Updates the {@code version} stored in this snapshot context. This version should be updated
   * upon version resolution for non time-travel-by-version queries. For latest snapshot queries
   * this is after log segment construction. For time-travel by timestamp queries this is after
   * timestamp to version resolution.
   */
  public void setVersion(long updatedVersion) {
    version = Optional.of(updatedVersion);
  }

  @Override
  public String toString() {
    return String.format(
        "SnapshotQueryContext(tablePath=%s, version=%s, providedTimestamp=%s, snapshotMetric=%s)",
        tablePath, version, providedTimestamp, snapshotMetrics);
  }
}
