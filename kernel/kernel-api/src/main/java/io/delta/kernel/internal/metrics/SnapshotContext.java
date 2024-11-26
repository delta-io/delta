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

import java.util.Optional;

/** Stores the context for a given Snapshot query. Used to generate a {@link SnapshotReport} */
public class SnapshotContext {

  public static SnapshotContext forLatestSnapshot(String tablePath) {
    return new SnapshotContext(tablePath, Optional.empty(), Optional.empty());
  }

  public static SnapshotContext forVersionSnapshot(String tablePath, long version) {
    return new SnapshotContext(tablePath, Optional.of(version), Optional.empty());
  }

  public static SnapshotContext forTimestampSnapshot(String tablePath, long timestamp) {
    return new SnapshotContext(tablePath, Optional.empty(), Optional.of(timestamp));
  }

  private Optional<Long> version;
  private final SnapshotMetrics snapshotMetrics = new SnapshotMetrics();
  private final String tablePath;
  private final Optional<Long> providedTimestamp;

  private SnapshotContext(
      String tablePath, Optional<Long> version, Optional<Long> providedTimestamp) {
    this.tablePath = tablePath;
    this.version = version;
    this.providedTimestamp = providedTimestamp;
  }

  public Optional<Long> getVersion() {
    return version;
  }

  public SnapshotMetrics getSnapshotMetrics() {
    return snapshotMetrics;
  }

  public String getTablePath() {
    return tablePath;
  }

  public Optional<Long> getProvidedTimestamp() {
    return providedTimestamp;
  }

  public void setVersion(long updatedVersion) {
    version = Optional.of(updatedVersion);
  }
}
