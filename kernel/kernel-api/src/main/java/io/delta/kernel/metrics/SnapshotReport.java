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
package io.delta.kernel.metrics;

import java.util.Optional;

/** Defines the metadata and metrics for a snapshot construction {@link MetricsReport} */
public interface SnapshotReport extends DeltaOperationReport {

  /**
   * For a time-travel by version query, this is the version provided. For a time-travel by
   * timestamp query, this is the version resolved from the provided timestamp. For a latest
   * snapshot, this is the version read from the delta log.
   *
   * <p>This is empty when this report is for a failed snapshot construction, and the error occurs
   * before a version can be resolved.
   *
   * @return the version of the snapshot
   */
  Optional<Long> version();

  /**
   * @return the timestamp provided for time-travel, empty if this is not a timestamp-based
   *     time-travel query
   */
  Optional<Long> providedTimestamp();

  /** @return the metrics for this snapshot construction */
  SnapshotMetricsResult snapshotMetrics();

  @Override
  default String operationType() {
    return "Snapshot";
  }
}
