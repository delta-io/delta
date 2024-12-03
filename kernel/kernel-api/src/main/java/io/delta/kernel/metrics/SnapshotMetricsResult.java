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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.internal.metrics.SnapshotMetrics;
import java.util.Optional;

/** Stores the metrics results for a {@link SnapshotReport} */
public interface SnapshotMetricsResult {

  /**
   * @return the duration (ns) to resolve the provided timestamp to a table version for timestamp
   *     time-travel queries. Empty for time-travel by version or non-time-travel queries.
   */
  Optional<Long> timestampToVersionResolutionDuration();

  /**
   * @return the duration (ns) to load the initial delta actions for the snapshot (such as the table
   *     protocol and metadata). 0 if snapshot construction fails before log replay.
   */
  long loadInitialDeltaActionsDuration();

  static SnapshotMetricsResult fromSnapshotMetrics(SnapshotMetrics snapshotMetrics) {
    checkArgument(snapshotMetrics != null, "snapshotMetrics cannot be null");

    return new SnapshotMetricsResult() {

      final Optional<Long> timestampToVersionResolutionDuration =
          Optional.of(snapshotMetrics.timestampToVersionResolutionDuration)
              .filter(t -> t.count() > 0) // If the timer hasn't been called this should be None
              .map(t -> t.totalDuration());
      final long loadProtocolAndMetadataDuration =
          snapshotMetrics.loadProtocolAndMetadataDuration.totalDuration();

      @Override
      public Optional<Long> timestampToVersionResolutionDuration() {
        return timestampToVersionResolutionDuration;
      }

      @Override
      public long loadInitialDeltaActionsDuration() {
        return loadProtocolAndMetadataDuration;
      }
    };
  }
}
