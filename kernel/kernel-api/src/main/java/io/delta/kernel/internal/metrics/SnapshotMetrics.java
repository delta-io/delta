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

import io.delta.kernel.metrics.SnapshotMetricsResult;
import java.util.Optional;

/** Stores the metrics for an ongoing snapshot creation */
public class SnapshotMetrics {

  public final Timer timestampToVersionResolutionDuration = new Timer();

  public final Timer loadProtocolAndMetadataDuration = new Timer();

  public SnapshotMetricsResult captureSnapshotMetricsResult() {
    return new SnapshotMetricsResult() {

      final Optional<Long> timestampToVersionResolutionDurationResult =
          Optional.of(timestampToVersionResolutionDuration)
              .filter(t -> t.count() > 0) // If the timer hasn't been called this should be None
              .map(t -> t.totalDuration());
      final long loadProtocolAndMetadataDurationResult =
          loadProtocolAndMetadataDuration.totalDuration();

      @Override
      public Optional<Long> timestampToVersionResolutionDuration() {
        return timestampToVersionResolutionDurationResult;
      }

      @Override
      public long loadInitialDeltaActionsDuration() {
        return loadProtocolAndMetadataDurationResult;
      }
    };
  }

  @Override
  public String toString() {
    return String.format(
        "SnapshotMetrics(timestampToVersionResolutionDuration=%s, "
            + "loadProtocolAndMetadataDuration=%s)",
        timestampToVersionResolutionDuration, loadProtocolAndMetadataDuration);
  }
}
