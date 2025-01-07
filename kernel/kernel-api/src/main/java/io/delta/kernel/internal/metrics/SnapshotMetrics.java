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

/**
 * Stores the metrics for an ongoing snapshot construction. These metrics are updated and recorded
 * throughout the snapshot query using this class.
 *
 * <p>At report time, we create an immutable {@link SnapshotMetricsResult} from an instance of
 * {@link SnapshotMetrics} to capture the metrics collected during the query. The {@link
 * SnapshotMetricsResult} interface exposes getters for any metrics collected in this class.
 */
public class SnapshotMetrics {

  public final Timer timestampToVersionResolutionDuration = new Timer();

  public final Timer loadInitialDeltaActionsDuration = new Timer();

  public SnapshotMetricsResult captureSnapshotMetricsResult() {
    return new SnapshotMetricsResult() {

      final Optional<Long> timestampToVersionResolutionDurationResult =
          timestampToVersionResolutionDuration.totalDurationIfRecorded();
      final long loadInitialDeltaActionsDurationResult =
          loadInitialDeltaActionsDuration.totalDuration();

      @Override
      public Optional<Long> timestampToVersionResolutionDuration() {
        return timestampToVersionResolutionDurationResult;
      }

      @Override
      public long loadInitialDeltaActionsDuration() {
        return loadInitialDeltaActionsDurationResult;
      }
    };
  }

  @Override
  public String toString() {
    return String.format(
        "SnapshotMetrics(timestampToVersionResolutionDuration=%s, "
            + "loadInitialDeltaActionsDuration=%s)",
        timestampToVersionResolutionDuration, loadInitialDeltaActionsDuration);
  }
}
