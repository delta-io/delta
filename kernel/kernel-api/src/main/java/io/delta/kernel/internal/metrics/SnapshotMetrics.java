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

import io.delta.kernel.metrics.ScanMetricsResult;
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

  public final Timer loadSnapshotTotalTimer = new Timer();

  public final Timer computeTimestampToVersionTotalDurationTimer = new Timer();

  public final Timer loadProtocolMetadataTotalDurationTimer = new Timer();

  public final Timer loadLogSegmentTotalDurationTimer = new Timer();

  public final Timer loadCrcTotalDurationTimer = new Timer();

  public final ScanMetrics scanMetrics = new ScanMetrics();

  public SnapshotMetricsResult captureSnapshotMetricsResult() {
    return new SnapshotMetricsResult() {
      final Optional<Long> computeTimestampToVersionTotalDurationResult =
          computeTimestampToVersionTotalDurationTimer.totalDurationIfRecorded();
      final long loadSnapshotTotalDurationResult = loadSnapshotTotalTimer.totalDurationNs();
      final long loadProtocolMetadataTotalDurationResult =
          loadProtocolMetadataTotalDurationTimer.totalDurationNs();
      final long loadLogSegmentTotalDurationResult =
          loadLogSegmentTotalDurationTimer.totalDurationNs();
      final long loadCrcTotalDurationResult = loadCrcTotalDurationTimer.totalDurationNs();
      final ScanMetricsResult scanMetricsResult = scanMetrics.captureScanMetricsResult();

      @Override
      public Optional<Long> getComputeTimestampToVersionTotalDurationNs() {
        return computeTimestampToVersionTotalDurationResult;
      }

      @Override
      public long getLoadSnapshotTotalDurationNs() {
        return loadSnapshotTotalDurationResult;
      }

      @Override
      public long getLoadProtocolMetadataTotalDurationNs() {
        return loadProtocolMetadataTotalDurationResult;
      }

      @Override
      public long getLoadLogSegmentTotalDurationNs() {
        return loadLogSegmentTotalDurationResult;
      }

      @Override
      public long getLoadCrcTotalDurationNs() {
        return loadCrcTotalDurationResult;
      }

      @Override
      public ScanMetricsResult getScanMetricsResult() {
        return scanMetricsResult;
      }
    };
  }

  @Override
  public String toString() {
    return String.format(
        "SnapshotMetrics("
            + "computeTimestampToVersionTotalDurationTimer=%s, "
            + "loadSnapshotTotalTimer=%s,"
            + "loadProtocolMetadataTotalDurationTimer=%s, "
            + "timeToBuildLogSegmentForVersionTimer=%s, "
            + "loadCrcTotalDurationNsTimer=%s)",
        computeTimestampToVersionTotalDurationTimer,
        loadSnapshotTotalTimer,
        loadProtocolMetadataTotalDurationTimer,
        loadLogSegmentTotalDurationTimer,
        loadCrcTotalDurationTimer);
  }
}
