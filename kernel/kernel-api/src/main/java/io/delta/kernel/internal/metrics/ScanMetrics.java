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
package io.delta.kernel.internal.metrics;

import io.delta.kernel.metrics.ScanMetricsResult;

/**
 * Stores the metrics for an ongoing scan. These metrics are updated and recorded throughout the
 * scan using this class.
 *
 * <p>At report time, we create an immutable {@link ScanMetricsResult} from an instance of {@link
 * ScanMetrics} to capture the metrics collected during the scan. The {@link ScanMetricsResult}
 * interface exposes getters for any metrics collected in this class.
 */
public class ScanMetrics {

  public final Timer totalPlanningTimer = new Timer();

  public final Counter addFilesCounter = new Counter();

  public final Counter addFilesFromDeltaFilesCounter = new Counter();

  public final Counter activeAddFilesCounter = new Counter();

  public final Counter duplicateAddFilesCounter = new Counter();

  public final Counter removeFilesFromDeltaFilesCounter = new Counter();

  public ScanMetricsResult captureScanMetricsResult() {
    return new ScanMetricsResult() {

      final long totalPlanningDurationNs = totalPlanningTimer.totalDurationNs();
      final long numAddFilesSeen = addFilesCounter.value();
      final long numAddFilesSeenFromDeltaFiles = addFilesFromDeltaFilesCounter.value();
      final long numActiveAddFiles = activeAddFilesCounter.value();
      final long numDuplicateAddFiles = duplicateAddFilesCounter.value();
      final long numRemoveFilesSeenFromDeltaFiles = removeFilesFromDeltaFilesCounter.value();

      @Override
      public long getTotalPlanningDurationNs() {
        return totalPlanningDurationNs;
      }

      @Override
      public long getNumAddFilesSeen() {
        return numAddFilesSeen;
      }

      @Override
      public long getNumAddFilesSeenFromDeltaFiles() {
        return numAddFilesSeenFromDeltaFiles;
      }

      @Override
      public long getNumActiveAddFiles() {
        return numActiveAddFiles;
      }

      @Override
      public long getNumDuplicateAddFiles() {
        return numDuplicateAddFiles;
      }

      @Override
      public long getNumRemoveFilesSeenFromDeltaFiles() {
        return numRemoveFilesSeenFromDeltaFiles;
      }
    };
  }

  @Override
  public String toString() {
    return String.format(
        "ScanMetrics(totalPlanningTimer=%s, addFilesCounter=%s, addFilesFromDeltaFilesCounter=%s,"
            + " activeAddFilesCounter=%s, duplicateAddFilesCounter=%s, "
            + "removeFilesFromDeltaFilesCounter=%s",
        totalPlanningTimer,
        addFilesCounter,
        addFilesFromDeltaFilesCounter,
        activeAddFilesCounter,
        duplicateAddFilesCounter,
        removeFilesFromDeltaFilesCounter);
  }
}
