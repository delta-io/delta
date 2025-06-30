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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Optional;

/** Stores the metrics results for a {@link SnapshotReport} */
@JsonPropertyOrder({
  "loadSnapshotTotalDurationNs",
  "computeTimestampToVersionTotalDurationNs",
  "loadProtocolMetadataTotalDurationNs",
  "timeToBuildLogSegmentForVersionNs",
  "loadCrcTotalDurationNs"
})
public interface SnapshotMetricsResult {

  /**
   * @return the total duration (ns) to load the snapshot, including all steps such as resolving
   *     timestamp to version, LISTing the _delta_log, building the log segment, and determining the
   *     latest protocol and metadata.
   */
  long getLoadSnapshotTotalDurationNs();

  /**
   * @return the duration (ns) to resolve the provided timestamp to a table version for timestamp
   *     time-travel queries. Empty for time-travel by version or non-time-travel queries.
   */
  Optional<Long> getComputeTimestampToVersionTotalDurationNs();

  /**
   * @return the duration (ns) to load the initial delta actions for the snapshot (such as the table
   *     protocol and metadata). 0 if snapshot construction fails before log replay.
   */
  long getLoadProtocolMetadataTotalDurationNs();

  /**
   * @return the duration (ns) to build the log segment for the specified version during snapshot
   *     construction. 0 if snapshot construction fails before this step.
   */
  long getTimeToBuildLogSegmentForVersionNs();

  /**
   * @return the duration (ns) to get CRC information during snapshot construction. 0 if snapshot
   *     construction fails before this step or if CRC is not read in loading snapshot.
   */
  long getLoadCrcTotalDurationNs();
}
