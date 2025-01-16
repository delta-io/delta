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

/** Stores the metrics results for a {@link ScanReport} */
@JsonPropertyOrder({
  "totalPlanningDurationNs",
  "numAddFilesSeen",
  "numAddFilesSeenFromDeltaFiles",
  "numActiveAddFiles",
  "numDuplicateAddFiles",
  "numRemoveFilesSeenFromDeltaFiles"
})
public interface ScanMetricsResult {

  /**
   * Returns the total duration to find, filter, and consume the scan files. This begins at the
   * request for the scan files and terminates once all the scan files have been consumed and the
   * scan file iterator closed. It includes reading the _delta_log, log replay, filtering
   * optimizations, and any work from the connector before closing the scan file iterator.
   *
   * @return the total duration to find, filter, and consume the scan files
   */
  long getTotalPlanningDurationNs();

  /**
   * @return the number of AddFile actions seen during log replay (from both checkpoint and delta
   *     files). For a failed scan this metric may be incomplete.
   */
  long getNumAddFilesSeen();

  /**
   * @return the number of AddFile actions seen during log replay from delta files only. For a
   *     failed scan this metric may be incomplete.
   */
  long getNumAddFilesSeenFromDeltaFiles();

  /**
   * @return the number of active AddFile actions that survived log replay (i.e. belong to the table
   *     state). For a failed scan this metric may be incomplete.
   */
  long getNumActiveAddFiles();

  /**
   * Returns the number of duplicate AddFile actions seen during log replay. The same AddFile (same
   * path and DV) can be present in multiple commit files when stats collection is run on the table.
   * In this case, the same AddFile will be added with stats without removing the original.
   *
   * @return the number of AddFile actions seen during log replay that are duplicates. For a failed
   *     scan this metric may be incomplete.
   */
  long getNumDuplicateAddFiles();

  /**
   * @return the number of RemoveFiles seen in log replay (only from delta files). For a failed scan
   *     this metric may be incomplete.
   */
  long getNumRemoveFilesSeenFromDeltaFiles();
}
