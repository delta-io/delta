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
package io.delta.kernel.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;

/** Defines the metadata and metrics for post-commit hook execution {@link MetricsReport} */
@JsonSerialize(as = PostCommitHookMetricsReport.class)
@JsonPropertyOrder({
  "tablePath",
  "operationType",
  "reportUUID",
  "exception",
  "hookType",
  "hookExecutionDetails",
  "postCommitHookMetrics"
})
public interface PostCommitHookMetricsReport extends DeltaOperationReport {

  /** @return the type of hook that was executed (CHECKPOINT, CHECKSUM_SIMPLE, etc.) */
  String getHookType();

  /**
   * @return hook-specific context provided by the hook implementation. For CHECKPOINT:
   *     checkpointVersion, checkpointInterval, etc. For LOG_COMPACTION: startVersion, endVersion,
   *     compactionInterval, etc. For CHECKSUM_*: checksumType, fileCount, etc.
   */
  Map<String, String> getHookExecutionDetails();

  /** @return the metrics for this specific post-commit hook execution */
  PostCommitHookMetricsResult getPostCommitHookMetrics();

  @Override
  default String getOperationType() {
    return "PostCommitHook";
  }
}
