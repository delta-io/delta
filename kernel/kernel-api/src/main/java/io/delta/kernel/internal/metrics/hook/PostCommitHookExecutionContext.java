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
package io.delta.kernel.internal.metrics.hook;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.hook.PostCommitHook.PostCommitHookType;
import io.delta.kernel.metrics.PostCommitHookMetricsResult;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Encapsulates context for a post-commit hook execution. */
public class PostCommitHookExecutionContext {

  private final String tablePath;
  private final PostCommitHookType hookType;
  private final Map<String, String> hookExecutionDetails;
  private Long executionDurationNs;

  /**
   * Creates a new context for a single hook execution.
   * @param tablePath absolute Delta-table path
   * @param hookType enum identifying the hook type.
   * @param hookExecutionDetails additional hook-specific execution details.
   */
  public PostCommitHookExecutionContext(
      String tablePath, PostCommitHookType hookType, Map<String, String> hookExecutionDetails) {
    this.tablePath = requireNonNull(tablePath, "Table path cannot be null");
    this.hookType = requireNonNull(hookType, "Hook type enum cannot be null");
    this.hookExecutionDetails =
        Collections.unmodifiableMap(
            requireNonNull(hookExecutionDetails, "hookExecutionDetails cannot be null"));
  }

  /** @return the Delta-table path associated with this hook run. */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the hook type executed for this context. */
  public PostCommitHookType getHookType() {
    return hookType;
  }

  /**
   * @return immutable map of diagnostic key/value pairs supplied by the hook (e.g.,
   *     checkpointVersion, startVersion→endVersion range).
   */
  public Map<String, String> getHookExecutionDetails() {
    return hookExecutionDetails;
  }

  /**
   * Records the execution duration for this hook
   * @param durationNs the duration in nanoseconds
   */
  public void setExecutionDurationNs(long durationNs) {
    this.executionDurationNs = durationNs;
  }

  /**
   * Creates a PostCommitHookMetricsResult reflecting the recorded duration.
   * @return PostCommitHookMetricsResult containing the timing for this hook type
   */
  public PostCommitHookMetricsResult getPostCommitHookMetrics() {
    return new PostCommitHookMetricsResult() {
      @Override
      public Optional<Long> getCheckpointDurationNs() {
        return hookType == PostCommitHookType.CHECKPOINT
            ? Optional.ofNullable(executionDurationNs)
            : Optional.empty();
      }

      @Override
      public Optional<Long> getChecksumDurationNs() {
        return hookType == PostCommitHookType.CHECKSUM_SIMPLE
            ? Optional.ofNullable(executionDurationNs)
            : Optional.empty();
      }

      @Override
      public Optional<Long> getChecksumFullDurationNs() {
        return hookType == PostCommitHookType.CHECKSUM_FULL
            ? Optional.ofNullable(executionDurationNs)
            : Optional.empty();
      }

      @Override
      public Optional<Long> getLogCompactionDurationNs() {
        return hookType == PostCommitHookType.LOG_COMPACTION
            ? Optional.ofNullable(executionDurationNs)
            : Optional.empty();
      }
    };
  }
}
