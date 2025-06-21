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
import java.util.Optional;

/** Stores the metrics results for post-commit hook execution */
@JsonPropertyOrder({
  "checkpointDurationNs",
  "checksumDurationNs",
  "checksumFullDurationNs",
  "logCompactionDurationNs"
})
public interface PostCommitHookMetricsResult {

  /**
   * @return the duration (ns) to execute the checkpoint hook. Empty if no checkpoint hook was
   *     executed.
   */
  Optional<Long> getCheckpointDurationNs();

  /**
   * @return the duration (ns) to execute the simple checksum hook. Empty if no simple checksum hook
   *     was executed.
   */
  Optional<Long> getChecksumDurationNs();

  /**
   * @return the duration (ns) to execute the full checksum hook. Empty if no full checksum hook was
   *     executed.
   */
  Optional<Long> getChecksumFullDurationNs();

  /**
   * @return the duration (ns) to execute the log compaction hook. Empty if no log compaction hook
   *     was executed.
   */
  Optional<Long> getLogCompactionDurationNs();
}
