/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.kernel.unitycatalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import java.util.Optional;

/** Options for loading a {@link CommitRange} from Unity Catalog. */
public final class UCLoadCommitRangeOptions {
  private final Optional<Long> startVersionOpt;
  private final Optional<Long> startTimestampOpt;
  private final Optional<Long> endVersionOpt;
  private final Optional<Long> endTimestampOpt;
  private final Optional<Boolean> commitRangeValidationOpt;
  private final Optional<Boolean> failOnDataLossOpt;

  /**
   * @param startVersionOpt Inclusive start version. Exactly one of this and {@code
   *     startTimestampOpt} must be present.
   * @param startTimestampOpt Start timestamp in milliseconds since epoch, resolved to the first
   *     commit at or after the timestamp. Mutually exclusive with {@code startVersionOpt}.
   * @param endVersionOpt Inclusive end version. Mutually exclusive with {@code endTimestampOpt}.
   * @param endTimestampOpt End timestamp in milliseconds since epoch, resolved to the last commit
   *     at or before the timestamp. If both end boundaries are empty, the latest version is used.
   * @param commitRangeValidationOpt Whether to validate that the range is complete and contiguous.
   *     If empty, uses the default of {@code true}. When {@code false}, only {@link
   *     CommitRange#getCommitFiles()} is supported. See {@link
   *     CommitRangeBuilder#withCommitRangeValidation(boolean)}.
   * @param failOnDataLossOpt Whether commit-file iteration fails on missing versions. If empty,
   *     uses the default of {@code true}. Only affects ranges with validation disabled. See {@link
   *     CommitRangeBuilder#withFailOnDataLoss(boolean)}.
   */
  public UCLoadCommitRangeOptions(
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt,
      Optional<Boolean> commitRangeValidationOpt,
      Optional<Boolean> failOnDataLossOpt) {
    this.startVersionOpt = requireNonNull(startVersionOpt, "startVersionOpt is null");
    this.startTimestampOpt = requireNonNull(startTimestampOpt, "startTimestampOpt is null");
    this.endVersionOpt = requireNonNull(endVersionOpt, "endVersionOpt is null");
    this.endTimestampOpt = requireNonNull(endTimestampOpt, "endTimestampOpt is null");
    this.commitRangeValidationOpt =
        requireNonNull(commitRangeValidationOpt, "commitRangeValidationOpt is null");
    this.failOnDataLossOpt = requireNonNull(failOnDataLossOpt, "failOnDataLossOpt is null");
  }

  public Optional<Long> getStartVersionOpt() {
    return startVersionOpt;
  }

  public Optional<Long> getStartTimestampOpt() {
    return startTimestampOpt;
  }

  public Optional<Long> getEndVersionOpt() {
    return endVersionOpt;
  }

  public Optional<Long> getEndTimestampOpt() {
    return endTimestampOpt;
  }

  public Optional<Boolean> getCommitRangeValidationOpt() {
    return commitRangeValidationOpt;
  }

  public Optional<Boolean> getFailOnDataLossOpt() {
    return failOnDataLossOpt;
  }
}
