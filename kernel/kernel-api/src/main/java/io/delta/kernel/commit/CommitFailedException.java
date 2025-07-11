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

package io.delta.kernel.commit;

import io.delta.kernel.annotation.Experimental;

/**
 * Exception raised by {@link Committer#commit}.
 *
 * <pre>
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 * </pre>
 */
@Experimental
public class CommitFailedException extends Exception {

  private final boolean retryable;
  private final boolean conflict;

  // TODO: [delta-io/delta#4908] Include the winning, conflicting catalog ratified commits here

  public CommitFailedException(boolean retryable, boolean conflict, String message) {
    super(message);
    this.retryable = retryable;
    this.conflict = conflict;
  }

  public CommitFailedException(
      boolean retryable, boolean conflict, String message, Throwable cause) {
    super(message, cause);
    this.retryable = retryable;
    this.conflict = conflict;
  }

  /** Returns whether the commit can be retried. */
  public boolean isRetryable() {
    return retryable;
  }

  /** Returns whether the commit failed due to a conflict. */
  public boolean isConflict() {
    return conflict;
  }
}
