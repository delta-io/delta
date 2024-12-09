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

package io.delta.kernel.internal.coordinatedcommits;

import io.delta.kernel.annotation.Evolving;

/**
 * Exception raised by {@link CommitCoordinatorClientHandler#commit}
 *
 * <pre>
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 *  </pre>
 *
 * @since 3.3.0
 */
@Evolving
public class CommitFailedException extends Exception {

  private final boolean retryable;

  private final boolean conflict;

  public CommitFailedException(boolean retryable, boolean conflict, String message) {
    super(message);
    this.retryable = retryable;
    this.conflict = conflict;
  }

  /**
   * Returns whether the commit attempt can be retried.
   *
   * @return whether the commit attempt can be retried.
   */
  public boolean getRetryable() {
    return retryable;
  }

  /**
   * Returns whether the commit failed due to a conflict.
   *
   * @return whether the commit failed due to a conflict.
   */
  public boolean getConflict() {
    return conflict;
  }
}
