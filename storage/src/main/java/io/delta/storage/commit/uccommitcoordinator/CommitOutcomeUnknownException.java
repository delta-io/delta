/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

/**
 * Raised when UC received an {@code add-commit} but cannot report whether it landed: the version
 * is at or below the latest commit, yet UC can neither match it by staged file name (the row was
 * backfilled and cleaned up) nor read the staged or published file to compare contents.
 *
 * <p>A client must not act on this error alone. It reloads the table and compares UC's ratified
 * file name for the version against the UUID file name it generated, falling back to comparing the
 * published {@code <version>.json} against its staged commit. Re-sending without that check
 * double-commits the data whenever the original commit had in fact landed, and so does rebasing
 * onto the next version.
 *
 * <p>Deliberately <em>not</em> a {@link io.delta.storage.commit.CommitFailedException}: there is no
 * {@code (retryable, conflict)} pair that means "unknown", and both retryable pairs tell an
 * unaware caller to re-commit the same data. Sitting under {@link UCCommitCoordinatorException}
 * means a caller that has not implemented recovery falls into that type's existing handling and
 * fails the operation, which is the only safe default.
 */
public class CommitOutcomeUnknownException extends UCCommitCoordinatorException {
  public CommitOutcomeUnknownException(String message) {
    super(message);
  }

  public CommitOutcomeUnknownException(String message, Throwable cause) {
    super(message, cause);
  }
}
