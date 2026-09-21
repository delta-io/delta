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

import io.delta.storage.commit.CommitFailedException;

/**
 * Raised when UC cannot determine whether the commit at the requested version already landed: the
 * version is at or below the latest commit, but UC can neither match it by staged file name (the
 * row was backfilled and cleaned up) nor read the staged or published file to compare contents, so
 * it cannot tell whether the version holds this caller's own commit or a different writer's.
 *
 * <p>The client must not act on the error alone. It reloads the table and compares UC's ratified
 * file name for the version against the UUID file name it generated, falling back to comparing the
 * published {@code <version>.json} against its staged commit. Re-sending without that check
 * double-commits the data at version+1 whenever the original commit had in fact landed.
 *
 * <p>Extends {@link CommitFailedException} (retryable + conflict) on purpose: it must be caught by
 * the commit-coordinator retry loop, where the recovery lives.
 *
 * @deprecated Use {@link CommitOutcomeUnknownException}. Carrying an unknown outcome as a
 *     retryable conflict makes it fail open: a caller that does not special-case this subclass
 *     reads the flags, rebases onto the winning version, and re-commits data that may already have
 *     landed. Retained because it ships in delta-storage 4.4.0 and {@code UCClient} implementations
 *     may already throw it; it is still recognised by
 *     {@link UCCommitCoordinatorClient#commit}, which routes it through the same recovery.
 */
@Deprecated
public class CommitCompletionUnknownException extends CommitFailedException {
  public CommitCompletionUnknownException(String message) {
    super(true /* retryable */, true /* conflict */, message);
  }
}
