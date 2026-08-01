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
 * version is at or below the latest commit, but UC retains no row at it (the row was backfilled and
 * cleaned up), so UC cannot tell whether the version holds this caller's own commit or a different
 * writer's.
 *
 * <p>The client must verify its staged commit against the backfilled {@code <version>.json} on the
 * filesystem before rebasing; a content match means the commit already landed and must not be
 * re-committed (otherwise a lost-ACK retry double-commits the data at version+1).
 *
 * <p>Extends {@link CommitFailedException} (retryable + conflict) on purpose: it must be caught by
 * the commit-coordinator retry loop, where the filesystem verification lives.
 */
public class CommitCompletionUnknownException extends CommitFailedException {
  public CommitCompletionUnknownException(String message) {
    super(true /* retryable */, true /* conflict */, message);
  }
}
