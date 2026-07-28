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

package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.commit.CommitFailedException;

/**
 * Exception thrown when the Delta transaction commit system cannot determine whether a previous
 * commit attempt succeeded or failed, making it unsafe to continue with automatic retries.
 *
 * <p>This exception occurs in a specific sequence during transaction commit retries:
 *
 * <ol>
 *   <li>First commit attempt fails with a retryable, non-conflict exception (e.g., IOException)
 *   <li>Second commit attempt fails with a conflict exception (e.g., FileAlreadyExistsException)
 * </ol>
 *
 * <p>In this scenario, the system cannot determine whether the first attempt actually wrote the
 * commit file successfully but failed to report success, or whether the commit file was never
 * written.
 *
 * <p>Since the system cannot distinguish between these cases, it cannot safely determine whether to
 * retry at the current version or advance to version N+1.
 *
 * <p>Resolution: When this exception occurs, manual intervention is required to:
 *
 * <ul>
 *   <li>Examine the commit history to determine if the first attempt actually succeeded
 *   <li>If the commit succeeded, avoid retrying to prevent duplicate records
 *   <li>If the commit failed, retry the operation from the beginning
 * </ul>
 */
@Evolving
public class CommitStateUnknownException extends RuntimeException {
  public CommitStateUnknownException(
      long commitVersion, int commitAttempt, CommitFailedException cfe) {
    super(
        String.format(
            "Commit attempt %d for version %d failed due to a concurrent write conflict after a "
                + "previous retry. Since Kernel cannot determine if that previous attempt actually "
                + "succeeded, retrying could create duplicate records. Please manually validate "
                + "the commit history to resolve this conflict and retry the operation.",
            commitAttempt, commitVersion),
        cfe);
  }
}
