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

@Evolving
public class CommitStateUnknownException extends ConcurrentWriteException {
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
