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
 * This exception is thrown by the UC client in case UC has reached the maximum
 * number of commits that it is allowed to track (50 by default). Upon receiving
 * this exception, the client should run a backfill.
 */
public class CommitLimitReachedException extends UCCommitCoordinatorException {
  public CommitLimitReachedException(String message) {
    super(message);
  }
}
