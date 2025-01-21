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

package io.delta.storage.commit.actions;

/**
 * Interface for objects that represents the base information for a commit.
 * Commits need to provide an in-commit timestamp. This timestamp is used
 * to specify the exact time the commit happened and determines the target
 * version for time-based time travel queries.
 */
public interface AbstractCommitInfo {

  /**
   * Get the timestamp of the commit as millis after the epoch.
   */
  long getCommitTimestamp();
}
