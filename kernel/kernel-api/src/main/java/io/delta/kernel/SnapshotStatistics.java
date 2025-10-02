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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;

/**
 * Statistics about a Delta snapshot, providing information about the state of staged commits and
 * publishing status.
 *
 * @since 3.4.0
 */
@Evolving
public interface SnapshotStatistics {

  /**
   * Returns the number of ratified commits that are available for publishing. These are commits
   * that have been ratified by the commit coordinator but have not yet been published to their
   * final locations.
   *
   * @return The number of ratified commits available for publishing
   */
  long getNumRatifiedCommits();

  /**
   * Returns true if there are commits that should be published. This is true when there are
   * ratified commits available for publishing.
   *
   * @return true if publishing should be performed, false otherwise
   */
  boolean shouldPublish();
}
