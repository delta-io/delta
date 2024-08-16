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

package io.delta.kernel.engine.coordinatedcommits;

import io.delta.kernel.annotation.Evolving;
import java.util.List;
import java.util.Map;

/**
 * Response container for {@link io.delta.kernel.engine.CommitCoordinatorClientHandler#getCommits(
 * String, Map, Long, Long)}. Holds all the commits that have not been backfilled as per the commit
 * coordinator.
 *
 * @since 3.3.0
 */
@Evolving
public class GetCommitsResponse {
  private final List<Commit> commits;

  private final long latestTableVersion;

  public GetCommitsResponse(List<Commit> commits, long latestTableVersion) {
    this.commits = commits;
    this.latestTableVersion = latestTableVersion;
  }

  /**
   * Get the list of commits that have not been backfilled as per the commit coordinator. It is
   * possible that some of these commits have been physically backfilled but the commit coordinator
   * is not aware of this.
   *
   * @return the list of commits.
   */
  public List<Commit> getCommits() {
    return commits;
  }

  /**
   * Get the latest table version as per the coordinator. This can be -1 when no commit has gone
   * through the coordinator even though the actual table has a non-negative version.
   *
   * @return the latest table version.
   */
  public long getLatestTableVersion() {
    return latestTableVersion;
  }
}
