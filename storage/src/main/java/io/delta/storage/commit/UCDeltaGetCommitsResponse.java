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

package io.delta.storage.commit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import io.delta.storage.commit.uniform.UniformMetadata;

/**
 * Extended response for {@link CommitCoordinatorClient#getCommits} implementations backed by the
 * Delta REST loadTable API.
 * Currently, it carries additional {@link UniformMetadata} returned
 * by the loadTable response alongside the standard commits and latest table version.
 */
public class UCDeltaGetCommitsResponse extends GetCommitsResponse {

  private final Optional<UniformMetadata> uniformMetadata;

  public UCDeltaGetCommitsResponse(
      List<Commit> commits,
      long latestTableVersion,
      Optional<UniformMetadata> uniformMetadata) {
    super(commits, latestTableVersion);
    this.uniformMetadata = uniformMetadata;
  }

  public Optional<UniformMetadata> getUniformMetadata() {
    return uniformMetadata;
  }

  @Override
  public UCDeltaGetCommitsResponse sortCommitsByVersion() {
    List<Commit> sorted = new ArrayList<>(getCommits());
    sorted.sort(Comparator.comparingLong(Commit::getVersion));
    return new UCDeltaGetCommitsResponse(sorted, getLatestTableVersion(), uniformMetadata);
  }
}
