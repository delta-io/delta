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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.delta.storage.commit.uniform.UniformMetadata;
import org.apache.hadoop.fs.Path;

/**
 * Response container for
 * {@link CommitCoordinatorClient#getCommits(TableDescriptor, Long, Long)}.
 */
public class GetCommitsResponse {

  private List<Commit> commits;

  private long latestTableVersion;

  private Optional<UniformMetadata> uniformMetadata;

  public GetCommitsResponse(List<Commit> commits, long latestTableVersion) {
    this(commits, latestTableVersion, Optional.empty());
  }

  public GetCommitsResponse(
      List<Commit> commits,
      long latestTableVersion,
      Optional<UniformMetadata> uniformMetadata) {
    this.commits = commits;
    this.latestTableVersion = latestTableVersion;
    this.uniformMetadata = uniformMetadata;
  }

  public List<Commit> getCommits() {
    return commits;
  }

  public long getLatestTableVersion() {
    return latestTableVersion;
  }

  public Optional<UniformMetadata> getUniformMetadata() {
    return uniformMetadata;
  }
}

