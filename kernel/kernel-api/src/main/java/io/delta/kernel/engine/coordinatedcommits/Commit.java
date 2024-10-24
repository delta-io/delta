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
import io.delta.kernel.utils.FileStatus;

/**
 * Representation of a commit file. It contains the version of the commit, the file status of the
 * commit, and the timestamp of the commit. This is used when we want to get the commit information
 * from the {@link io.delta.kernel.engine.CommitCoordinatorClientHandler#commit} and {@link
 * io.delta.kernel.engine.CommitCoordinatorClientHandler#getCommits} APIs.
 *
 * @since 3.3.0
 */
@Evolving
public class Commit {

  private final long version;

  private final FileStatus fileStatus;

  private final long timestamp;

  public Commit(long version, FileStatus fileStatus, long timestamp) {
    this.version = version;
    this.fileStatus = fileStatus;
    this.timestamp = timestamp;
  }

  /**
   * Get the version of the commit.
   *
   * @return the version of the commit.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Get the file status of the commit.
   *
   * @return the file status of the commit.
   */
  public FileStatus getFileStatus() {
    return fileStatus;
  }

  /**
   * Get the timestamp that represents the time since epoch in milliseconds when the commit write
   * was started.
   *
   * @return the timestamp of the commit.
   */
  public long getTimestamp() {
    return timestamp;
  }
}
