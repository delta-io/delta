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

import org.apache.hadoop.fs.FileStatus;

/**
 * Representation of a commit file
 */
public class Commit {

  private long version;

  private FileStatus fileStatus;

  private long commitTimestamp;

  public Commit(long version, FileStatus fileStatus, long commitTimestamp) {
    this.version = version;
    this.fileStatus = fileStatus;
    this.commitTimestamp = commitTimestamp;
  }

  public long getVersion() {
    return version;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  public Commit withFileStatus(FileStatus fileStatus) {
    return new Commit(version, fileStatus, commitTimestamp);
  }

  public Commit withCommitTimestamp(long commitTimestamp) {
      return new Commit(version, fileStatus, commitTimestamp);
  }
}
