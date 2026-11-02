/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.utils.FileStatus;

/** Immutable metadata for one commit JSON file in a {@link CommitRange}. */
@Evolving
public final class CommitFileStatus {
  private final long version;
  private final FileStatus fileStatus;

  public CommitFileStatus(long version, FileStatus fileStatus) {
    this.version = version;
    this.fileStatus = requireNonNull(fileStatus, "fileStatus is null");
  }

  /** Returns the commit version represented by the file. */
  public long getVersion() {
    return version;
  }

  /** Returns the commit file metadata. */
  public FileStatus getFileStatus() {
    return fileStatus;
  }
}
