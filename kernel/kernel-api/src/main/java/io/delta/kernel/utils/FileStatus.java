/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.utils;

import io.delta.kernel.annotation.Evolving;
import java.util.Objects;

/**
 * Class for encapsulating metadata about a file in Delta Lake table.
 *
 * @since 3.0.0
 */
@Evolving
public class FileStatus {
  private final String path;
  private final long size;
  private final long modificationTime;

  // TODO add further documentation about the expected format for modificationTime?
  protected FileStatus(String path, long size, long modificationTime) {
    this.path = Objects.requireNonNull(path, "path is null");
    this.size = size; // TODO: validation
    this.modificationTime = modificationTime; // TODO: validation
  }

  /**
   * Get the path to the file.
   *
   * @return Fully qualified file path
   */
  public String getPath() {
    return path;
  }

  /**
   * Get the size of the file in bytes.
   *
   * @return File size in bytes.
   */
  public long getSize() {
    return size;
  }

  /**
   * Get the modification time of the file in epoch millis.
   *
   * @return Modification time in epoch millis
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Create a {@link FileStatus} with the given path, size and modification time.
   *
   * @param path Fully qualified file path.
   * @param size File size in bytes
   * @param modificationTime Modification time of the file in epoch millis
   */
  public static FileStatus of(String path, long size, long modificationTime) {
    return new FileStatus(path, size, modificationTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileStatus that = (FileStatus) o;
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.size, that.size)
        && Objects.equals(this.modificationTime, that.modificationTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, size, modificationTime);
  }
}
