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
package io.delta.kernel.internal.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A metric that tracks both the number of files and their total size. The size is passed in when
 * incrementing the counter.
 */
public class FileCounter {

  private final LongAdder fileCount = new LongAdder();
  private final LongAdder totalSize = new LongAdder();

  /**
   * Increment the file count by 1 and add the file size.
   *
   * @param size The size of the file in bytes.
   */
  public void increment(long size) {
    fileCount.increment();
    totalSize.add(size);
  }

  /**
   * Reports the current file count.
   *
   * @return The current file count.
   */
  public long count() {
    return fileCount.longValue();
  }

  /**
   * Reports the current total size.
   *
   * @return The current total size in bytes.
   */
  public long size() {
    return totalSize.longValue();
  }

  /** Resets both the file count and total size to 0. */
  public void reset() {
    fileCount.reset();
    totalSize.reset();
  }

  @Override
  public String toString() {
    return String.format(
        "FileCount(count=%s, size=%s)", fileCount.longValue(), totalSize.longValue());
  }
}
