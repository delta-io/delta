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
package io.delta.kernel.metrics;

/** Stores the file size histogram information to track file size distribution and their counts. */
public interface FileSizeHistogramResult {

  /**
   * Sorted list of bin boundaries where each element represents the start of the bin (inclusive)
   * and the next element represents the end of the bin (exclusive).
   */
  long[] getSortedBinBoundaries();

  /**
   * The total number of files in each bin of {@link
   * FileSizeHistogramResult#getSortedBinBoundaries()}
   */
  long[] getFileCounts();

  /**
   * The total number of bytes in each bin of {@link
   * FileSizeHistogramResult#getSortedBinBoundaries()}
   */
  long[] getTotalBytes();
}
