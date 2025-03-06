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
package io.delta.kernel.internal.stats;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.*;

/** A histogram that tracks file size distributions and their counts. */
public class FileSizeHistogram {
  private static final long KB = 1024;
  private static final long MB = KB * 1024;
  private static final long GB = MB * 1024;

  private static final List<Long> DEFAULT_BIN_BOUNDARIES = createDefaultBinBoundaries();

  private final List<Long> sortedBinBoundaries;
  private final Long[] fileCounts;
  private final Long[] totalBytes;

  // A sorted array of bin boundaries where each element represents the start of a bin (inclusive)
  // and the next element represents the end of the bin (exclusive). The first element must be 0.
  private static List<Long> createDefaultBinBoundaries() {
    return Arrays.asList(
        0L,
        // Power of 2 till 4 MB
        8 * KB,
        16 * KB,
        32 * KB,
        64 * KB,
        128 * KB,
        256 * KB,
        512 * KB,
        1 * MB,
        2 * MB,
        4 * MB,
        // 4 MB jumps till 40 MB
        8 * MB,
        12 * MB,
        16 * MB,
        20 * MB,
        24 * MB,
        28 * MB,
        32 * MB,
        36 * MB,
        40 * MB,
        // 8 MB jumps till 120 MB
        48 * MB,
        56 * MB,
        64 * MB,
        72 * MB,
        80 * MB,
        88 * MB,
        96 * MB,
        104 * MB,
        112 * MB,
        120 * MB,
        // 4 MB jumps till 144 MB
        124 * MB,
        128 * MB,
        132 * MB,
        136 * MB,
        140 * MB,
        144 * MB,
        // 16 MB jumps till 576 MB
        160 * MB,
        176 * MB,
        192 * MB,
        208 * MB,
        224 * MB,
        240 * MB,
        256 * MB,
        272 * MB,
        288 * MB,
        304 * MB,
        320 * MB,
        336 * MB,
        352 * MB,
        368 * MB,
        384 * MB,
        400 * MB,
        416 * MB,
        432 * MB,
        448 * MB,
        464 * MB,
        480 * MB,
        496 * MB,
        512 * MB,
        528 * MB,
        544 * MB,
        560 * MB,
        576 * MB,
        // 64 MB jumps till 1408 MB
        640 * MB,
        704 * MB,
        768 * MB,
        832 * MB,
        896 * MB,
        960 * MB,
        1024 * MB,
        1088 * MB,
        1152 * MB,
        1216 * MB,
        1280 * MB,
        1344 * MB,
        1408 * MB,
        // 128 MB jumps till 2 GB
        1536 * MB,
        1664 * MB,
        1792 * MB,
        1920 * MB,
        2048 * MB,
        // 256 MB jumps till 4 GB
        2304 * MB,
        2560 * MB,
        2816 * MB,
        3072 * MB,
        3328 * MB,
        3584 * MB,
        3840 * MB,
        4 * GB,
        // power of 2 till 256 GB
        8 * GB,
        16 * GB,
        32 * GB,
        64 * GB,
        128 * GB,
        256 * GB);
  }

  public static FileSizeHistogram init() {
    return new FileSizeHistogram(
        DEFAULT_BIN_BOUNDARIES,
        initializeZeroArray(DEFAULT_BIN_BOUNDARIES.size()),
        initializeZeroArray(DEFAULT_BIN_BOUNDARIES.size()));
  }

  /**
   * Creates a histogram with custom boundaries and initial counts.
   *
   * @param sortedBinBoundaries List of boundary values in ascending order
   * @param fileCounts Array of file counts for each bin
   * @param totalBytes Array of total bytes for each bin
   * @throws IllegalArgumentException if arrays have different lengths
   */
  public FileSizeHistogram(List<Long> sortedBinBoundaries, Long[] fileCounts, Long[] totalBytes) {
    this.sortedBinBoundaries =
        new ArrayList<>(requireNonNull(sortedBinBoundaries, "sortedBinBoundaries cannot be null"));
    this.fileCounts =
        Arrays.copyOf(requireNonNull(fileCounts, "fileCounts cannot be null"), fileCounts.length);
    this.totalBytes =
        Arrays.copyOf(requireNonNull(totalBytes, "totalBytes cannot be null"), totalBytes.length);

    if (sortedBinBoundaries.size() != fileCounts.length
        || sortedBinBoundaries.size() != totalBytes.length) {
      throw new IllegalArgumentException("All arrays must have the same length");
    }
  }

  /**
   * Adds a file size to the histogram, incrementing the appropriate bin's count and total bytes.
   *
   * @param fileSize The size of the file in bytes
   * @throws IllegalArgumentException if fileSize is negative
   */
  public void insert(long fileSize) {
    checkArgument(fileSize >= 0, "File size must be non-negative, got %s", fileSize);
    int index = getBinIndex(fileSize);
    if (index >= 0) {
      fileCounts[index]++;
      totalBytes[index] += fileSize;
    }
  }

  /**
   * Removes a file size from the histogram, decrementing the appropriate bin's count and total
   * bytes.
   *
   * @param fileSize The size of the file in bytes
   * @throws IllegalArgumentException if fileSize is negative
   */
  public void remove(long fileSize) {
    checkArgument(fileSize >= 0, "File size must be non-negative, got %s", fileSize);
    int index = getBinIndex(fileSize);
    if (index >= 0) {
      checkArgument(
          totalBytes[index] >= fileSize,
          "Cannot remove %s bytes from bin %d which only has %s bytes",
          fileSize,
          index,
          totalBytes[index]);
      fileCounts[index]--;
      totalBytes[index] -= fileSize;
    }
  }

  private int getBinIndex(long fileSize) {
    int searchResult = Collections.binarySearch(sortedBinBoundaries, fileSize);
    return searchResult >= 0 ? searchResult : -(searchResult + 1) - 1;
  }

  private static Long[] initializeZeroArray(int size) {
    Long[] array = new Long[size];
    Arrays.fill(array, 0L);
    return array;
  }
}
