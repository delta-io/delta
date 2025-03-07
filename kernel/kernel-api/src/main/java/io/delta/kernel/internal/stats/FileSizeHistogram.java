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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

/** A histogram that tracks file size distributions and their counts. */
public class FileSizeHistogram {

  //////////////////////////////////
  // Static variables and methods //
  //////////////////////////////////

  private static final long KB = 1024;
  private static final long MB = KB * 1024;
  private static final long GB = MB * 1024;

  private static final List<Long> DEFAULT_BIN_BOUNDARIES = createDefaultBinBoundaries();

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("sortedBinBoundaries", new ArrayType(LongType.LONG, false))
          .add("fileCounts", new ArrayType(LongType.LONG, false))
          .add("totalBytes", new ArrayType(LongType.LONG, false));

  /**
   * Creates default bin boundaries as specified by the Delta Lake specification. The boundaries
   * are: - 0 and powers of 2 till 4 MB - 4 MB jumps till 40 MB - 8 MB jumps till 120 MB - 4 MB
   * jumps till 144 MB - 16 MB jumps till 576 MB - 64 MB jumps till 1408 MB - 128 MB jumps till 2 GB
   * - 256 MB jumps till 4 GB - power of 2 till 256 GB
   */
  private static List<Long> createDefaultBinBoundaries() {
    List<Long> sizes = new ArrayList<>();

    // 0 and powers of 2 till 4 MB
    sizes.add(0L);
    for (long size = 8 * KB; size <= 4 * MB; size *= 2) {
      sizes.add(size);
    }

    // 4 MB jumps till 40 MB
    for (long size = 8 * MB; size <= 40 * MB; size += 4 * MB) {
      sizes.add(size);
    }

    // 8 MB jumps till 120 MB
    for (long size = 48 * MB; size <= 120 * MB; size += 8 * MB) {
      sizes.add(size);
    }

    // 4 MB jumps till 144 MB
    for (long size = 124 * MB; size <= 144 * MB; size += 4 * MB) {
      sizes.add(size);
    }

    // 16 MB jumps till 576 MB
    for (long size = 160 * MB; size <= 576 * MB; size += 16 * MB) {
      sizes.add(size);
    }

    // 64 MB jumps till 1408 MB
    for (long size = 640 * MB; size <= 1408 * MB; size += 64 * MB) {
      sizes.add(size);
    }

    // 128 MB jumps till 2 GB
    for (long size = 1536 * MB; size <= 2048 * MB; size += 128 * MB) {
      sizes.add(size);
    }

    // 256 MB jumps till 4 GB
    for (long size = 2304 * MB; size <= 4096 * MB; size += 256 * MB) {
      sizes.add(size);
    }

    // Power of 2 till 256 GB
    for (long size = 8 * GB; size <= 256 * GB; size *= 2) {
      sizes.add(size);
    }

    return sizes;
  }

  public static FileSizeHistogram createDefaultHistogram() {
    return new FileSizeHistogram(
        DEFAULT_BIN_BOUNDARIES,
        initializeZeroArray(DEFAULT_BIN_BOUNDARIES.size()),
        initializeZeroArray(DEFAULT_BIN_BOUNDARIES.size()));
  }

  public static Optional<FileSizeHistogram> fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return Optional.empty();
    }
    List<Long> sortedBinBoundaries =
        VectorUtils.toJavaList(
            vector.getChild(FULL_SCHEMA.indexOf("sortedBinBoundaries")).getArray(rowId));
    Long[] totalBytes =
        VectorUtils.toJavaList(vector.getChild(FULL_SCHEMA.indexOf("totalBytes")).getArray(rowId))
            .toArray(new Long[sortedBinBoundaries.size()]);
    Long[] fileCounts =
        VectorUtils.toJavaList(vector.getChild(FULL_SCHEMA.indexOf("fileCounts")).getArray(rowId))
            .toArray(new Long[sortedBinBoundaries.size()]);
    return Optional.of(new FileSizeHistogram(sortedBinBoundaries, fileCounts, totalBytes));
  }

  private static Long[] initializeZeroArray(int size) {
    Long[] array = new Long[size];
    Arrays.fill(array, 0L);
    return array;
  }

  ////////////////////////////////////
  // Member variables and methods  //
  ////////////////////////////////////

  private final List<Long> sortedBinBoundaries;
  private final Long[] fileCounts;
  private final Long[] totalBytes;

  /**
   * Creates a histogram with custom boundaries and initial counts.
   *
   * @param sortedBinBoundaries List of boundary values in ascending order
   * @param fileCounts Array of file counts for each bin
   * @param totalBytes Array of total bytes for each bin
   * @throws IllegalArgumentException if arrays have different lengths
   */
  private FileSizeHistogram(List<Long> sortedBinBoundaries, Long[] fileCounts, Long[] totalBytes) {
    this.sortedBinBoundaries =
        new ArrayList<>(requireNonNull(sortedBinBoundaries, "sortedBinBoundaries cannot be null"));
    this.fileCounts =
        Arrays.copyOf(requireNonNull(fileCounts, "fileCounts cannot be null"), fileCounts.length);
    this.totalBytes =
        Arrays.copyOf(requireNonNull(totalBytes, "totalBytes cannot be null"), totalBytes.length);

    checkArgument(sortedBinBoundaries.size() >= 2,
            "sortedBinBoundaries must have at least 2 elements to define a range");
    checkArgument(sortedBinBoundaries.get(0) == 0,
            "First boundary must be 0, got %s", sortedBinBoundaries.get(0));
    checkArgument(sortedBinBoundaries.size() == fileCounts.length
                    && sortedBinBoundaries.size() == totalBytes.length,
            "All arrays must have the same length");
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
    checkArgument(index >= 0,
            "getBinIndex must return non-negative index for non-negative fileSize, got %s", index);
    fileCounts[index]++;
    totalBytes[index] += fileSize;
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
    checkArgument(index >= 0,
            "getBinIndex must return non-negative index for non-negative fileSize, got %s", index);
    checkArgument(
            totalBytes[index] >= fileSize,
            "Cannot remove %s bytes from bin %d which only has %s bytes",
            fileSize,
            index,
            totalBytes[index]);
    fileCounts[index]--;
    totalBytes[index] -= fileSize;
  }

  /**
   * Encode as a {@link Row} object with the schema {@link FileSizeHistogram#FULL_SCHEMA}.
   *
   * @return {@link Row} object with the schema {@link FileSizeHistogram#FULL_SCHEMA}
   */
  public Row toRow() {
    Map<Integer, Object> value = new HashMap<>();
    value.put(
        FULL_SCHEMA.indexOf("sortedBinBoundaries"),
        VectorUtils.buildArrayValue(sortedBinBoundaries, LongType.LONG));
    value.put(
        FULL_SCHEMA.indexOf("fileCounts"),
        VectorUtils.buildArrayValue(
            Arrays.stream(fileCounts).collect(Collectors.toList()), LongType.LONG));
    value.put(
        FULL_SCHEMA.indexOf("totalBytes"),
        VectorUtils.buildArrayValue(
            Arrays.stream(totalBytes).collect(Collectors.toList()), LongType.LONG));
    return new GenericRow(FULL_SCHEMA, value);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;

    FileSizeHistogram that = (FileSizeHistogram) o;
    return sortedBinBoundaries.equals(that.sortedBinBoundaries)
        && Arrays.equals(fileCounts, that.fileCounts)
        && Arrays.equals(totalBytes, that.totalBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sortedBinBoundaries, Arrays.hashCode(fileCounts), Arrays.hashCode(totalBytes));
  }

  private int getBinIndex(long fileSize) {
    int searchResult = Collections.binarySearch(sortedBinBoundaries, fileSize);
    return searchResult >= 0 ? searchResult : -(searchResult + 1) - 1;
  }
}
