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
import io.delta.kernel.internal.util.InternalUtils;
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

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("sortedBinBoundaries", new ArrayType(LongType.LONG, false))
          .add("fileCounts", new ArrayType(LongType.LONG, false))
          .add("totalBytes", new ArrayType(LongType.LONG, false));

  /** Creates a default FileSizeHistogram with predefined bin boundaries and zero counts. */
  public static FileSizeHistogram createDefaultHistogram() {
    long[] defaultBoundaries = createDefaultBinBoundaries();
    long[] zeroCounts = new long[defaultBoundaries.length];
    long[] zeroBytes = new long[defaultBoundaries.length];
    return new FileSizeHistogram(defaultBoundaries, zeroCounts, zeroBytes);
  }

  /** Creates a FileSizeHistogram from a column vector. */
  public static Optional<FileSizeHistogram> fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return Optional.empty();
    }

    int boundariesIdx = FULL_SCHEMA.indexOf("sortedBinBoundaries");
    int totalBytesIdx = FULL_SCHEMA.indexOf("totalBytes");
    int fileCountsIdx = FULL_SCHEMA.indexOf("fileCounts");

    List<Long> boundariesList =
        VectorUtils.toJavaList(
            InternalUtils.requireNonNull(
                    vector.getChild(boundariesIdx), rowId, "sortedBinBoundaries")
                .getArray(rowId));
    List<Long> totalBytesList =
        VectorUtils.toJavaList(
            InternalUtils.requireNonNull(vector.getChild(totalBytesIdx), rowId, "totalBytes")
                .getArray(rowId));
    List<Long> fileCountsList =
        VectorUtils.toJavaList(
            InternalUtils.requireNonNull(vector.getChild(fileCountsIdx), rowId, "fileCounts")
                .getArray(rowId));

    long[] boundaries = boundariesList.stream().mapToLong(Long::longValue).toArray();
    long[] totalBytesArray = totalBytesList.stream().mapToLong(Long::longValue).toArray();
    long[] fileCountsArray = fileCountsList.stream().mapToLong(Long::longValue).toArray();

    return Optional.of(new FileSizeHistogram(boundaries, fileCountsArray, totalBytesArray));
  }

  /**
   * Creates the default bin boundaries for file size categorization.
   *
   * <ul>
   *   <li>Starts with 0 and powers of 2 from 8KB to 4MB
   *   <li>4MB jumps from 8MB to 40MB
   *   <li>8MB jumps from 48MB to 120MB
   *   <li>4MB jumps from 124MB to 144MB
   *   <li>16MB jumps from 160MB to 576MB
   *   <li>64MB jumps from 640MB to 1408MB
   *   <li>128MB jumps from 1536MB to 2GB
   *   <li>256MB jumps from 2304MB to 4GB
   *   <li>Powers of 2 from 8GB to 256GB
   * </ul>
   *
   * @return An array of bin boundaries sorted in ascending order
   */
  private static long[] createDefaultBinBoundaries() {
    // Pre-calculate the size to avoid resizing
    int totalSize = 95; // Known size from all the boundaries
    long[] boundaries = new long[totalSize];
    int idx = 0;

    // 0 and powers of 2 till 4 MB
    boundaries[idx++] = 0L;
    for (long size = 8 * KB; size <= 4 * MB; size *= 2) {
      boundaries[idx++] = size;
    }

    // 4 MB jumps till 40 MB
    for (long size = 8 * MB; size <= 40 * MB; size += 4 * MB) {
      boundaries[idx++] = size;
    }

    // 8 MB jumps till 120 MB
    for (long size = 48 * MB; size <= 120 * MB; size += 8 * MB) {
      boundaries[idx++] = size;
    }

    // 4 MB jumps till 144 MB
    for (long size = 124 * MB; size <= 144 * MB; size += 4 * MB) {
      boundaries[idx++] = size;
    }

    // 16 MB jumps till 576 MB
    for (long size = 160 * MB; size <= 576 * MB; size += 16 * MB) {
      boundaries[idx++] = size;
    }

    // 64 MB jumps till 1408 MB
    for (long size = 640 * MB; size <= 1408 * MB; size += 64 * MB) {
      boundaries[idx++] = size;
    }

    // 128 MB jumps till 2 GB
    for (long size = 1536 * MB; size <= 2048 * MB; size += 128 * MB) {
      boundaries[idx++] = size;
    }

    // 256 MB jumps till 4 GB
    for (long size = 2304 * MB; size <= 4096 * MB; size += 256 * MB) {
      boundaries[idx++] = size;
    }

    // Power of 2 till 256 GB
    for (long size = 8 * GB; size <= 256 * GB; size *= 2) {
      boundaries[idx++] = size;
    }

    checkArgument(
        idx == totalSize, "Incorrect pre-calculated size. Expected %s but got %s", totalSize, idx);
    return boundaries;
  }

  ////////////////////////////////////
  // Member variables and methods  //
  ////////////////////////////////////

  private final long[] sortedBinBoundaries;
  private final long[] fileCounts;
  private final long[] totalBytes;

  private FileSizeHistogram(long[] sortedBinBoundaries, long[] fileCounts, long[] totalBytes) {
    requireNonNull(sortedBinBoundaries, "sortedBinBoundaries cannot be null");
    requireNonNull(fileCounts, "fileCounts cannot be null");
    requireNonNull(totalBytes, "totalBytes cannot be null");

    checkArgument(
        sortedBinBoundaries.length >= 2,
        "sortedBinBoundaries must have at least 2 elements to define a range");
    checkArgument(
        sortedBinBoundaries[0] == 0, "First boundary must be 0, got %s", sortedBinBoundaries[0]);
    checkArgument(
        sortedBinBoundaries.length == fileCounts.length
            && sortedBinBoundaries.length == totalBytes.length,
        "All arrays must have the same length");

    this.sortedBinBoundaries = sortedBinBoundaries;
    this.fileCounts = fileCounts;
    this.totalBytes = totalBytes;
  }

  /**
   * Adds a file size to the histogram, incrementing the appropriate bin's count and total bytes.
   * The appropriate bin refers to a bin with boundary that is less than or equal to the file size.
   * Files larger than the maximum bin boundary (256 GB) are placed in the last bin.
   *
   * @param fileSize The size of the file in bytes
   * @throws IllegalArgumentException if fileSize is negative or if getBinIndex returns an invalid
   *     index
   */
  public void insert(long fileSize) {
    checkArgument(fileSize >= 0, "File size must be non-negative, got %s", fileSize);
    int index = getBinIndex(fileSize);
    checkArgument(
        index >= 0,
        "getBinIndex must return non-negative index for non-negative fileSize, got %s",
        index);
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
    checkArgument(
        index >= 0,
        "getBinIndex must return non-negative index for non-negative fileSize, got %s",
        index);
    checkArgument(
        totalBytes[index] >= fileSize && fileCounts[index] > 0,
        "Cannot remove %s bytes from bin %d which only has %s bytes or does not have any files",
        fileSize,
        index,
        totalBytes[index]);
    fileCounts[index]--;
    totalBytes[index] -= fileSize;
  }

  private int getBinIndex(long fileSize) {
    int index = Arrays.binarySearch(sortedBinBoundaries, fileSize);
    // When fileSize is not found in the array, binarySearch returns -(insertion_point) - 1
    // We need to get the bin that comes before the insertion point, which is (insertion_point - 1)
    return index >= 0 ? index : -(index + 1) - 1;
  }

  /** Encode as a {@link Row} object with the schema {@link FileSizeHistogram#FULL_SCHEMA}. */
  public Row toRow() {
    Map<Integer, Object> value = new HashMap<>();
    value.put(
        FULL_SCHEMA.indexOf("sortedBinBoundaries"),
        VectorUtils.buildArrayValue(
            Arrays.stream(sortedBinBoundaries).boxed().collect(Collectors.toList()),
            LongType.LONG));
    value.put(
        FULL_SCHEMA.indexOf("fileCounts"),
        VectorUtils.buildArrayValue(
            Arrays.stream(fileCounts).boxed().collect(Collectors.toList()), LongType.LONG));
    value.put(
        FULL_SCHEMA.indexOf("totalBytes"),
        VectorUtils.buildArrayValue(
            Arrays.stream(totalBytes).boxed().collect(Collectors.toList()), LongType.LONG));
    return new GenericRow(FULL_SCHEMA, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FileSizeHistogram that = (FileSizeHistogram) o;
    return Arrays.equals(sortedBinBoundaries, that.sortedBinBoundaries)
        && Arrays.equals(fileCounts, that.fileCounts)
        && Arrays.equals(totalBytes, that.totalBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        Arrays.hashCode(sortedBinBoundaries),
        Arrays.hashCode(fileCounts),
        Arrays.hashCode(totalBytes));
  }
}
