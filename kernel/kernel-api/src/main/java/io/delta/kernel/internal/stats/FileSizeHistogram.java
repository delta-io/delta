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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.*;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class FileSizeHistogram {

  private static final String SORTED_BIN_BOUNDARIES = "sortedBinBoundaries";
  private static final String FILE_COUNTS = "fileCounts";
  private static final String TOTAL_BYTES = "totalbytes";

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add(SORTED_BIN_BOUNDARIES, new ArrayType(LongType.LONG, false))
          .add(FILE_COUNTS, new ArrayType(LongType.LONG, false))
          .add(TOTAL_BYTES, new ArrayType(LongType.LONG, false));

  private final Long[] fileCounts;
  private final Long[] totalBytes;
  private final List<Long> sortedBinBoundary;

  // Factory methods
  public static FileSizeHistogram init(List<Long> sortedBinBoundary) {
    return new FileSizeHistogram(
        sortedBinBoundary,
        createZeroArray(sortedBinBoundary.size()),
        createZeroArray(sortedBinBoundary.size()));
  }

  public static Optional<FileSizeHistogram> fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return Optional.empty();
    }

    List<Long> boundaries =
        VectorUtils.toJavaList(
            vector.getChild(getSchemaIndex(SORTED_BIN_BOUNDARIES)).getArray(rowId));
    Long[] bytes =
        VectorUtils.toJavaList(vector.getChild(getSchemaIndex(TOTAL_BYTES)).getArray(rowId))
            .toArray(new Long[boundaries.size()]);
    Long[] counts =
        VectorUtils.toJavaList(vector.getChild(getSchemaIndex(FILE_COUNTS)).getArray(rowId))
            .toArray(new Long[boundaries.size()]);

    return Optional.of(new FileSizeHistogram(boundaries, counts, bytes));
  }

  // Constructor
  private FileSizeHistogram(List<Long> sortedBinBoundary, Long[] fileCounts, Long[] totalBytes) {
    this.sortedBinBoundary = sortedBinBoundary;
    this.fileCounts = fileCounts;
    this.totalBytes = totalBytes;
  }

  // Public methods
  public void insert(Long fileSize) {
    int index = findBinIndex(fileSize);
    if (index >= 0) {
      fileCounts[index] += 1;
      totalBytes[index] += fileSize;
    }
  }

  public void remove(Long fileSize) {
    int index = findBinIndex(fileSize);
    if (index >= 0) {
      fileCounts[index] -= 1;
      checkArgument(totalBytes[index] > fileSize);
      totalBytes[index] -= fileSize;
    }
  }

  public Row toRow() {
    Map<Integer, Object> values = new HashMap<>();
    values.put(
        getSchemaIndex(SORTED_BIN_BOUNDARIES), VectorUtils.longArrayValue(sortedBinBoundary));
    values.put(getSchemaIndex(FILE_COUNTS), VectorUtils.longArrayValue(Arrays.asList(fileCounts)));
    values.put(getSchemaIndex(TOTAL_BYTES), VectorUtils.longArrayValue(Arrays.asList(totalBytes)));
    return new GenericRow(FULL_SCHEMA, values);
  }

  private int findBinIndex(long fileSize) {
    int searchResult = Collections.binarySearch(sortedBinBoundary, fileSize);
    return searchResult >= 0 ? searchResult : -(searchResult + 1);
  }

  private static Long[] createZeroArray(int size) {
    Long[] array = new Long[size];
    Arrays.fill(array, 0L);
    return array;
  }

  private static int getSchemaIndex(String fieldName) {
    return FULL_SCHEMA.indexOf(fieldName);
  }
}
