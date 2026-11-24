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
package io.delta.kernel.internal.stats;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Counts files by DV cardinality, including files without a DV in the zero bucket. The ten fixed
 * buckets match Spark's deletedRecordCountsHistogramOpt CRC field.
 */
public class DeletedRecordCountsHistogram {
  private static final long[] LOWER_BOUNDS = {
    0, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, Integer.MAX_VALUE
  };
  public static final StructType FULL_SCHEMA =
      new StructType().add("deletedRecordCounts", new ArrayType(LongType.LONG, false));

  private final long[] counts;

  public DeletedRecordCountsHistogram(long[] counts) {
    checkArgument(counts.length == LOWER_BOUNDS.length, "Expected ten histogram buckets");
    this.counts = counts.clone();
  }

  public static DeletedRecordCountsHistogram empty() {
    return new DeletedRecordCountsHistogram(new long[LOWER_BOUNDS.length]);
  }

  public long[] getCounts() {
    return counts.clone();
  }

  public static Optional<DeletedRecordCountsHistogram> fromColumnVector(
      ColumnVector vector, int rowId) {
    return vector.isNullAt(rowId)
        ? Optional.empty()
        : Optional.of(fromRow(StructRow.fromStructVector(vector, rowId)));
  }

  public static DeletedRecordCountsHistogram fromRow(Row row) {
    return new DeletedRecordCountsHistogram(
        VectorUtils.<Long>toJavaList(row.getArray(/* ordinal */ 0)).stream()
            .mapToLong(Long::longValue)
            .toArray());
  }

  public void applyDvChange(long cardinality, long fileCount) {
    checkArgument(cardinality >= 0, "DV cardinality must be non-negative");
    int index = Arrays.binarySearch(LOWER_BOUNDS, cardinality);
    counts[index >= 0 ? index : -index - 2] += fileCount;
  }

  /** Applies this delta to a copy of the base CRC's histogram, preserving both inputs. */
  public DeletedRecordCountsHistogram applyToBase(DeletedRecordCountsHistogram base) {
    long[] result = base.counts.clone();
    for (int i = 0; i < result.length; i++) {
      result[i] += counts[i];
    }
    return new DeletedRecordCountsHistogram(result);
  }

  public Row toRow() {
    return new GenericRow(
        FULL_SCHEMA,
        Collections.singletonMap(
            /* key */ 0,
            VectorUtils.buildArrayValue(
                Arrays.stream(counts).boxed().collect(Collectors.toList()), LongType.LONG)));
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DeletedRecordCountsHistogram
        && Arrays.equals(counts, ((DeletedRecordCountsHistogram) other).counts);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counts);
  }
}
