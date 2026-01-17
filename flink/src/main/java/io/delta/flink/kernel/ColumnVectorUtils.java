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

package io.delta.flink.kernel;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Utility methods for building column-based filters and projections on {@link ColumnarBatch} and
 * {@link ColumnVector} instances.
 *
 * <p>These helpers are primarily used to derive {@link FilteredColumnarBatch} views by applying
 * row-level predicates or navigating nested (struct) columns without copying underlying data.
 */
public class ColumnVectorUtils {

  /**
   * Creates a filter function that keeps only rows where the column at the given ordinal is
   * non-null.
   *
   * @param ordinal column index to check for null values
   * @return a function producing a {@link FilteredColumnarBatch} with null rows removed
   */
  public static Function<ColumnarBatch, FilteredColumnarBatch> notNullAt(int ordinal) {
    return (batch) -> new FilteredColumnarBatch(batch, notNull(batch.getColumnVector(ordinal)));
  }

  /**
   * Creates a projection function that navigates into a struct-typed column and exposes its child
   * columns as a new {@link ColumnarBatch}.
   *
   * <p>The returned batch shares the same row selection vector as the input and does not copy data.
   *
   * @param childName name of the struct column
   * @return a function producing a {@link FilteredColumnarBatch} over the struct’s children
   */
  public static Function<FilteredColumnarBatch, FilteredColumnarBatch> child(String childName) {
    return (batch) -> {
      StructType childSchema =
          (StructType) batch.getData().getSchema().get(childName).getDataType();
      int childIndex = batch.getData().getSchema().indexOf(childName);
      ColumnarBatch newData =
          new ColumnarBatch() {
            @Override
            public StructType getSchema() {
              return childSchema;
            }

            @Override
            public ColumnVector getColumnVector(int ordinal) {
              return batch.getData().getColumnVector(childIndex).getChild(ordinal);
            }

            @Override
            public int getSize() {
              return batch.getData().getSize();
            }
          };
      return new FilteredColumnarBatch(newData, batch.getSelectionVector());
    };
  }

  /**
   * Builds a boolean {@link ColumnVector} representing a row-level filter.
   *
   * <p>The predicate is evaluated per row index, and rows for which the predicate returns {@code
   * false} are masked out.
   *
   * @param size number of rows in the vector
   * @param pred predicate evaluated on each row index
   * @return a boolean column vector encoding the filter
   */
  public static Optional<ColumnVector> filter(int size, Predicate<Integer> pred) {
    return Optional.of(
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return BooleanType.BOOLEAN;
          }

          @Override
          public int getSize() {
            return size;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return false;
          }

          @Override
          public boolean getBoolean(int rowId) {
            return pred.test(rowId);
          }
        });
  }

  /**
   * Creates a boolean filter vector that keeps only non-null rows from the input column.
   *
   * @param input input column vector
   * @return a boolean column vector masking out null rows
   */
  public static Optional<ColumnVector> notNull(ColumnVector input) {
    return filter(input.getSize(), (rowId) -> !input.isNullAt(rowId));
  }
}
