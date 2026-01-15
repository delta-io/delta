/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

public class ColumnVectorUtils {

  public static Function<ColumnarBatch, FilteredColumnarBatch> notNullAt(int ordinal) {
    return (batch) -> new FilteredColumnarBatch(batch, notNull(batch.getColumnVector(ordinal)));
  }

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
   * Create a column vector that filter out data based on the pred(data, rowId)
   *
   * @param size the filter size
   * @param pred a predicate taking (data, rowId) as input
   * @return a column vector masking out the rows with pred returning false
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

  public static Optional<ColumnVector> notNull(ColumnVector input) {
    return filter(input.getSize(), (rowId) -> !input.isNullAt(rowId));
  }
}
