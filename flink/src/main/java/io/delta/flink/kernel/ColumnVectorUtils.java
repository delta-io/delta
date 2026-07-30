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
import io.delta.kernel.types.*;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Static helpers for Kernel {@link ColumnVector} / {@link ColumnarBatch} / {@link
 * FilteredColumnarBatch}: typed single-cell read ({@link #get}), batch wrapping ({@link #wrap},
 * {@link #child}, {@link #notNullAt}), and selection-vector construction ({@link #filter}, {@link
 * #notNull}).
 *
 * <p>The selection-vector convention is {@code true = keep, false = drop} — matches {@code
 * FilteredColumnarBatch}.
 */
public class ColumnVectorUtils {

  /**
   * Returns the value at {@code rowId} from {@code input} as a plain Java object.
   *
   * <p>Mirrors the type coverage of {@link
   * io.delta.flink.sink.Conversions.DeltaToJava#data(StructType, io.delta.kernel.data.Row, int)} —
   * only primitive Kernel types are supported. Complex types (struct, array, map) throw {@link
   * UnsupportedOperationException}; the caller is expected to descend into them explicitly via
   * {@link ColumnVector#getChild(int)}, {@link ColumnVector#getArray(int)}, or {@link
   * ColumnVector#getMap(int)}.
   *
   * @param input column vector to read from
   * @param rowId 0-based row index within {@code input}
   * @return the value at {@code (input, rowId)}; {@code null} if the cell is null
   * @throws UnsupportedOperationException for complex types
   */
  public static Object get(ColumnVector input, int rowId) {
    if (input.isNullAt(rowId)) {
      return null;
    }
    DataType type = input.getDataType();
    if (type.equivalent(BooleanType.BOOLEAN)) {
      return input.getBoolean(rowId);
    } else if (type.equivalent(ByteType.BYTE)) {
      return input.getByte(rowId);
    } else if (type.equivalent(ShortType.SHORT)) {
      return input.getShort(rowId);
    } else if (type.equivalent(IntegerType.INTEGER)) {
      return input.getInt(rowId);
    } else if (type.equivalent(LongType.LONG)) {
      return input.getLong(rowId);
    } else if (type.equivalent(FloatType.FLOAT)) {
      return input.getFloat(rowId);
    } else if (type.equivalent(DoubleType.DOUBLE)) {
      return input.getDouble(rowId);
    } else if (type.equivalent(StringType.STRING)) {
      return input.getString(rowId);
    } else if (type.equivalent(BinaryType.BINARY)) {
      return input.getBinary(rowId);
    } else if (type.equivalent(DateType.DATE)) {
      // Days since 1970-01-01
      return input.getInt(rowId);
    } else if (type.equivalent(TimestampType.TIMESTAMP)) {
      // Microseconds since epoch, UTC
      return input.getLong(rowId);
    } else if (type.equivalent(TimestampNTZType.TIMESTAMP_NTZ)) {
      // Microseconds since epoch, no time zone
      return input.getLong(rowId);
    } else if (type instanceof DecimalType) {
      return input.getDecimal(rowId);
    } else {
      // StructType, ArrayType, MapType — caller must navigate these explicitly.
      throw new UnsupportedOperationException("Unsupported column vector type: " + type);
    }
  }

  public static FilteredColumnarBatch wrap(ColumnarBatch data) {
    return new FilteredColumnarBatch(data, Optional.empty());
  }

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
