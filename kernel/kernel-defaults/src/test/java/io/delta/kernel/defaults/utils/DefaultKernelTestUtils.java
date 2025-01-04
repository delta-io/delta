/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.utils;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;

public class DefaultKernelTestUtils {
  private DefaultKernelTestUtils() {}

  /** Returns a URI encoded path of the resource. */
  public static String getTestResourceFilePath(String resourcePath) {
    return DefaultKernelTestUtils.class.getClassLoader().getResource(resourcePath).getFile();
  }

  // This will no longer be needed once all tests have been moved to Scala
  public static Object getValueAsObject(Row row, int columnOrdinal) {
    final DataType dataType = row.getSchema().at(columnOrdinal).getDataType();

    if (row.isNullAt(columnOrdinal)) {
      return null;
    }

    if (dataType instanceof BooleanType) {
      return row.getBoolean(columnOrdinal);
    } else if (dataType instanceof ByteType) {
      return row.getByte(columnOrdinal);
    } else if (dataType instanceof ShortType) {
      return row.getShort(columnOrdinal);
    } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
      return row.getInt(columnOrdinal);
    } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
      return row.getLong(columnOrdinal);
    } else if (dataType instanceof FloatType) {
      return row.getFloat(columnOrdinal);
    } else if (dataType instanceof DoubleType) {
      return row.getDouble(columnOrdinal);
    } else if (dataType instanceof StringType) {
      return row.getString(columnOrdinal);
    } else if (dataType instanceof BinaryType) {
      return row.getBinary(columnOrdinal);
    } else if (dataType instanceof StructType) {
      return row.getStruct(columnOrdinal);
    }

    throw new UnsupportedOperationException(dataType + " is not supported yet");
  }

  /**
   * Get the value at given {@code rowId} from the column vector. The type of the value object
   * depends on the data type of the {@code vector}.
   */
  public static Object getValueAsObject(ColumnVector vector, int rowId) {
    final DataType dataType = vector.getDataType();

    if (vector.isNullAt(rowId)) {
      return null;
    }

    if (dataType instanceof BooleanType) {
      return vector.getBoolean(rowId);
    } else if (dataType instanceof ByteType) {
      return vector.getByte(rowId);
    } else if (dataType instanceof ShortType) {
      return vector.getShort(rowId);
    } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
      return vector.getInt(rowId);
    } else if (dataType instanceof LongType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      return vector.getLong(rowId);
    } else if (dataType instanceof FloatType) {
      return vector.getFloat(rowId);
    } else if (dataType instanceof DoubleType) {
      return vector.getDouble(rowId);
    } else if (dataType instanceof StringType) {
      return vector.getString(rowId);
    } else if (dataType instanceof BinaryType) {
      return vector.getBinary(rowId);
    } else if (dataType instanceof DecimalType) {
      return vector.getDecimal(rowId);
    } else if (dataType instanceof StructType) {
      return VectorUtils.toJavaList(vector, rowId);
    } else if (dataType instanceof ArrayType) {
      return VectorUtils.toJavaList(vector.getArray(rowId));
    } else if (dataType instanceof MapType) {
      return VectorUtils.toJavaMap(vector.getMap(rowId));
    }

    throw new UnsupportedOperationException(dataType + " is not supported yet");
  }
}
