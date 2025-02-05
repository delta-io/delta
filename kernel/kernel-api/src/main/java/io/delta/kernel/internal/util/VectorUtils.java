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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class VectorUtils {

  private VectorUtils() {}

  /**
   * Converts an {@link ArrayValue} to a Java list. Any nested complex types are also converted to
   * their Java type.
   */
  public static <T> List<T> toJavaList(ArrayValue arrayValue) {
    final ColumnVector elementVector = arrayValue.getElements();
    final DataType dataType = elementVector.getDataType();

    List<T> elements = new ArrayList<>();
    for (int i = 0; i < arrayValue.getSize(); i++) {
      elements.add((T) getValueAsObject(elementVector, dataType, i));
    }
    return elements;
  }

  /**
   * Converts a {@link MapValue} to a Java map. Any nested complex types are also converted to their
   * Java type.
   *
   * <p>Please note not all key types override hashCode/equals. Be careful when using with keys of:
   * - Struct type at any nesting level (i.e. ArrayType(StructType) does not) - Binary type
   */
  public static <K, V> Map<K, V> toJavaMap(MapValue mapValue) {
    final ColumnVector keyVector = mapValue.getKeys();
    final DataType keyDataType = keyVector.getDataType();
    final ColumnVector valueVector = mapValue.getValues();
    final DataType valueDataType = valueVector.getDataType();

    Map<K, V> values = new HashMap<>();

    for (int i = 0; i < mapValue.getSize(); i++) {
      Object key = getValueAsObject(keyVector, keyDataType, i);
      Object value = getValueAsObject(valueVector, valueDataType, i);
      values.put((K) key, (V) value);
    }
    return values;
  }

  /**
   * Creates an {@link ArrayValue} from list of object.
   *
   * @param values list of object
   * @return an {@link ArrayValue} with the given values of type {@link StringType}
   */
  public static ArrayValue buildArrayValue(List<?> values, DataType dataType) {
    if (values == null) {
      return null;
    }
    return new ArrayValue() {
      @Override
      public int getSize() {
        return values.size();
      }

      @Override
      public ColumnVector getElements() {
        return buildColumnVector(values, dataType);
      }
    };
  }

  /**
   * Creates a {@link MapValue} from map of string keys and string values. The type {@code
   * map(string -> string)} is a common occurrence in Delta Log schema.
   *
   * @param keyValues
   * @return
   */
  public static MapValue stringStringMapValue(Map<String, String> keyValues) {
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (Map.Entry<String, String> entry : keyValues.entrySet()) {
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    return new MapValue() {
      @Override
      public int getSize() {
        return values.size();
      }

      @Override
      public ColumnVector getKeys() {
        return buildColumnVector(keys, StringType.STRING);
      }

      @Override
      public ColumnVector getValues() {
        return buildColumnVector(values, StringType.STRING);
      }
    };
  }

  /**
   * Utility method to create a {@link ColumnVector} for given list of object, the object should be
   * primitive type or an Row instance.
   *
   * @param values list of strings
   * @return a {@link ColumnVector} with the given values of type {@link StringType}
   */
  public static ColumnVector buildColumnVector(List<?> values, DataType dataType) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return dataType;
      }

      @Override
      public int getSize() {
        return values.size();
      }

      @Override
      public void close() {
        // no-op
      }

      @Override
      public boolean isNullAt(int rowId) {
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        return values.get(rowId) == null;
      }

      @Override
      public boolean getBoolean(int rowId) {
        checkArgument(BooleanType.BOOLEAN.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Boolean);
        return (Boolean) value;
      }

      @Override
      public byte getByte(int rowId) {
        checkArgument(ByteType.BYTE.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Byte);
        return (Byte) value;
      }

      @Override
      public short getShort(int rowId) {
        checkArgument(ShortType.SHORT.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Short);
        return (Short) value;
      }

      @Override
      public int getInt(int rowId) {
        checkArgument(IntegerType.INTEGER.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Integer);
        return (Integer) value;
      }

      @Override
      public long getLong(int rowId) {
        checkArgument(LongType.LONG.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Long);
        return (Long) value;
      }

      @Override
      public float getFloat(int rowId) {
        checkArgument(FloatType.FLOAT.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Float);
        return (Float) value;
      }

      @Override
      public double getDouble(int rowId) {
        checkArgument(DoubleType.DOUBLE.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof Double);
        return (Double) value;
      }

      @Override
      public BigDecimal getDecimal(int rowId) {
        checkArgument(dataType instanceof DecimalType);
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof BigDecimal);
        return (BigDecimal) value;
      }

      @Override
      public String getString(int rowId) {
        checkArgument(StringType.STRING.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof String);
        return (String) value;
      }

      @Override
      public byte[] getBinary(int rowId) {
        checkArgument(BinaryType.BINARY.equals(dataType));
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof byte[]);
        return (byte[]) value;
      }

      @Override
      public ArrayValue getArray(int rowId) {
        checkArgument(dataType instanceof ArrayValue);
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof ArrayValue);
        return (ArrayValue) value;
      }

      @Override
      public MapValue getMap(int rowId) {
        checkArgument(dataType instanceof MapType);
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
        Object value = values.get(rowId);
        checkArgument(value instanceof MapType);
        return (MapValue) value;
      }

      @Override
      public ColumnVector getChild(int ordinal) {
        checkArgument(dataType instanceof StructType);
        checkArgument(ordinal < ((StructType) dataType).length());
        DataType childDatatype = ((StructType) dataType).at(ordinal).getDataType();
        return buildColumnVector(
            values.stream()
                .map(
                    e -> {
                      checkArgument(e instanceof Row);
                      Row row = (Row) e;
                      if (row.isNullAt(ordinal)) {
                        return null;
                      }
                      if (childDatatype instanceof BooleanType) {
                        return row.getBoolean(ordinal);
                      } else if (childDatatype instanceof ByteType) {
                        return row.getByte(ordinal);
                      } else if (childDatatype instanceof ShortType) {
                        return row.getShort(ordinal);
                      } else if (childDatatype instanceof IntegerType
                          || childDatatype instanceof DateType) {
                        // DateType data is stored internally as the number of days since 1970-01-01
                        return row.getInt(ordinal);
                      } else if (childDatatype instanceof LongType
                          || childDatatype instanceof TimestampType) {
                        // TimestampType data is stored internally as the number of microseconds
                        // since the unix epoch
                        return row.getLong(ordinal);
                      } else if (childDatatype instanceof FloatType) {
                        return row.getFloat(ordinal);
                      } else if (childDatatype instanceof DoubleType) {
                        return row.getDouble(ordinal);
                      } else if (childDatatype instanceof StringType) {
                        return row.getString(ordinal);
                      } else if (childDatatype instanceof BinaryType) {
                        return row.getBinary(ordinal);
                      } else if (childDatatype instanceof StructType) {
                        return row.getStruct(ordinal);
                      } else if (childDatatype instanceof DecimalType) {
                        return row.getDecimal(ordinal);
                      } else if (childDatatype instanceof ArrayType) {
                        return row.getArray(ordinal);
                      } else if (dataType instanceof MapType) {
                        return row.getMap(ordinal);
                      } else {
                        throw new UnsupportedOperationException("unsupported data type");
                      }
                    })
                .collect(Collectors.toList()),
            childDatatype);
      }
    };
  }

  /**
   * Gets the value at {@code rowId} from the column vector. The type of the Object returned depends
   * on the data type of the column vector. For complex types array and map, returns the value as
   * Java list or Java map. For struct type, returns an {@link Row}.
   */
  private static Object getValueAsObject(ColumnVector columnVector, DataType dataType, int rowId) {
    if (columnVector.isNullAt(rowId)) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return columnVector.getBoolean(rowId);
    } else if (dataType instanceof ByteType) {
      return columnVector.getByte(rowId);
    } else if (dataType instanceof ShortType) {
      return columnVector.getShort(rowId);
    } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
      // DateType data is stored internally as the number of days since 1970-01-01
      return columnVector.getInt(rowId);
    } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
      // TimestampType data is stored internally as the number of microseconds since the unix
      // epoch
      return columnVector.getLong(rowId);
    } else if (dataType instanceof FloatType) {
      return columnVector.getFloat(rowId);
    } else if (dataType instanceof DoubleType) {
      return columnVector.getDouble(rowId);
    } else if (dataType instanceof StringType) {
      return columnVector.getString(rowId);
    } else if (dataType instanceof BinaryType) {
      return columnVector.getBinary(rowId);
    } else if (dataType instanceof StructType) {
      // TODO are we okay with this usage of StructRow?
      return StructRow.fromStructVector(columnVector, rowId);
    } else if (dataType instanceof DecimalType) {
      return columnVector.getDecimal(rowId);
    } else if (dataType instanceof ArrayType) {
      return toJavaList(columnVector.getArray(rowId));
    } else if (dataType instanceof MapType) {
      return toJavaMap(columnVector.getMap(rowId));
    } else {
      throw new UnsupportedOperationException("unsupported data type");
    }
  }
}
