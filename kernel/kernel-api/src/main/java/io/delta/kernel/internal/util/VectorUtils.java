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
   * Creates an {@link ArrayValue} from list of objects.
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
   * Utility method to create a {@link ColumnVector} for given list of object, the object should be
   * primitive type or an Row instance.
   *
   * @param values list of strings
   * @return a {@link ColumnVector} with the given values type.
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
        validateRowId(rowId);
        return values.get(rowId) == null;
      }

      @Override
      public boolean getBoolean(int rowId) {
        checkArgument(BooleanType.BOOLEAN.equals(dataType));
        return (Boolean) getValidatedValue(rowId, Boolean.class);
      }

      @Override
      public byte getByte(int rowId) {
        checkArgument(ByteType.BYTE.equals(dataType));
        return (Byte) getValidatedValue(rowId, Byte.class);
      }

      @Override
      public short getShort(int rowId) {
        checkArgument(ShortType.SHORT.equals(dataType));
        return (Short) getValidatedValue(rowId, Short.class);
      }

      @Override
      public int getInt(int rowId) {
        checkArgument(IntegerType.INTEGER.equals(dataType));
        return (Integer) getValidatedValue(rowId, Integer.class);
      }

      @Override
      public long getLong(int rowId) {
        checkArgument(LongType.LONG.equals(dataType));
        return (Long) getValidatedValue(rowId, Long.class);
      }

      @Override
      public float getFloat(int rowId) {
        checkArgument(FloatType.FLOAT.equals(dataType));
        return (Float) getValidatedValue(rowId, Float.class);
      }

      @Override
      public double getDouble(int rowId) {
        checkArgument(DoubleType.DOUBLE.equals(dataType));
        return (Double) getValidatedValue(rowId, Double.class);
      }

      @Override
      public BigDecimal getDecimal(int rowId) {
        checkArgument(dataType instanceof DecimalType);
        return (BigDecimal) getValidatedValue(rowId, BigDecimal.class);
      }

      @Override
      public String getString(int rowId) {
        checkArgument(StringType.STRING.equals(dataType));
        return (String) getValidatedValue(rowId, String.class);
      }

      @Override
      public byte[] getBinary(int rowId) {
        checkArgument(BinaryType.BINARY.equals(dataType));
        return (byte[]) getValidatedValue(rowId, byte[].class);
      }

      @Override
      public ArrayValue getArray(int rowId) {
        checkArgument(dataType instanceof ArrayType);
        return (ArrayValue) getValidatedValue(rowId, ArrayValue.class);
      }

      @Override
      public MapValue getMap(int rowId) {
        checkArgument(dataType instanceof MapType);
        return (MapValue) getValidatedValue(rowId, MapValue.class);
      }

      @Override
      public ColumnVector getChild(int ordinal) {
        checkArgument(dataType instanceof StructType);
        checkArgument(ordinal < ((StructType) dataType).length());

        DataType childDatatype = ((StructType) dataType).at(ordinal).getDataType();
        List<?> childValues = extractChildValues(ordinal, childDatatype);

        return buildColumnVector(childValues, childDatatype);
      }

      private void validateRowId(int rowId) {
        checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
      }

      private Object getValidatedValue(int rowId, Class<?> expectedType) {
        validateRowId(rowId);
        Object value = values.get(rowId);
        checkArgument(expectedType.isInstance(value),
                "Value must be of type %s", expectedType.getSimpleName());
        return value;
      }


      private List<?> extractChildValues(int ordinal, DataType childDatatype) {
        return values.stream()
                .map(e -> extractChildValue(e, ordinal, childDatatype))
                .collect(Collectors.toList());
      }

      private Object extractChildValue(Object element, int ordinal, DataType childDatatype) {
        checkArgument(element instanceof Row);
        Row row = (Row) element;

        if (row.isNullAt(ordinal)) {
          return null;
        }

        return extractTypedValue(row, ordinal, childDatatype);
      }

      private Object extractTypedValue(Row row, int ordinal, DataType childDatatype) {
        // Primitive Types
        if (childDatatype instanceof BooleanType) return row.getBoolean(ordinal);
        if (childDatatype instanceof ByteType) return row.getByte(ordinal);
        if (childDatatype instanceof ShortType) return row.getShort(ordinal);
        if (childDatatype instanceof IntegerType ||
                childDatatype instanceof DateType) return row.getInt(ordinal);
        if (childDatatype instanceof LongType ||
                childDatatype instanceof TimestampType) return row.getLong(ordinal);
        if (childDatatype instanceof FloatType) return row.getFloat(ordinal);
        if (childDatatype instanceof DoubleType) return row.getDouble(ordinal);

        // Complex Types
        if (childDatatype instanceof StringType) return row.getString(ordinal);
        if (childDatatype instanceof BinaryType) return row.getBinary(ordinal);
        if (childDatatype instanceof DecimalType) return row.getDecimal(ordinal);

        // Nested Types
        if (childDatatype instanceof StructType) return row.getStruct(ordinal);
        if (childDatatype instanceof ArrayType) return row.getArray(ordinal);
        if (childDatatype instanceof MapType) return row.getMap(ordinal);

        throw new UnsupportedOperationException(
                String.format("Unsupported data type: %s", childDatatype.getClass().getSimpleName()));
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
