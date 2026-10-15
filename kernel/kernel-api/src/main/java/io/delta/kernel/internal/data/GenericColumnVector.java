/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.data;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/** A generic implementation of {@link ColumnVector} that wraps a list of values. */
public class GenericColumnVector implements ColumnVector {
  private final List<?> values;
  private final DataType dataType;

  public GenericColumnVector(List<?> values, DataType dataType) {
    this.values = values;
    this.dataType = dataType;
  }

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
    checkArgument(IntegerType.INTEGER.equals(dataType) || DateType.DATE.equals(dataType));
    return (Integer) getValidatedValue(rowId, Integer.class);
  }

  @Override
  public long getLong(int rowId) {
    checkArgument(
        LongType.LONG.equals(dataType)
            || TimestampType.TIMESTAMP.equals(dataType)
            || TimestampNTZType.TIMESTAMP_NTZ.equals(dataType));
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

    return new GenericColumnVector(childValues, childDatatype);
  }

  private void validateRowId(int rowId) {
    checkArgument(rowId >= 0 && rowId < values.size(), "Invalid rowId: %s", rowId);
  }

  private Object getValidatedValue(int rowId, Class<?> expectedType) {
    validateRowId(rowId);
    Object value = values.get(rowId);
    checkArgument(
        expectedType.isInstance(value), "Value must be of type %s", expectedType.getSimpleName());
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
    if (childDatatype instanceof BooleanType) {
      return row.getBoolean(ordinal);
    }
    if (childDatatype instanceof ByteType) {
      return row.getByte(ordinal);
    }
    if (childDatatype instanceof ShortType) {
      return row.getShort(ordinal);
    }
    if (childDatatype instanceof IntegerType || childDatatype instanceof DateType) {
      return row.getInt(ordinal);
    }
    if (childDatatype instanceof LongType
        || childDatatype instanceof TimestampType
        || childDatatype instanceof TimestampNTZType) {
      return row.getLong(ordinal);
    }
    if (childDatatype instanceof FloatType) {
      return row.getFloat(ordinal);
    }
    if (childDatatype instanceof DoubleType) {
      return row.getDouble(ordinal);
    }

    // Complex Types
    if (childDatatype instanceof StringType) {
      return row.getString(ordinal);
    }
    if (childDatatype instanceof BinaryType) {
      return row.getBinary(ordinal);
    }
    if (childDatatype instanceof DecimalType) {
      return row.getDecimal(ordinal);
    }

    // Nested Types
    if (childDatatype instanceof StructType) {
      return row.getStruct(ordinal);
    }
    if (childDatatype instanceof ArrayType) {
      return row.getArray(ordinal);
    }
    if (childDatatype instanceof MapType) {
      return row.getMap(ordinal);
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported data type: %s", childDatatype.getClass().getSimpleName()));
  }
}
