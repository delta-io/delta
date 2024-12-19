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
package io.delta.kernel.internal.data;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * This wraps an existing {@link Row} and allows overriding values for some particular ordinals.
 * This enables creating a modified view of a row without mutating the original row.
 */
public class DelegateRow implements Row {

  /** The underlying row being delegated to. */
  private final Row row;

  /**
   * A map of ordinal-to-value overrides that takes precedence over the underlying row's data. When
   * accessing data, this map is checked first before falling back to the underlying row.
   */
  private final Map<Integer, Object> overrides;

  public DelegateRow(Row row, Map<Integer, Object> overrides) {
    requireNonNull(row, "row is null");
    requireNonNull(overrides, "map of overrides is null");

    if (row instanceof DelegateRow) {
      // If the row is already a delegation of another row, we merge the overrides and keep only
      // one layer of delegation.
      DelegateRow delegateRow = (DelegateRow) row;
      this.row = delegateRow.row;
      this.overrides = new HashMap<>(delegateRow.overrides);
      this.overrides.putAll(overrides);
    } else {
      this.row = row;
      this.overrides = new HashMap<>(overrides);
    }
  }

  @Override
  public StructType getSchema() {
    return row.getSchema();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      return overrides.get(ordinal) == null;
    }
    return row.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, BooleanType.class, "boolean");
      return (boolean) overrides.get(ordinal);
    }
    return row.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, ByteType.class, "byte");
      return (byte) overrides.get(ordinal);
    }
    return row.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    throwIfUnsafeAccess(ordinal, ShortType.class, "short");
    if (overrides.containsKey(ordinal)) {
      return (short) overrides.get(ordinal);
    }
    return row.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, IntegerType.class, "integer");
      return (int) overrides.get(ordinal);
    }
    return row.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, LongType.class, "long");
      return (long) overrides.get(ordinal);
    }
    return row.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, FloatType.class, "float");
      return (float) overrides.get(ordinal);
    }
    return row.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, DoubleType.class, "double");
      return (double) overrides.get(ordinal);
    }
    return row.getDouble(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, StringType.class, "string");
      return (String) overrides.get(ordinal);
    }
    return row.getString(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, DecimalType.class, "decimal");
      return (BigDecimal) overrides.get(ordinal);
    }
    return row.getDecimal(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, BinaryType.class, "binary");
      return (byte[]) overrides.get(ordinal);
    }
    return row.getBinary(ordinal);
  }

  @Override
  public Row getStruct(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      throwIfUnsafeAccess(ordinal, StructType.class, "struct");
      return (Row) overrides.get(ordinal);
    }
    return row.getStruct(ordinal);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      // TODO: Not sufficient check, also need to check the element type. This should be revisited
      //  together with the GenericRow.
      throwIfUnsafeAccess(ordinal, ArrayType.class, "array");
      return (ArrayValue) overrides.get(ordinal);
    }
    return row.getArray(ordinal);
  }

  @Override
  public MapValue getMap(int ordinal) {
    if (overrides.containsKey(ordinal)) {
      // TODO: Not sufficient check, also need to check the element type. This should be revisited
      //  together with the GenericRow.
      throwIfUnsafeAccess(ordinal, MapType.class, "map");
      return (MapValue) overrides.get(ordinal);
    }
    return row.getMap(ordinal);
  }

  private void throwIfUnsafeAccess(
      int ordinal, Class<? extends DataType> expDataType, String accessType) {

    DataType actualDataType = dataType(ordinal);
    if (!expDataType.isAssignableFrom(actualDataType.getClass())) {
      String msg =
          String.format(
              "Trying to access a `%s` value from vector of type `%s`", accessType, actualDataType);
      throw new UnsupportedOperationException(msg);
    }
  }

  private DataType dataType(int ordinal) {
    if (row.getSchema().length() <= ordinal) {
      throw new IllegalArgumentException("invalid ordinal: " + ordinal);
    }

    return row.getSchema().at(ordinal).getDataType();
  }
}
