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
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.DelegateRow;
import io.delta.kernel.types.*;
import java.util.Collections;
import java.util.Map;

/**
 * An abstract base class for Delta Log actions that are backed by a {@link Row}. This design is to
 * avoid materialization of all fields when creating action instances from action rows within
 * Kernel. Actions like {@link AddFile} can extend this class to maintain just a reference to the
 * underlying action row. Subclasses can:
 *
 * <ul>
 *   <li>Access field values directly from the row via {@link #getValueAsObject}
 *   <li>Create new action instances with updated fields via {@link #createRowWithOverriddenValue}
 * </ul>
 */
public abstract class RowBackedAction {

  /** The underlying {@link Row} that represents an action and contains all its field values. */
  private final Row row;

  protected RowBackedAction(Row row) {
    checkArgument(
        row.getSchema().equals(getFullSchema()),
        "Expected row schema: %s, found: %s",
        getFullSchema(),
        row.getSchema());

    this.row = row;
  }

  /** Returns the full schema of the row that represents this action. */
  protected abstract StructType getFullSchema();

  /**
   * Returns the index of the field with the given name in the full schema of the row. Throws an
   * {@link IllegalArgumentException} if the field is not found.
   */
  protected int getFieldIndex(String fieldName) {
    int index = getFullSchema().indexOf(fieldName);
    checkArgument(index >= 0, "Field '%s' not found in schema: %s", fieldName, getFullSchema());
    return index;
  }

  /**
   * Gets the value with the given field name from the row. The type of the Object returned depends
   * on the data type of the field. If the field is nullable and the value is null, this method will
   * return null. If the field is not nullable and the value is null, this method will throw an
   * {@link IllegalArgumentException}.
   */
  protected Object getValueAsObject(String fieldName) {
    StructField field = getFullSchema().get(fieldName);
    int ordinal = getFieldIndex(fieldName);

    if (!field.isNullable()) {
      requireNonNull(row, ordinal, field.getName());
    } else if (row.isNullAt(ordinal)) {
      return null;
    }

    DataType dataType = field.getDataType();
    if (dataType instanceof BooleanType) {
      return row.getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return row.getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return row.getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return row.getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return row.getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return row.getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return row.getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return row.getString(ordinal);
    } else if (dataType instanceof DecimalType) {
      return row.getDecimal(ordinal);
    } else if (dataType instanceof BinaryType) {
      return row.getBinary(ordinal);
    } else if (dataType instanceof StructType) {
      return row.getStruct(ordinal);
    } else if (dataType instanceof ArrayType) {
      return row.getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return row.getMap(ordinal);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported data type: " + dataType.getClass().getName());
    }
  }

  /**
   * Creates a new {@link Row} with the same schema and values as the row backing this action, but
   * with the value of the field with the given name overridden by the given value.
   */
  protected Row createRowWithOverriddenValue(String fieldName, Object value) {
    Map<Integer, Object> overrides = Collections.singletonMap(getFieldIndex(fieldName), value);
    return new DelegateRow(row, overrides);
  }

  /** Returns the underlying {@link Row} that represents this action. */
  public final Row toRow() {
    return row;
  }
}
