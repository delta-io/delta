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
package io.delta.kernel.internal.data;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.getValueAsObject;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static io.delta.kernel.internal.util.VectorUtils.toJavaMap;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link Row} abstraction for a struct type column vector and a specific {@code rowId}. */
public class StructRow extends ChildVectorBasedRow {

  public static StructRow fromStructVector(ColumnVector columnVector, int rowId) {
    checkArgument(columnVector.getDataType() instanceof StructType);
    if (columnVector.isNullAt(rowId)) {
      return null;
    } else {
      return new StructRow(columnVector, rowId, (StructType) columnVector.getDataType());
    }
  }

  /**
   * Returns a deep copy of {@code row} as a {@link GenericRow} of plain Java values (nested {@link
   * GenericRow}s, {@link ArrayValue}s, and {@link MapValue}s), holding no reference to any {@link
   * ColumnVector}.
   */
  public static GenericRow deepCopy(Row row) {
    StructType schema = row.getSchema();
    Map<Integer, Object> values = new HashMap<>();
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      if (!row.isNullAt(ordinal)) {
        values.put(ordinal, copyValue(row, ordinal, schema.at(ordinal).getDataType()));
      }
    }
    return new GenericRow(schema, values);
  }

  private static Object copyValue(Row row, int ordinal, DataType type) {
    if (type instanceof StructType) {
      return deepCopy(row.getStruct(ordinal));
    } else if (type instanceof ArrayType) {
      return copyArray(row.getArray(ordinal), ((ArrayType) type).getElementType());
    } else if (type instanceof MapType) {
      return stringStringMapValue(toJavaMap(row.getMap(ordinal)));
    } else if (type instanceof StringType) {
      return row.getString(ordinal);
    } else if (type instanceof LongType) {
      return row.getLong(ordinal);
    } else if (type instanceof IntegerType) {
      return row.getInt(ordinal);
    } else if (type instanceof BooleanType) {
      return row.getBoolean(ordinal);
    } else {
      throw new UnsupportedOperationException("Unsupported data type in deep copy: " + type);
    }
  }

  private static ArrayValue copyArray(ArrayValue array, DataType elementType) {
    ColumnVector elements = array.getElements();
    List<Object> copied = new ArrayList<>(array.getSize());
    for (int i = 0; i < array.getSize(); i++) {
      copied.add(elements.isNullAt(i) ? null : copyElement(elements, i, elementType));
    }
    return buildArrayValue(copied, elementType);
  }

  private static Object copyElement(ColumnVector vector, int rowId, DataType type) {
    if (type instanceof StructType) {
      return deepCopy(fromStructVector(vector, rowId));
    } else if (type instanceof ArrayType) {
      return copyArray(vector.getArray(rowId), ((ArrayType) type).getElementType());
    } else if (type instanceof MapType) {
      return stringStringMapValue(toJavaMap(vector.getMap(rowId)));
    } else {
      return getValueAsObject(vector, type, rowId);
    }
  }

  private final ColumnVector structVector;

  private StructRow(ColumnVector structVector, int rowId, StructType schema) {
    super(rowId, schema);
    this.structVector = structVector;
  }

  @Override
  protected ColumnVector getChild(int ordinal) {
    return structVector.getChild(ordinal);
  }
}
