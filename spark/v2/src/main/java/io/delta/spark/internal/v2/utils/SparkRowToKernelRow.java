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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

/**
 * Adapts a Spark Row to the Kernel Row interface. Designed and tested for AddFile schema; other
 * schemas may work but are not validated.
 *
 * <p>Null contract: primitive getters ({@code getBoolean}, {@code getInt}, etc.) throw
 * {@link IllegalStateException} if the field is null (callers must check {@code isNullAt} first).
 * Reference-type getters ({@code getString}, {@code getStruct}, {@code getMap}, {@code getArray})
 * return {@code null} for null fields.
 */
public class SparkRowToKernelRow implements Row {

  private static final Set<Class<? extends DataType>> PASSTHROUGH_TYPES =
      Set.of(
          BooleanType.class,
          ByteType.class,
          ShortType.class,
          IntegerType.class,
          LongType.class,
          FloatType.class,
          DoubleType.class,
          StringType.class,
          BinaryType.class,
          DecimalType.class);

  private final org.apache.spark.sql.Row sparkRow;
  private final StructType kernelSchema;

  public SparkRowToKernelRow(org.apache.spark.sql.Row sparkRow, StructType kernelSchema) {
    this.sparkRow = sparkRow;
    this.kernelSchema = kernelSchema;
  }

  @Override
  public StructType getSchema() {
    return kernelSchema;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return sparkRow.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as BOOLEAN at ordinal " + ordinal);
    }
    return sparkRow.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as BYTE at ordinal " + ordinal);
    }
    return sparkRow.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as SHORT at ordinal " + ordinal);
    }
    return sparkRow.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as INT at ordinal " + ordinal);
    }
    return sparkRow.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as LONG at ordinal " + ordinal);
    }
    return sparkRow.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as FLOAT at ordinal " + ordinal);
    }
    return sparkRow.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      throw new IllegalStateException(
          "Cannot read a null value as DOUBLE at ordinal " + ordinal);
    }
    return sparkRow.getDouble(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    return sparkRow.getString(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    return sparkRow.getDecimal(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    return (byte[]) sparkRow.get(ordinal);
  }

  @Override
  public Row getStruct(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    org.apache.spark.sql.Row nested = sparkRow.getStruct(ordinal);
    StructType nestedSchema = (StructType) kernelSchema.at(ordinal).getDataType();
    return new SparkRowToKernelRow(nested, nestedSchema);
  }

  @Override
  public MapValue getMap(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    Object raw = sparkRow.get(ordinal);
    Map<?, ?> javaMap;
    if (raw instanceof scala.collection.Map) {
      javaMap = scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Map<?, ?>) raw);
    } else {
      javaMap = (Map<?, ?>) raw;
    }
    MapType mt = (MapType) kernelSchema.at(ordinal).getDataType();
    return javaMapToKernelMapValue(javaMap, mt);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    if (sparkRow.isNullAt(ordinal)) {
      return null;
    }
    List<?> javaList = sparkRow.getList(ordinal);
    ArrayType at = (ArrayType) kernelSchema.at(ordinal).getDataType();
    return javaListToKernelArrayValue(javaList, at);
  }

  static MapValue javaMapToKernelMapValue(Map<?, ?> javaMap, MapType mt) {
    List<Object> keys = new ArrayList<>(javaMap.size());
    List<Object> values = new ArrayList<>(javaMap.size());
    for (Map.Entry<?, ?> entry : javaMap.entrySet()) {
      keys.add(sparkValueToKernel(entry.getKey(), mt.getKeyType()));
      values.add(sparkValueToKernel(entry.getValue(), mt.getValueType()));
    }
    int size = javaMap.size();
    ColumnVector keyVector = VectorUtils.buildColumnVector(keys, mt.getKeyType());
    ColumnVector valueVector = VectorUtils.buildColumnVector(values, mt.getValueType());
    return new MapValue() {
      @Override
      public int getSize() {
        return size;
      }

      @Override
      public ColumnVector getKeys() {
        return keyVector;
      }

      @Override
      public ColumnVector getValues() {
        return valueVector;
      }
    };
  }

  static ArrayValue javaListToKernelArrayValue(List<?> javaList, ArrayType at) {
    List<Object> kernelValues = new ArrayList<>(javaList.size());
    for (Object element : javaList) {
      kernelValues.add(sparkValueToKernel(element, at.getElementType()));
    }
    return VectorUtils.buildArrayValue(kernelValues, at.getElementType());
  }

  @SuppressWarnings("unchecked")
  static Object sparkValueToKernel(Object sparkValue, DataType dt) {
    if (sparkValue == null) {
      return null;
    }
    if (PASSTHROUGH_TYPES.contains(dt.getClass())) {
      return sparkValue;
    }
    if (dt instanceof DateType) {
      if (sparkValue instanceof Integer) {
        return sparkValue;
      } else if (sparkValue instanceof java.sql.Date) {
        return DateTimeUtils.fromJavaDate((java.sql.Date) sparkValue);
      } else if (sparkValue instanceof java.time.LocalDate) {
        return DateTimeUtils.localDateToDays((java.time.LocalDate) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert " + sparkValue.getClass() + " to DateType");
    }
    if (dt instanceof TimestampType) {
      if (sparkValue instanceof Long) {
        return sparkValue;
      } else if (sparkValue instanceof java.sql.Timestamp) {
        return DateTimeUtils.fromJavaTimestamp((java.sql.Timestamp) sparkValue);
      } else if (sparkValue instanceof java.time.Instant) {
        return DateTimeUtils.instantToMicros((java.time.Instant) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert " + sparkValue.getClass() + " to TimestampType");
    }
    if (dt instanceof TimestampNTZType) {
      if (sparkValue instanceof Long) {
        return sparkValue;
      } else if (sparkValue instanceof java.time.LocalDateTime) {
        return DateTimeUtils.localDateTimeToMicros((java.time.LocalDateTime) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert " + sparkValue.getClass() + " to TimestampNTZType");
    }
    if (dt instanceof StructType) {
      return new SparkRowToKernelRow((org.apache.spark.sql.Row) sparkValue, (StructType) dt);
    }
    if (dt instanceof MapType) {
      Map<?, ?> javaMap;
      if (sparkValue instanceof scala.collection.Map) {
        javaMap =
            scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Map<?, ?>) sparkValue);
      } else {
        javaMap = (Map<?, ?>) sparkValue;
      }
      return javaMapToKernelMapValue(javaMap, (MapType) dt);
    }
    if (dt instanceof ArrayType) {
      List<?> javaList;
      if (sparkValue instanceof scala.collection.Seq) {
        javaList =
            scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Seq<?>) sparkValue);
      } else {
        javaList = (List<?>) sparkValue;
      }
      return javaListToKernelArrayValue(javaList, (ArrayType) dt);
    }
    throw new UnsupportedOperationException("Unsupported Kernel DataType: " + dt);
  }
}
