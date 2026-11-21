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

/**
 * Adapts a Spark Row to the Kernel Row interface. Designed and tested for AddFile schema; other
 * schemas may work but are not validated.
 */
public class SparkRowToKernelRow implements Row {

  private static final Set<Class<? extends DataType>> PASSTHROUGH_TYPES =
      Set.of(
          BooleanType.class,
          ByteType.class,
          ShortType.class,
          IntegerType.class,
          DateType.class,
          LongType.class,
          TimestampType.class,
          TimestampNTZType.class,
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
    return sparkRow.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return sparkRow.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return sparkRow.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return sparkRow.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return sparkRow.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return sparkRow.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return sparkRow.getDouble(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    return sparkRow.getString(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    return sparkRow.getDecimal(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return (byte[]) sparkRow.get(ordinal);
  }

  @Override
  public Row getStruct(int ordinal) {
    org.apache.spark.sql.Row nested = sparkRow.getStruct(ordinal);
    StructType nestedSchema = (StructType) kernelSchema.at(ordinal).getDataType();
    return new SparkRowToKernelRow(nested, nestedSchema);
  }

  @Override
  public MapValue getMap(int ordinal) {
    Map<?, ?> javaMap = sparkRow.getJavaMap(ordinal);
    MapType mt = (MapType) kernelSchema.at(ordinal).getDataType();
    return javaMapToKernelMapValue(javaMap, mt);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
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
