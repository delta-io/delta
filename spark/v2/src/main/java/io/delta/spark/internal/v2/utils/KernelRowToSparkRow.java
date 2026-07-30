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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

/**
 * Adapts a Kernel Row to the Spark Row interface. Designed and tested for AddFile schema; other
 * schemas may work but are not validated.
 */
public class KernelRowToSparkRow implements Row {

  private final io.delta.kernel.data.Row kernelRow;
  private final StructType kernelSchema;
  private final org.apache.spark.sql.types.StructType sparkSchema;
  private final FieldAccessor rowFieldAccessor;

  public KernelRowToSparkRow(io.delta.kernel.data.Row kernelRow) {
    this(kernelRow, SchemaUtils.convertKernelSchemaToSparkSchema(kernelRow.getSchema()));
  }

  /**
   * Constructor that accepts a pre-computed Spark schema to avoid redundant schema conversion when
   * wrapping many rows that share the same schema.
   */
  public KernelRowToSparkRow(
      io.delta.kernel.data.Row kernelRow, org.apache.spark.sql.types.StructType sparkSchema) {
    this.kernelRow = kernelRow;
    this.kernelSchema = kernelRow.getSchema();
    this.sparkSchema = sparkSchema;
    this.rowFieldAccessor = rowAccessor(kernelRow);
  }

  @Override
  public int length() {
    return kernelSchema.length();
  }

  @Override
  public org.apache.spark.sql.types.StructType schema() {
    return sparkSchema;
  }

  @Override
  public Object get(int i) {
    return toSparkValue(
        rowFieldAccessor, i, kernelSchema.at(i).getDataType(), sparkSchema.fields()[i].dataType());
  }

  /**
   * Field-by-field equality matching Spark's Row.equals semantics. Required because Java classes
   * implementing Scala traits get Object.equals (reference equality) -- the JVM does not inherit
   * default methods from interfaces for Object methods.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Row)) return false;
    Row other = (Row) o;
    if (length() != other.length()) return false;
    for (int i = 0; i < length(); i++) {
      if (isNullAt(i) != other.isNullAt(i)) return false;
      if (!isNullAt(i)) {
        Object thisVal = get(i);
        Object otherVal = other.get(i);
        if (thisVal instanceof byte[] && otherVal instanceof byte[]) {
          if (!java.util.Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) return false;
        } else if (!thisVal.equals(otherVal)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 37;
    for (int i = 0; i < length(); i++) {
      Object val = isNullAt(i) ? null : get(i);
      if (val == null) {
        result = 37 * result;
      } else if (val instanceof byte[]) {
        result = 37 * result + java.util.Arrays.hashCode((byte[]) val);
      } else {
        result = 37 * result + val.hashCode();
      }
    }
    return result;
  }

  @Override
  public Row copy() {
    Object[] values = new Object[length()];
    for (int i = 0; i < values.length; i++) {
      Object v = get(i);
      // Recursively copy nested struct wrappers that still reference the live kernel row.
      // Structs nested inside maps/arrays are materialized when those collections are built
      // by mapValueToScalaMap() and arrayValueToScalaSeq(), so only top-level fields need this.
      if (v instanceof KernelRowToSparkRow) {
        v = ((KernelRowToSparkRow) v).copy();
      }
      values[i] = v;
    }
    return RowFactory.create(values);
  }

  interface FieldAccessor {
    boolean isNullAt(int i);

    boolean getBoolean(int i);

    byte getByte(int i);

    short getShort(int i);

    int getInt(int i);

    long getLong(int i);

    float getFloat(int i);

    double getDouble(int i);

    String getString(int i);

    BigDecimal getDecimal(int i);

    byte[] getBinary(int i);

    io.delta.kernel.data.Row getStruct(int i);

    MapValue getMap(int i);

    ArrayValue getArray(int i);
  }

  private static FieldAccessor rowAccessor(io.delta.kernel.data.Row row) {
    return new FieldAccessor() {
      @Override
      public boolean isNullAt(int i) {
        return row.isNullAt(i);
      }

      @Override
      public boolean getBoolean(int i) {
        return row.getBoolean(i);
      }

      @Override
      public byte getByte(int i) {
        return row.getByte(i);
      }

      @Override
      public short getShort(int i) {
        return row.getShort(i);
      }

      @Override
      public int getInt(int i) {
        return row.getInt(i);
      }

      @Override
      public long getLong(int i) {
        return row.getLong(i);
      }

      @Override
      public float getFloat(int i) {
        return row.getFloat(i);
      }

      @Override
      public double getDouble(int i) {
        return row.getDouble(i);
      }

      @Override
      public String getString(int i) {
        return row.getString(i);
      }

      @Override
      public BigDecimal getDecimal(int i) {
        return row.getDecimal(i);
      }

      @Override
      public byte[] getBinary(int i) {
        return row.getBinary(i);
      }

      @Override
      public io.delta.kernel.data.Row getStruct(int i) {
        return row.getStruct(i);
      }

      @Override
      public MapValue getMap(int i) {
        return row.getMap(i);
      }

      @Override
      public ArrayValue getArray(int i) {
        return row.getArray(i);
      }
    };
  }

  static FieldAccessor vectorAccessor(ColumnVector cv) {
    return new FieldAccessor() {
      @Override
      public boolean isNullAt(int i) {
        return cv.isNullAt(i);
      }

      @Override
      public boolean getBoolean(int i) {
        return cv.getBoolean(i);
      }

      @Override
      public byte getByte(int i) {
        return cv.getByte(i);
      }

      @Override
      public short getShort(int i) {
        return cv.getShort(i);
      }

      @Override
      public int getInt(int i) {
        return cv.getInt(i);
      }

      @Override
      public long getLong(int i) {
        return cv.getLong(i);
      }

      @Override
      public float getFloat(int i) {
        return cv.getFloat(i);
      }

      @Override
      public double getDouble(int i) {
        return cv.getDouble(i);
      }

      @Override
      public String getString(int i) {
        return cv.getString(i);
      }

      @Override
      public BigDecimal getDecimal(int i) {
        return cv.getDecimal(i);
      }

      @Override
      public byte[] getBinary(int i) {
        return cv.getBinary(i);
      }

      @Override
      public io.delta.kernel.data.Row getStruct(int i) {
        return StructRow.fromStructVector(cv, i);
      }

      @Override
      public MapValue getMap(int i) {
        return cv.getMap(i);
      }

      @Override
      public ArrayValue getArray(int i) {
        return cv.getArray(i);
      }
    };
  }

  /**
   * Converts a Kernel field value to its Spark equivalent. Accepts a pre-computed Spark DataType to
   * avoid redundant schema conversion on every row for nested types (structs, maps, arrays).
   */
  static Object toSparkValue(
      FieldAccessor accessor,
      int ordinal,
      DataType dt,
      org.apache.spark.sql.types.DataType sparkDt) {
    if (accessor.isNullAt(ordinal)) {
      return null;
    }
    if (dt instanceof BooleanType) {
      return accessor.getBoolean(ordinal);
    } else if (dt instanceof ByteType) {
      return accessor.getByte(ordinal);
    } else if (dt instanceof ShortType) {
      return accessor.getShort(ordinal);
    } else if (dt instanceof IntegerType) {
      return accessor.getInt(ordinal);
    } else if (dt instanceof DateType) {
      // Kernel stores DateType internally as int (epoch days since 1970-01-01).
      // Spark's public Row.getDate() expects java.sql.Date objects.
      return DateTimeUtils.toJavaDate(accessor.getInt(ordinal));
    } else if (dt instanceof LongType) {
      return accessor.getLong(ordinal);
    } else if (dt instanceof TimestampType) {
      // Kernel stores TimestampType as long (microseconds since epoch, UTC).
      // Spark's public Row expects java.sql.Timestamp objects.
      return DateTimeUtils.toJavaTimestamp(accessor.getLong(ordinal));
    } else if (dt instanceof TimestampNTZType) {
      // Kernel stores TimestampNTZType as long (microseconds, wall-clock / no timezone).
      // Spark's public Row expects java.time.LocalDateTime objects.
      return DateTimeUtils.microsToLocalDateTime(accessor.getLong(ordinal));
    } else if (dt instanceof FloatType) {
      return accessor.getFloat(ordinal);
    } else if (dt instanceof DoubleType) {
      return accessor.getDouble(ordinal);
    } else if (dt instanceof StringType) {
      return accessor.getString(ordinal);
    } else if (dt instanceof DecimalType) {
      return accessor.getDecimal(ordinal);
    } else if (dt instanceof BinaryType) {
      return accessor.getBinary(ordinal);
    } else if (dt instanceof StructType) {
      return new KernelRowToSparkRow(
          accessor.getStruct(ordinal), (org.apache.spark.sql.types.StructType) sparkDt);
    } else if (dt instanceof MapType) {
      org.apache.spark.sql.types.MapType sparkMapType =
          (org.apache.spark.sql.types.MapType) sparkDt;
      return mapValueToScalaMap(accessor.getMap(ordinal), (MapType) dt, sparkMapType);
    } else if (dt instanceof ArrayType) {
      org.apache.spark.sql.types.ArrayType sparkArrayType =
          (org.apache.spark.sql.types.ArrayType) sparkDt;
      return arrayValueToScalaSeq(accessor.getArray(ordinal), (ArrayType) dt, sparkArrayType);
    }
    throw new UnsupportedOperationException("Unsupported Kernel DataType: " + dt);
  }

  static scala.collection.Map<Object, Object> mapValueToScalaMap(
      MapValue mv, MapType mt, org.apache.spark.sql.types.MapType sparkMt) {
    ColumnVector keys = mv.getKeys();
    ColumnVector values = mv.getValues();
    FieldAccessor keyAccessor = vectorAccessor(keys);
    FieldAccessor valueAccessor = vectorAccessor(values);
    Map<Object, Object> javaMap = new HashMap<>();
    for (int i = 0; i < mv.getSize(); i++) {
      Object key = toSparkValue(keyAccessor, i, mt.getKeyType(), sparkMt.keyType());
      if (key instanceof KernelRowToSparkRow) {
        key = ((KernelRowToSparkRow) key).copy();
      }
      Object value = toSparkValue(valueAccessor, i, mt.getValueType(), sparkMt.valueType());
      if (value instanceof KernelRowToSparkRow) {
        value = ((KernelRowToSparkRow) value).copy();
      }
      javaMap.put(key, value);
    }
    return scala.jdk.javaapi.CollectionConverters.asScala(javaMap);
  }

  static scala.collection.Seq<Object> arrayValueToScalaSeq(
      ArrayValue av, ArrayType at, org.apache.spark.sql.types.ArrayType sparkAt) {
    ColumnVector elements = av.getElements();
    FieldAccessor elemAccessor = vectorAccessor(elements);
    List<Object> javaList = new ArrayList<>();
    for (int i = 0; i < av.getSize(); i++) {
      Object elem = toSparkValue(elemAccessor, i, at.getElementType(), sparkAt.elementType());
      if (elem instanceof KernelRowToSparkRow) {
        elem = ((KernelRowToSparkRow) elem).copy();
      }
      javaList.add(elem);
    }
    return scala.jdk.javaapi.CollectionConverters.asScala(javaList).toList();
  }
}
