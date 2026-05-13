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

import static io.delta.kernel.internal.util.Preconditions.checkState;

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
 * <p>Null contract: primitive getters ({@code getBoolean}, {@code getInt}, etc.) throw {@link
 * IllegalStateException} if the field is null (callers must check {@code isNullAt} first).
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
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as BOOLEAN at ordinal %d", ordinal));
    return sparkRow.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as BYTE at ordinal %d", ordinal));
    return sparkRow.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as SHORT at ordinal %d", ordinal));
    return sparkRow.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as INT at ordinal %d", ordinal));
    if (kernelSchema.at(ordinal).getDataType() instanceof DateType) {
      // Kernel's Row.getInt() contract for DateType: return epoch days since 1970-01-01.
      // (Evidence: VectorUtils.java groups IntegerType||DateType -> getInt(),
      // GenericColumnVector does the same, StructRow delegates to ColumnVector.getInt()
      // which natively stores dates as epoch-day ints.)
      //
      // Spark's public Row stores DateType as java.sql.Date (or java.time.LocalDate
      // depending on spark.sql.datetime.java8API.enabled), NOT as raw int.
      // Row.getInt() on Spark's side is just a raw cast -- it would ClassCastException.
      // We must convert the java.sql/java.time object to the int Kernel expects.
      //
      // We handle multiple input types because the wrapped Spark Row could be:
      // - GenericRowWithSchema from RowFactory.create() -> stores java.sql.Date
      // - InternalRow wrapper -> may already store as int (epoch days)
      // - Any other Row implementation
      Object raw = sparkRow.get(ordinal);
      return (int) sparkValueToKernel(raw, DateType.DATE);
    }
    return sparkRow.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as LONG at ordinal %d", ordinal));
    DataType dt = kernelSchema.at(ordinal).getDataType();
    if (dt instanceof TimestampType || dt instanceof TimestampNTZType) {
      // Kernel's Row.getLong() contract: return microseconds for timestamp types.
      // TimestampType = UTC micros, TimestampNTZType = wall-clock micros.
      // (Evidence: VectorUtils groups LongType||TimestampType -> getLong(),
      // GenericColumnVector does the same, StructRow delegates to ColumnVector.getLong().)
      //
      // Spark's public Row stores TimestampType as java.sql.Timestamp (or Instant) and
      // TimestampNTZType as java.time.LocalDateTime — not raw long. We must convert.
      //
      // Note: unlike sparkValueToKernel() which rejects Long for TimestampNTZType (to
      // prevent UTC/wall-clock ambiguity in recursive map/array values), getLong() accepts
      // Long directly because it fulfills Kernel's Row.getLong() contract — the schema
      // already establishes the timestamp semantics at this level.
      Object raw = sparkRow.get(ordinal);
      if (raw instanceof Long) {
        return (long) raw;
      }
      return (long) sparkValueToKernel(raw, dt);
    }
    return sparkRow.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as FLOAT at ordinal %d", ordinal));
    return sparkRow.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    checkState(
        !sparkRow.isNullAt(ordinal),
        String.format("Cannot read a null value as DOUBLE at ordinal %d", ordinal));
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
    Object raw = sparkRow.get(ordinal);
    List<?> javaList;
    if (raw instanceof scala.collection.Seq) {
      javaList = scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Seq<?>) raw);
    } else {
      javaList = (List<?>) raw;
    }
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
    // --- Date/Timestamp conversion (Spark -> Kernel) ---
    //
    // Kernel expects dates and timestamps as raw primitives:
    //   DateType        -> int  (epoch days since 1970-01-01)
    //   TimestampType   -> long (microseconds since epoch, UTC)
    //   TimestampNTZType -> long (microseconds since epoch, wall-clock / no timezone)
    //
    // However, the incoming sparkValue can arrive in MULTIPLE forms depending on the
    // call site:
    //   1. Already a raw int/long — happens when the value originated from Spark's
    //      InternalRow (e.g., a GenericInternalRow or UnsafeRow), where dates/timestamps
    //      are stored as primitives matching Kernel's representation.
    //   2. A java.sql.Date / java.sql.Timestamp / java.time.* object — happens when the
    //      value comes through Spark's public Row interface (Row.get(i)), which returns
    //      the user-facing Java types.
    //
    // We handle both forms for each type so this converter works regardless of whether
    // the caller supplies internal or external row values.
    if (dt instanceof DateType) {
      if (sparkValue instanceof Integer) {
        // Already epoch-days (from InternalRow); matches Kernel's DateType representation.
        return sparkValue;
      } else if (sparkValue instanceof java.sql.Date) {
        // Convert java.sql.Date -> epoch-days int.
        return DateTimeUtils.fromJavaDate((java.sql.Date) sparkValue);
      } else if (sparkValue instanceof java.time.LocalDate) {
        // Convert java.time.LocalDate -> epoch-days int.
        return DateTimeUtils.localDateToDays((java.time.LocalDate) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert " + sparkValue.getClass() + " to DateType");
    }
    if (dt instanceof TimestampType) {
      if (sparkValue instanceof Long) {
        // Already UTC epoch-microseconds (from InternalRow or pre-converted value).
        // Kernel stores TimestampType as long micros. Callers are responsible for
        // ensuring the long value represents UTC microseconds.
        return sparkValue;
      } else if (sparkValue instanceof java.sql.Timestamp) {
        // Convert java.sql.Timestamp -> epoch-microseconds long (UTC).
        return DateTimeUtils.fromJavaTimestamp((java.sql.Timestamp) sparkValue);
      } else if (sparkValue instanceof java.time.Instant) {
        // Convert java.time.Instant -> epoch-microseconds long (UTC).
        return DateTimeUtils.instantToMicros((java.time.Instant) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert " + sparkValue.getClass() + " to TimestampType");
    }
    if (dt instanceof TimestampNTZType) {
      // Unlike TimestampType, we do NOT accept raw Long here. A Long is ambiguous for
      // TimestampNTZType: it could be UTC micros (from a TimestampType context) silently
      // misinterpreted as wall-clock micros. KernelRowToSparkRow.toSparkValue() converts
      // TimestampNTZType to LocalDateTime (via DateTimeUtils.microsToLocalDateTime), so
      // the reverse path must require LocalDateTime to preserve the round-trip contract.
      // Callers with a raw long must explicitly wrap it:
      //   DateTimeUtils.microsToLocalDateTime(longVal)
      if (sparkValue instanceof java.time.LocalDateTime) {
        return DateTimeUtils.localDateTimeToMicros((java.time.LocalDateTime) sparkValue);
      }
      throw new UnsupportedOperationException(
          "Cannot convert "
              + sparkValue.getClass()
              + " to TimestampNTZType. Use java.time.LocalDateTime"
              + " (raw Long is rejected to prevent UTC/wall-clock ambiguity).");
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
