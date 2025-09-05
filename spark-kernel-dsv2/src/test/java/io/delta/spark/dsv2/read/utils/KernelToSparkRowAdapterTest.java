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
package io.delta.spark.dsv2.read.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

/** Tests for {@link KernelToSparkRowAdapter}. */
public class KernelToSparkRowAdapterTest {

  @Test
  public void testPrimitiveType() {
    StructType schema =
        new StructType(
            Arrays.asList(
                new StructField("b", BooleanType.BOOLEAN, true),
                new StructField("y", ByteType.BYTE, true),
                new StructField("s", ShortType.SHORT, true),
                new StructField("i", IntegerType.INTEGER, true),
                new StructField("l", LongType.LONG, true),
                new StructField("f", FloatType.FLOAT, true),
                new StructField("d", DoubleType.DOUBLE, true),
                new StructField("dec", new DecimalType(10, 2), true),
                new StructField("bin", BinaryType.BINARY, true),
                new StructField("str", StringType.STRING, true),
                new StructField("dateCol", DateType.DATE, true),
                new StructField("tsCol", TimestampType.TIMESTAMP, true),
                new StructField("tsNtzCol", TimestampNTZType.TIMESTAMP_NTZ, true),
                new StructField("decNull", new DecimalType(10, 2), true),
                new StructField("strNull", StringType.STRING, true)));

    byte[] sampleBytes = new byte[] {1, 2, 3};
    BigDecimal sampleDecimal = new BigDecimal("1234.56");

    List<Object> values =
        Arrays.asList(
            true,
            (byte) 7,
            (short) 300,
            42,
            1234567890123L,
            3.14f,
            6.28d,
            sampleDecimal,
            sampleBytes,
            "hello",
            10,
            1_000_000L,
            2_000_000L,
            null,
            null);

    Row kernelRow = makePrimitiveRow(schema, values);
    InternalRow row = new KernelToSparkRowAdapter(kernelRow);

    assertEquals(15, row.numFields(), "field count");
    assertAll(
        "primitive getters",
        () -> assertTrue(row.getBoolean(0), "boolean"),
        () -> assertEquals((byte) 7, row.getByte(1), "byte"),
        () -> assertEquals((short) 300, row.getShort(2), "short"),
        () -> assertEquals(42, row.getInt(3), "int"),
        () -> assertEquals(1234567890123L, row.getLong(4), "long"),
        () -> assertEquals(3.14f, row.getFloat(5), "float"),
        () -> assertEquals(6.28d, row.getDouble(6), "double"),
        () -> assertEquals(sampleDecimal, row.getDecimal(7, 10, 2).toJavaBigDecimal(), "decimal"),
        () -> assertArrayEquals(sampleBytes, row.getBinary(8), "binary"),
        () -> assertEquals(UTF8String.fromString("hello"), row.getUTF8String(9), "string"),
        () -> assertEquals(10, row.getInt(10), "date as days"),
        () -> assertEquals(1_000_000L, row.getLong(11), "timestamp as micros"),
        () -> assertEquals(2_000_000L, row.getLong(12), "timestamp_ntz as micros"));
    assertAll(
        "null value",
        () -> assertTrue(row.isNullAt(13), "decNull isNull"),
        () -> assertNull(row.getDecimal(13, 10, 2), "decNull value"),
        () -> assertTrue(row.isNullAt(14), "strNull isNull"),
        () -> assertNull(row.getUTF8String(14), "strNull value"));
  }

  @Test
  public void testNestedType_unsupported() {
    StructType schema =
        new StructType(Arrays.asList(new StructField("i", IntegerType.INTEGER, true)));

    Row kernelRow = makePrimitiveRow(schema, Arrays.asList(1));
    InternalRow row = new KernelToSparkRowAdapter(kernelRow);

    assertThrows(UnsupportedOperationException.class, () -> row.getArray(0), "array unsupported");
    assertThrows(UnsupportedOperationException.class, () -> row.getMap(0), "map unsupported");
    assertThrows(
        UnsupportedOperationException.class, () -> row.getStruct(0, 0), "struct unsupported");
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private static Row makePrimitiveRow(StructType schema, List<Object> values) {
    return new Row() {
      @Override
      public StructType getSchema() {
        return schema;
      }

      @Override
      public boolean isNullAt(int ordinal) {
        return values.get(ordinal) == null;
      }

      @Override
      public boolean getBoolean(int ordinal) {
        return (Boolean) values.get(ordinal);
      }

      @Override
      public byte getByte(int ordinal) {
        return (Byte) values.get(ordinal);
      }

      @Override
      public short getShort(int ordinal) {
        return (Short) values.get(ordinal);
      }

      @Override
      public int getInt(int ordinal) {
        return (Integer) values.get(ordinal);
      }

      @Override
      public long getLong(int ordinal) {
        return (Long) values.get(ordinal);
      }

      @Override
      public float getFloat(int ordinal) {
        return (Float) values.get(ordinal);
      }

      @Override
      public double getDouble(int ordinal) {
        return (Double) values.get(ordinal);
      }

      @Override
      public BigDecimal getDecimal(int ordinal) {
        return (BigDecimal) values.get(ordinal);
      }

      @Override
      public String getString(int ordinal) {
        return (String) values.get(ordinal);
      }

      @Override
      public byte[] getBinary(int ordinal) {
        return (byte[]) values.get(ordinal);
      }

      @Override
      public Row getStruct(int ordinal) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ArrayValue getArray(int ordinal) {
        throw new UnsupportedOperationException();
      }

      @Override
      public MapValue getMap(int ordinal) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
