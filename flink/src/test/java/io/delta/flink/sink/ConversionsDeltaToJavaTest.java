/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link Conversions.DeltaToJava}. */
public class ConversionsDeltaToJavaTest {

  // -------------------------------------------------------------------------
  // data(): per-type round-trip — every primitive Delta type in one method
  // -------------------------------------------------------------------------

  @Test
  void testData() {
    // Boolean
    assertEquals(Boolean.TRUE, dataAt(BooleanType.BOOLEAN, true), "BOOLEAN true");
    assertEquals(Boolean.FALSE, dataAt(BooleanType.BOOLEAN, false), "BOOLEAN false");

    // Integers
    assertEquals(Byte.valueOf((byte) 7), dataAt(ByteType.BYTE, (byte) 7), "BYTE");
    assertEquals(Short.valueOf((short) 1234), dataAt(ShortType.SHORT, (short) 1234), "SHORT");
    assertEquals(Integer.valueOf(42), dataAt(IntegerType.INTEGER, 42), "INTEGER");
    assertEquals(
        Long.valueOf(1_000_000_000_000L), dataAt(LongType.LONG, 1_000_000_000_000L), "LONG");

    // Floating point
    assertEquals(Float.valueOf(3.5f), dataAt(FloatType.FLOAT, 3.5f), "FLOAT");
    assertEquals(Double.valueOf(3.14159d), dataAt(DoubleType.DOUBLE, 3.14159d), "DOUBLE");

    // String
    assertEquals("delta", dataAt(StringType.STRING, "delta"), "STRING");

    // Binary — DeltaToJava returns the raw byte[] (unlike FlinkToJava which base64-encodes).
    byte[] bytes = new byte[] {0x01, 0x02, 0x03};
    assertArrayEquals(bytes, (byte[]) dataAt(BinaryType.BINARY, bytes), "BINARY");

    // Decimal
    BigDecimal decValue = new BigDecimal("12.34");
    assertEquals(decValue, dataAt(new DecimalType(10, 2), decValue), "DECIMAL");
  }

  // Temporal types: the production code reads them via getInt(date) / getLong(timestamp), which
  // works on Row implementations backed by Parquet reads but throws on a strict GenericRow (which
  // rejects "wide" access for typed columns). Once the helper is tested against a Parquet-backed
  // Row or GenericRow relaxes its access checks, these can be enabled.
  @Test
  @Disabled("GenericRow rejects rowData.getInt(...) on a DateType column")
  void testDate() {
    assertEquals(Integer.valueOf(19000), dataAt(DateType.DATE, 19000));
  }

  @Test
  @Disabled("GenericRow rejects rowData.getLong(...) on a TimestampType column")
  void testTimestamp() {
    assertEquals(
        Long.valueOf(1_700_000_000_000_000L),
        dataAt(TimestampType.TIMESTAMP, 1_700_000_000_000_000L));
  }

  @Test
  @Disabled("GenericRow rejects rowData.getLong(...) on a TimestampNTZType column")
  void testTimestampNTZ() {
    assertEquals(
        Long.valueOf(1_700_000_000_000_000L),
        dataAt(TimestampNTZType.TIMESTAMP_NTZ, 1_700_000_000_000_000L));
  }

  // -------------------------------------------------------------------------
  // data(): null handling
  // -------------------------------------------------------------------------

  @Test
  void testNullReturnsNull() {
    DataType[] types = {
      BooleanType.BOOLEAN,
      ByteType.BYTE,
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG,
      FloatType.FLOAT,
      DoubleType.DOUBLE,
      StringType.STRING,
      BinaryType.BINARY,
      DateType.DATE,
      TimestampType.TIMESTAMP,
      TimestampNTZType.TIMESTAMP_NTZ,
      new DecimalType(10, 2)
    };
    for (DataType type : types) {
      StructType schema = new StructType().add("col", type);
      Row row = new GenericRow(schema, new HashMap<>()); // empty map → null at every ordinal
      assertNull(Conversions.DeltaToJava.data(schema, row, 0), "expected null for type " + type);
    }
  }

  // -------------------------------------------------------------------------
  // data(): unsupported (complex) types throw
  // -------------------------------------------------------------------------

  @Test
  void testStructTypeThrows() {
    assertUnsupported(new StructType().add("inner", IntegerType.INTEGER));
  }

  @Test
  void testArrayTypeThrows() {
    assertUnsupported(new ArrayType(IntegerType.INTEGER, true));
  }

  @Test
  void testMapTypeThrows() {
    assertUnsupported(new MapType(StringType.STRING, IntegerType.INTEGER, true));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Build a one-column Row of the given type/value and invoke {@code DeltaToJava.data(...)}. */
  private static Object dataAt(DataType type, Object value) {
    StructType schema = new StructType().add("col", type);
    Map<Integer, Object> values = new HashMap<>();
    values.put(0, value);
    Row row = new GenericRow(schema, values);
    return Conversions.DeltaToJava.data(schema, row, 0);
  }

  /** Assert that {@code DeltaToJava.data} throws {@link UnsupportedOperationException}. */
  private static void assertUnsupported(DataType type) {
    StructType schema = new StructType().add("col", type);
    // For complex types we still need a non-null cell so we don't short-circuit on the null
    // path; use a sentinel object — the function should throw before accessing it.
    Map<Integer, Object> values = new HashMap<>();
    values.put(0, new Object());
    Row row = new GenericRow(schema, values);
    assertThrows(
        UnsupportedOperationException.class, () -> Conversions.DeltaToJava.data(schema, row, 0));
  }
}
