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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link Conversions.FlinkToJava}. */
public class ConversionsFlinkToJavaTest implements FlinkTypeTests {

  // -------------------------------------------------------------------------
  // data(): per-type round-trip — every primitive Flink type in one method
  // -------------------------------------------------------------------------

  @Test
  void testData() {
    // Boolean
    assertEquals(Boolean.TRUE, dataAt(new BooleanType(), Boolean.TRUE), "BOOLEAN true");
    assertEquals(Boolean.FALSE, dataAt(new BooleanType(), Boolean.FALSE), "BOOLEAN false");

    // Integers
    assertEquals(Byte.valueOf((byte) 7), dataAt(new TinyIntType(), (byte) 7), "TINYINT");
    assertEquals(Short.valueOf((short) 1234), dataAt(new SmallIntType(), (short) 1234), "SMALLINT");
    assertEquals(Integer.valueOf(42), dataAt(new IntType(), 42), "INTEGER");
    assertEquals(
        Long.valueOf(1_000_000_000_000L), dataAt(new BigIntType(), 1_000_000_000_000L), "BIGINT");

    // Floating point
    assertEquals(Float.valueOf(3.5f), dataAt(new FloatType(), 3.5f), "FLOAT");
    assertEquals(Double.valueOf(3.14159d), dataAt(new DoubleType(), 3.14159d), "DOUBLE");

    // Character
    assertEquals("hello", dataAt(new CharType(5), StringData.fromString("hello")), "CHAR");
    assertEquals(
        "delta",
        dataAt(new VarCharType(VarCharType.MAX_LENGTH), StringData.fromString("delta")),
        "VARCHAR");

    // Binary — FlinkToJava base64-encodes (unlike DeltaToJava which returns raw byte[]).
    assertEquals("AQID", dataAt(new BinaryType(3), new byte[] {0x01, 0x02, 0x03}), "BINARY base64");
    assertEquals(
        "ECAwQA==",
        dataAt(new VarBinaryType(VarBinaryType.MAX_LENGTH), new byte[] {0x10, 0x20, 0x30, 0x40}),
        "VARBINARY base64");

    // Decimal
    DecimalData decValue = DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2);
    assertEquals(decValue, dataAt(new DecimalType(10, 2), decValue), "DECIMAL");

    // Temporal
    assertEquals(Integer.valueOf(19000), dataAt(new DateType(), 19000), "DATE");
    assertEquals(Integer.valueOf(123456), dataAt(new TimeType(3), 123456), "TIME");
    // Timestamps are read via getLong; callers pre-store them as long microseconds.
    assertEquals(
        Long.valueOf(1_700_000_000_000_000L),
        dataAt(new TimestampType(6), 1_700_000_000_000_000L),
        "TIMESTAMP_WITHOUT_TIME_ZONE");
    assertEquals(
        Long.valueOf(1_700_000_000_000_000L),
        dataAt(new LocalZonedTimestampType(6), 1_700_000_000_000_000L),
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE");

    // Intervals
    assertEquals(
        Integer.valueOf(15),
        dataAt(new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, 4), 15),
        "INTERVAL_YEAR_MONTH");
    assertEquals(
        Long.valueOf(86_400_000L),
        dataAt(new DayTimeIntervalType(DayTimeResolution.DAY, 3, 6), 86_400_000L),
        "INTERVAL_DAY_TIME");
  }

  // -------------------------------------------------------------------------
  // data(): null handling — parameterized across all primitive types
  // -------------------------------------------------------------------------

  @TestAllFlinkTypes
  void testPrimitiveDataNull(LogicalType primitiveType) {
    RowType flinkType = RowType.of(new LogicalType[] {primitiveType}, new String[] {"id"});
    RowData withNull = GenericRowData.of(new Object[] {null});
    assertNull(Conversions.FlinkToJava.data(flinkType, withNull, 0));
  }

  // -------------------------------------------------------------------------
  // data(): unsupported types
  // -------------------------------------------------------------------------

  @Test
  void testUnsupportedTypeThrows() {
    RowType rowType =
        RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"arr"});
    RowData row = GenericRowData.of(new Object[] {new Object()});
    assertThrows(
        UnsupportedOperationException.class, () -> Conversions.FlinkToJava.data(rowType, row, 0));
  }

  // -------------------------------------------------------------------------
  // partitionValues(): shape and null behavior
  // -------------------------------------------------------------------------

  @Test
  void testPartitionValuesReturnsOptionalForEachColumn() {
    RowType schema =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "part"});
    RowData row = GenericRowData.of(42, StringData.fromString("p0"));

    Map<String, Object> result =
        Conversions.FlinkToJava.partitionValues(schema, List.of("id", "part"), row);

    assertEquals(2, result.size());
    assertEquals(Optional.of(42), result.get("id"));
    assertEquals(Optional.of("p0"), result.get("part"));
  }

  @Test
  void testPartitionValuesWrapsNullAsEmptyOptional() {
    RowType schema = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    RowData row = GenericRowData.of(new Object[] {null});

    Map<String, Object> result =
        Conversions.FlinkToJava.partitionValues(schema, List.of("id"), row);

    assertEquals(1, result.size());
    assertEquals(Optional.empty(), result.get("id"));
  }

  @Test
  void testPartitionValuesEmptyColumnsReturnsEmptyMap() {
    RowType schema = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    RowData row = GenericRowData.of(42);

    Map<String, Object> result = Conversions.FlinkToJava.partitionValues(schema, List.of(), row);

    assertTrue(result.isEmpty());
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Wrap {@code type} in a one-column RowType and invoke {@link Conversions.FlinkToJava#data}. */
  private static Object dataAt(LogicalType type, Object value) {
    RowType schema = RowType.of(new LogicalType[] {type}, new String[] {"col"});
    RowData row = GenericRowData.of(value);
    return Conversions.FlinkToJava.data(schema, row, 0);
  }
}
