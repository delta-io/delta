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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for Flink to Delta type conversions. */
class ConversionsTest implements FlinkTypeTests {

  private RowType row(RowField... fields) {
    return new RowType(Arrays.asList(fields));
  }

  private RowField f(String name, LogicalType type) {
    return new RowField(name, type);
  }

  @Test
  void testPrimitiveTypes() {
    RowType flinkSchema =
        row(
            f("b", new BooleanType()),
            f("ti", new TinyIntType()),
            f("sm", new SmallIntType()),
            f("i", new IntType()),
            f("l", new BigIntType()),
            f("f", new FloatType()),
            f("d", new DoubleType()),
            f("s", new VarCharType(VarCharType.MAX_LENGTH)),
            f("bin", new VarBinaryType(VarBinaryType.MAX_LENGTH)),
            f("dec", new DecimalType(10, 2)),
            f("iym", new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, 4)),
            f("idt", new DayTimeIntervalType(DayTimeResolution.DAY, 3, 6)));

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add("b", io.delta.kernel.types.BooleanType.BOOLEAN, true)
            .add("ti", io.delta.kernel.types.ByteType.BYTE, true)
            .add("sm", io.delta.kernel.types.ShortType.SHORT, true)
            .add("i", io.delta.kernel.types.IntegerType.INTEGER, true)
            .add("l", io.delta.kernel.types.LongType.LONG, true)
            .add("f", io.delta.kernel.types.FloatType.FLOAT, true)
            .add("d", io.delta.kernel.types.DoubleType.DOUBLE, true)
            .add("s", io.delta.kernel.types.StringType.STRING, true)
            .add("bin", io.delta.kernel.types.BinaryType.BINARY, true)
            .add("dec", new io.delta.kernel.types.DecimalType(10, 2), true)
            .add("iym", io.delta.kernel.types.IntegerType.INTEGER, true)
            .add("idt", io.delta.kernel.types.LongType.LONG, true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @Test
  void testStructAndNestedStruct() {
    RowType inner = row(f("x", new IntType()), f("y", new VarCharType(32)));
    RowType flinkSchema = row(f("id", new BigIntType()), f("payload", inner));

    io.delta.kernel.types.StructType expectedInner =
        new io.delta.kernel.types.StructType()
            .add("x", io.delta.kernel.types.IntegerType.INTEGER, true)
            .add("y", io.delta.kernel.types.StringType.STRING, true);

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add("id", io.delta.kernel.types.LongType.LONG, true)
            .add("payload", expectedInner, true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @Test
  void testList() {
    ArrayType arrayOfInt = new ArrayType(new IntType());

    RowType flinkSchema =
        row(
            f("ids", arrayOfInt),
            f("names", new ArrayType(new VarCharType(VarCharType.MAX_LENGTH))));

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add(
                "ids",
                new io.delta.kernel.types.ArrayType(
                    io.delta.kernel.types.IntegerType.INTEGER, true),
                true)
            .add(
                "names",
                new io.delta.kernel.types.ArrayType(io.delta.kernel.types.StringType.STRING, true),
                true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @Test
  void testMap() {
    MapType map = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType());

    RowType flinkSchema = row(f("counters", map));

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add(
                "counters",
                new io.delta.kernel.types.MapType(
                    io.delta.kernel.types.StringType.STRING,
                    io.delta.kernel.types.LongType.LONG,
                    true),
                true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @Test
  void testNestedStructInArrayMap() {
    // ARRAY<ROW<a INT, b STRING>>
    RowType elementStruct = row(f("a", new IntType()), f("b", new VarCharType(20)));
    ArrayType arrayOfStruct = new ArrayType(elementStruct);

    // MAP<STRING, ROW<x DOUBLE, y DECIMAL(8,3)>>
    RowType valueStruct = row(f("x", new DoubleType()), f("y", new DecimalType(8, 3)));
    MapType mapToStruct = new MapType(new VarCharType(VarCharType.MAX_LENGTH), valueStruct);

    RowType flinkSchema = row(f("events", arrayOfStruct), f("metrics", mapToStruct));

    io.delta.kernel.types.StructType elementExpected =
        new io.delta.kernel.types.StructType()
            .add("a", io.delta.kernel.types.IntegerType.INTEGER, true)
            .add("b", io.delta.kernel.types.StringType.STRING, true);

    io.delta.kernel.types.StructType valueExpected =
        new io.delta.kernel.types.StructType()
            .add("x", io.delta.kernel.types.DoubleType.DOUBLE, true)
            .add("y", new io.delta.kernel.types.DecimalType(8, 3), true);

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add("events", new io.delta.kernel.types.ArrayType(elementExpected, true), true)
            .add(
                "metrics",
                new io.delta.kernel.types.MapType(
                    io.delta.kernel.types.StringType.STRING, valueExpected, true),
                true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @Test
  void testNullability() {
    IntType notNullInt = new IntType(false); // false => NOT NULL in Flink LogicalType

    VarCharType nullableStr = new VarCharType(true, VarCharType.MAX_LENGTH);

    RowType flinkSchema = row(f("id", notNullInt), f("name", nullableStr));

    io.delta.kernel.types.StructType expected =
        new io.delta.kernel.types.StructType()
            .add("id", io.delta.kernel.types.IntegerType.INTEGER, false)
            .add("name", io.delta.kernel.types.StringType.STRING, true);

    io.delta.kernel.types.StructType actual = Conversions.FlinkToDelta.schema(flinkSchema);
    assertTrue(expected.equivalent(actual));
  }

  @TestAllFlinkTypes
  void testPrimitiveDataNotNull(LogicalType primitiveType) {
    RowType flinkType = RowType.of(new LogicalType[] {primitiveType}, new String[] {"id"});
    DataType deltaType = Conversions.FlinkToDelta.dataType(primitiveType);
    StructType schema = new StructType().add("id", deltaType);
    RowData withNull = GenericRowData.of(new Object[] {null});
    assertEquals(Literal.ofNull(deltaType), Conversions.FlinkToDelta.data(schema, withNull, 0));
    assertNull(Conversions.FlinkToJava.data(flinkType, withNull, 0));
  }
}
