/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink

import scala.jdk.CollectionConverters.SeqHasAsJava

import io.delta.kernel.{types => ktypes}

import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.RowType.RowField
import org.scalatest.funsuite.AnyFunSuite

class ConversionsSuite extends AnyFunSuite {
  private def row(fields: RowField*) = new RowType(fields.asJava)
  private def f(name: String, `type`: LogicalType) = new RowField(name, `type`)

  test("primitive types") {
    val flinkSchema = row(
      f("b", new BooleanType),
      f("i", new IntType),
      f("l", new BigIntType),
      f("f", new FloatType),
      f("d", new DoubleType),
      f("s", new VarCharType(VarCharType.MAX_LENGTH)),
      f("bin", new VarBinaryType(VarBinaryType.MAX_LENGTH)),
      f("dec", new DecimalType(10, 2)))

    val expected = new ktypes.StructType()
      .add("b", ktypes.BooleanType.BOOLEAN, true)
      .add("i", ktypes.IntegerType.INTEGER, true)
      .add("l", ktypes.LongType.LONG, true)
      .add("f", ktypes.FloatType.FLOAT, true)
      .add("d", ktypes.DoubleType.DOUBLE, true)
      .add("s", ktypes.StringType.STRING, true)
      .add("bin", ktypes.BinaryType.BINARY, true)
      .add("dec", new ktypes.DecimalType(10, 2), true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }

  test("struct and nested struct") {
    val inner = row(f("x", new IntType), f("y", new VarCharType(32)))
    val flinkSchema = row(f("id", new BigIntType), f("payload", inner))
    val expectedInner = new ktypes.StructType()
      .add("x", ktypes.IntegerType.INTEGER, true)
      .add("y", ktypes.StringType.STRING, true)

    val expected = new ktypes.StructType()
      .add("id", ktypes.LongType.LONG, true)
      .add("payload", expectedInner, true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }

  test("list") {
    val arrayOfInt = new ArrayType(new IntType)

    val flinkSchema = row(
      f("ids", arrayOfInt),
      f("names", new ArrayType(new VarCharType(VarCharType.MAX_LENGTH))))

    val expected = new ktypes.StructType()
      .add("ids", new ktypes.ArrayType(ktypes.IntegerType.INTEGER, true), true)
      .add("names", new ktypes.ArrayType(ktypes.StringType.STRING, true), true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }

  test("map") {
    val map = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType)

    val flinkSchema = row(f("counters", map))

    val expected = new ktypes.StructType()
      .add(
        "counters",
        new ktypes.MapType(ktypes.StringType.STRING, ktypes.LongType.LONG, true),
        true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }

  test("nested struct in array / map") {
    // ARRAY<ROW<a INT, b STRING>>// ARRAY<ROW<a INT, b STRING>>

    val elementStruct = row(f("a", new IntType), f("b", new VarCharType(20)))
    val arrayOfStruct = new ArrayType(elementStruct)

    // MAP<STRING, ROW<x DOUBLE, y DECIMAL(8,3)>>
    val valueStruct = row(f("x", new DoubleType), f("y", new DecimalType(8, 3)))
    val mapToStruct = new MapType(new VarCharType(VarCharType.MAX_LENGTH), valueStruct)

    val flinkSchema = row(f("events", arrayOfStruct), f("metrics", mapToStruct))

    val elementExpected = new ktypes.StructType()
      .add("a", ktypes.IntegerType.INTEGER, true)
      .add("b", ktypes.StringType.STRING, true)

    val valueExpected = new ktypes.StructType()
      .add("x", ktypes.DoubleType.DOUBLE, true)
      .add("y", new ktypes.DecimalType(8, 3), true)

    val expected = new ktypes.StructType()
      .add("events", new ktypes.ArrayType(elementExpected, true), true)
      .add("metrics", new ktypes.MapType(ktypes.StringType.STRING, valueExpected, true), true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }

  test("nullability") {
    val notNullInt = new IntType(false) // false => NOT NULL in Flink LogicalType

    val nullableStr = new VarCharType(true, VarCharType.MAX_LENGTH)

    val flinkSchema = row(f("id", notNullInt), f("name", nullableStr))

    val expected = new ktypes.StructType()
      .add("id", ktypes.IntegerType.INTEGER, false)
      .add("name", ktypes.StringType.STRING, true)

    val actual = Conversions.FlinkToDelta.schema(flinkSchema)
    assert(expected.equivalent(actual))
  }
}
