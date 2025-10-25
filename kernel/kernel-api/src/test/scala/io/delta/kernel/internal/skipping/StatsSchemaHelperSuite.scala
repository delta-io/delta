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
package io.delta.kernel.internal.skipping

import scala.collection.JavaConverters.setAsJavaSetConverter

import io.delta.kernel.types.{ArrayType, BinaryType, BooleanType, ByteType, CollationIdentifier, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.scalatest.funsuite.AnyFunSuite

class StatsSchemaHelperSuite extends AnyFunSuite {
  val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
  val unicode = CollationIdentifier.fromString("ICU.UNICODE")

  test("check getStatsSchema for supported data types") {
    val testCases = Seq(
      (
        new StructType().add("a", IntegerType.INTEGER),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("a", IntegerType.INTEGER, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("a", IntegerType.INTEGER, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("a", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("b", StringType.STRING),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("b", StringType.STRING, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("b", StringType.STRING, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("b", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("c", ByteType.BYTE),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("c", ByteType.BYTE, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("c", ByteType.BYTE, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("c", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("d", ShortType.SHORT),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("d", ShortType.SHORT, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("d", ShortType.SHORT, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("d", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("e", LongType.LONG),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("e", LongType.LONG, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("e", LongType.LONG, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("e", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("f", FloatType.FLOAT),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("f", FloatType.FLOAT, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("f", FloatType.FLOAT, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("f", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("g", DoubleType.DOUBLE),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("g", DoubleType.DOUBLE, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("g", DoubleType.DOUBLE, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("g", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("h", DateType.DATE),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("h", DateType.DATE, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("h", DateType.DATE, true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("h", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("i", TimestampType.TIMESTAMP),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(
            StatsSchemaHelper.MIN,
            new StructType().add("i", TimestampType.TIMESTAMP, true),
            true)
          .add(
            StatsSchemaHelper.MAX,
            new StructType().add("i", TimestampType.TIMESTAMP, true),
            true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("i", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("j", TimestampNTZType.TIMESTAMP_NTZ),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(
            StatsSchemaHelper.MIN,
            new StructType().add("j", TimestampNTZType.TIMESTAMP_NTZ, true),
            true)
          .add(
            StatsSchemaHelper.MAX,
            new StructType().add("j", TimestampNTZType.TIMESTAMP_NTZ, true),
            true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("j", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType().add("k", new DecimalType(20, 5)),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("k", new DecimalType(20, 5), true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("k", new DecimalType(20, 5), true), true)
          .add(StatsSchemaHelper.NULL_COUNT, new StructType().add("k", LongType.LONG, true), true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)))

    testCases.foreach { case (dataSchema, expectedStatsSchema) =>
      val statsSchema = StatsSchemaHelper.getStatsSchema(
        dataSchema,
        Set.empty[CollationIdentifier].asJava)
      assert(
        statsSchema == expectedStatsSchema,
        s"Stats schema mismatch for data schema: $dataSchema")
    }
  }

  test("check getStatsSchema with mix of supported and unsupported data types") {
    val testCases = Seq(
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", BinaryType.BINARY)
          .add("c", new ArrayType(LongType.LONG, true)),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(StatsSchemaHelper.MIN, new StructType().add("a", IntegerType.INTEGER, true), true)
          .add(StatsSchemaHelper.MAX, new StructType().add("a", IntegerType.INTEGER, true), true)
          .add(
            StatsSchemaHelper.NULL_COUNT,
            new StructType()
              .add("a", LongType.LONG, true)
              .add("b", LongType.LONG, true)
              .add("c", LongType.LONG, true),
            true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      (
        new StructType()
          .add(
            "s",
            new StructType()
              .add("s1", StringType.STRING)
              .add("s2", BooleanType.BOOLEAN)),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(
            StatsSchemaHelper.MIN,
            new StructType()
              .add("s", new StructType().add("s1", StringType.STRING, true), true),
            true)
          .add(
            StatsSchemaHelper.MAX,
            new StructType()
              .add("s", new StructType().add("s1", StringType.STRING, true), true),
            true)
          .add(
            StatsSchemaHelper.NULL_COUNT,
            new StructType()
              .add(
                "s",
                new StructType()
                  .add("s1", LongType.LONG, true)
                  .add("s2", LongType.LONG, true),
                true),
            true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      // Un-nested array/map alongside a supported type
      (
        new StructType()
          .add("arr", new ArrayType(IntegerType.INTEGER, true))
          .add("mp", new MapType(StringType.STRING, LongType.LONG, true))
          .add("z", DoubleType.DOUBLE),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(
            StatsSchemaHelper.MIN,
            new StructType().add("z", DoubleType.DOUBLE, true),
            true)
          .add(
            StatsSchemaHelper.MAX,
            new StructType().add("z", DoubleType.DOUBLE, true),
            true)
          .add(
            StatsSchemaHelper.NULL_COUNT,
            new StructType()
              .add("arr", LongType.LONG, true)
              .add("mp", LongType.LONG, true)
              .add("z", LongType.LONG, true),
            true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)),
      // Nested array/map inside a struct; empty struct preserved in min/max
      (
        new StructType()
          .add(
            "s",
            new StructType()
              .add("arr", new ArrayType(StringType.STRING, true))
              .add("mp", new MapType(IntegerType.INTEGER, StringType.STRING, true)))
          .add("k", StringType.STRING),
        new StructType()
          .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
          .add(
            StatsSchemaHelper.MIN,
            new StructType()
              .add("s", new StructType(), true)
              .add("k", StringType.STRING, true),
            true)
          .add(
            StatsSchemaHelper.MAX,
            new StructType()
              .add("s", new StructType(), true)
              .add("k", StringType.STRING, true),
            true)
          .add(
            StatsSchemaHelper.NULL_COUNT,
            new StructType()
              .add(
                "s",
                new StructType()
                  .add("arr", LongType.LONG, true)
                  .add("mp", LongType.LONG, true),
                true)
              .add("k", LongType.LONG, true),
            true)
          .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)))

    testCases.foreach { case (dataSchema, expectedStatsSchema) =>
      val statsSchema = StatsSchemaHelper.getStatsSchema(
        dataSchema,
        Set.empty[CollationIdentifier].asJava)
      assert(
        statsSchema == expectedStatsSchema,
        s"Stats schema mismatch for data schema: $dataSchema")
    }
  }

  test("check getStatsSchema with collations - un-nested mix") {
    val dataSchema = new StructType()
      .add("a", StringType.STRING)
      .add("b", IntegerType.INTEGER)
      .add("c", BinaryType.BINARY)

    val collations = Set(utf8Lcase)

    val expectedCollatedMinMax = new StructType().add("a", StringType.STRING, true)

    val expectedStatsSchema = new StructType()
      .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
      .add(
        StatsSchemaHelper.MIN,
        new StructType()
          .add("a", StringType.STRING, true)
          .add("b", IntegerType.INTEGER, true),
        true)
      .add(
        StatsSchemaHelper.MAX,
        new StructType()
          .add("a", StringType.STRING, true)
          .add("b", IntegerType.INTEGER, true),
        true)
      .add(
        StatsSchemaHelper.NULL_COUNT,
        new StructType()
          .add("a", LongType.LONG, true)
          .add("b", LongType.LONG, true)
          .add("c", LongType.LONG, true),
        true)
      .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)
      .add(
        StatsSchemaHelper.STATS_WITH_COLLATION,
        new StructType()
          .add(
            utf8Lcase.toString,
            new StructType()
              .add(StatsSchemaHelper.MIN, expectedCollatedMinMax, true)
              .add(StatsSchemaHelper.MAX, expectedCollatedMinMax, true),
            true),
        true)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, collations.asJava)
    assert(statsSchema == expectedStatsSchema)
  }

  test("check getStatsSchema with collations - nested mix and multiple collations") {
    val dataSchema = new StructType()
      .add(
        "s",
        new StructType()
          .add("x", StringType.STRING)
          .add("y", IntegerType.INTEGER)
          .add("z", new StructType().add("p", StringType.STRING).add("q", DoubleType.DOUBLE)))
      .add("arr", new ArrayType(StringType.STRING, true))
      .add("mp", new MapType(StringType.STRING, StringType.STRING, true))

    val collations = Set(utf8Lcase, CollationIdentifier.SPARK_UTF8_BINARY)

    val expectedCollatedNested = new StructType()
      .add(
        "s",
        new StructType()
          .add("x", StringType.STRING, true)
          .add("z", new StructType().add("p", StringType.STRING, true), true),
        true)

    val expectedStatsSchema = new StructType()
      .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
      .add(
        StatsSchemaHelper.MIN,
        new StructType()
          .add(
            "s",
            new StructType()
              .add("x", StringType.STRING, true)
              .add("y", IntegerType.INTEGER, true)
              .add(
                "z",
                new StructType()
                  .add("p", StringType.STRING, true)
                  .add("q", DoubleType.DOUBLE, true),
                true),
            true),
        true)
      .add(
        StatsSchemaHelper.MAX,
        new StructType()
          .add(
            "s",
            new StructType()
              .add("x", StringType.STRING, true)
              .add("y", IntegerType.INTEGER, true)
              .add(
                "z",
                new StructType()
                  .add("p", StringType.STRING, true)
                  .add("q", DoubleType.DOUBLE, true),
                true),
            true),
        true)
      .add(
        StatsSchemaHelper.NULL_COUNT,
        new StructType()
          .add(
            "s",
            new StructType()
              .add("x", LongType.LONG, true)
              .add("y", LongType.LONG, true)
              .add(
                "z",
                new StructType()
                  .add("p", LongType.LONG, true)
                  .add("q", LongType.LONG, true),
                true),
            true)
          .add("arr", LongType.LONG, true)
          .add("mp", LongType.LONG, true),
        true)
      .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)
      .add(
        StatsSchemaHelper.STATS_WITH_COLLATION,
        new StructType()
          .add(
            utf8Lcase.toString,
            new StructType()
              .add(StatsSchemaHelper.MIN, expectedCollatedNested, true)
              .add(StatsSchemaHelper.MAX, expectedCollatedNested, true),
            true)
          .add(
            CollationIdentifier.SPARK_UTF8_BINARY.toString,
            new StructType()
              .add(StatsSchemaHelper.MIN, expectedCollatedNested, true)
              .add(StatsSchemaHelper.MAX, expectedCollatedNested, true),
            true),
        true)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, collations.asJava)
    assert(statsSchema == expectedStatsSchema)
  }

  test("check getStatsSchema with collations - no eligible string columns") {
    val dataSchema = new StructType()
      .add("a", IntegerType.INTEGER)
      .add("b", new ArrayType(StringType.STRING, true))
      .add("c", new MapType(StringType.STRING, LongType.LONG, true))

    val collations = Set(utf8Lcase, unicode, CollationIdentifier.SPARK_UTF8_BINARY)

    val expectedStatsSchema = new StructType()
      .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
      .add(
        StatsSchemaHelper.MIN,
        new StructType().add("a", IntegerType.INTEGER, true),
        true)
      .add(
        StatsSchemaHelper.MAX,
        new StructType().add("a", IntegerType.INTEGER, true),
        true)
      .add(
        StatsSchemaHelper.NULL_COUNT,
        new StructType()
          .add("a", LongType.LONG, true)
          .add("b", LongType.LONG, true)
          .add("c", LongType.LONG, true),
        true)
      .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, collations.asJava)
    assert(statsSchema == expectedStatsSchema)
  }
}
