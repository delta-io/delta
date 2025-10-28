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

import scala.collection.JavaConverters.{asJavaIterableConverter, setAsJavaSetConverter}

import io.delta.kernel.expressions.{Column, Expression}
import io.delta.kernel.test.TestUtils
import io.delta.kernel.types.{ArrayType, BinaryType, BooleanType, ByteType, CollationIdentifier, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.scalatest.funsuite.AnyFunSuite

class StatsSchemaHelperSuite extends AnyFunSuite with TestUtils {
  val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE.75")
  val unicodeWithVersion = CollationIdentifier.fromString("ICU.UNICODE.74.1")
  val unicodeWithoutVersion = CollationIdentifier.fromString("ICU.UNICODE")

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
        new DataSkippingPredicate(
          "ALWAYS_TRUE",
          java.util.Collections.emptyList[Expression](),
          java.util.Collections.emptySet[Column]()))
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
        new DataSkippingPredicate(
          "ALWAYS_TRUE",
          java.util.Collections.emptyList[Expression](),
          java.util.Collections.emptySet[Column]()))
      assert(
        statsSchema == expectedStatsSchema,
        s"Stats schema mismatch for data schema: $dataSchema")
    }
  }

  test("check getStatsSchema with collations - un-nested mix") {
    val dataSchema = new StructType()
      .add("a", StringType.STRING)
      .add("b", new StringType(utf8Lcase))
      .add("c", IntegerType.INTEGER)

    val expectedStatsSchema = new StructType()
      .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
      .add(
        StatsSchemaHelper.MIN,
        new StructType()
          .add("a", StringType.STRING, true)
          .add("b", new StringType(utf8Lcase), true)
          .add("c", IntegerType.INTEGER, true),
        true)
      .add(
        StatsSchemaHelper.MAX,
        new StructType()
          .add("a", StringType.STRING, true)
          .add("b", new StringType(utf8Lcase), true)
          .add("c", IntegerType.INTEGER, true),
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
              .add(
                StatsSchemaHelper.MIN,
                new StructType().add("a", StringType.STRING, true),
                true)
              .add(
                StatsSchemaHelper.MAX,
                new StructType().add("a", StringType.STRING, true),
                true),
            true)
          .add(
            unicodeWithVersion.toString,
            new StructType()
              .add(
                StatsSchemaHelper.MIN,
                new StructType().add("b", new StringType(utf8Lcase), true),
                true)
              .add(
                StatsSchemaHelper.MAX,
                new StructType().add("b", new StringType(utf8Lcase), true),
                true),
            true),
        true)

    val minAUtf8Lcase = collatedStatsCol(utf8Lcase, StatsSchemaHelper.MIN, "a")
    val maxBUnicodeWithoutVersion =
      collatedStatsCol(unicodeWithoutVersion, StatsSchemaHelper.MAX, "b")
    val maxBUnicodeWithVersion = collatedStatsCol(unicodeWithVersion, StatsSchemaHelper.MAX, "b")
    val minBUnicodeWithVersion = collatedStatsCol(unicodeWithVersion, StatsSchemaHelper.MIN, "b")

    val leftAnd = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        "<",
        java.util.Arrays.asList[Expression](minAUtf8Lcase, literal("aaa")),
        utf8Lcase,
        new java.util.HashSet[Column](java.util.Arrays.asList(minAUtf8Lcase))),
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxBUnicodeWithoutVersion, literal("ccc")),
        unicodeWithoutVersion,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxBUnicodeWithoutVersion))))

    val rightAnd = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxBUnicodeWithVersion, literal("bbb")),
        unicodeWithVersion,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxBUnicodeWithVersion))),
      new DataSkippingPredicate(
        "<",
        java.util.Arrays.asList[Expression](minBUnicodeWithVersion, literal("bbb")),
        unicodeWithVersion,
        new java.util.HashSet[Column](java.util.Arrays.asList(minBUnicodeWithVersion))))

    val predicate = new DataSkippingPredicate(
      "AND",
      leftAnd,
      rightAnd)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, predicate)
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
              .add(StatsSchemaHelper.MIN, expectedCollatedNested, true),
            true)
          .add(
            unicodeWithVersion.toString,
            new StructType()
              .add(StatsSchemaHelper.MAX, expectedCollatedNested, true),
            true),
        true)

    val minXUTF8Lcase = collatedStatsCol(utf8Lcase, StatsSchemaHelper.MIN, "s.x")
    val maxPUnicodeWithoutVersion =
      collatedStatsCol(unicodeWithoutVersion, StatsSchemaHelper.MAX, "s.z.p")
    val maxPUnicodeWithVersion =
      collatedStatsCol(unicodeWithVersion, StatsSchemaHelper.MAX, "s.z.p")

    val left = new DataSkippingPredicate(
      "<",
      java.util.Arrays.asList[Expression](minXUTF8Lcase, literal("m")),
      utf8Lcase,
      new java.util.HashSet[Column](java.util.Arrays.asList(minXUTF8Lcase)))

    val right = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxPUnicodeWithoutVersion, literal("t")),
        unicodeWithoutVersion,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxPUnicodeWithoutVersion))),
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxPUnicodeWithVersion, literal("s")),
        unicodeWithVersion,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxPUnicodeWithVersion))))

    val predicate = new DataSkippingPredicate("AND", left, right)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, predicate)
    assert(statsSchema == expectedStatsSchema)
  }
}
