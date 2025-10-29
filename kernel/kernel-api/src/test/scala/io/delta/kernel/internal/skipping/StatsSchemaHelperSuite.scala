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
import io.delta.kernel.types.{ArrayType, BinaryType, BooleanType, ByteType, CollationIdentifier, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

import org.scalatest.funsuite.AnyFunSuite

class StatsSchemaHelperSuite extends AnyFunSuite with TestUtils {
  val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE.75")
  val unicode = CollationIdentifier.fromString("ICU.UNICODE.74.1")

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
      assert(statsSchema == expectedStatsSchema)
    }
  }

  test("check getStatsSchema with collations - un-nested mix") {
    val dataSchema = new StructType()
      .add("a", StringType.STRING)
      .add("b", new StringType(utf8Lcase))
      .add("c", IntegerType.INTEGER)

    val commonStatsSchema = new StructType()
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

    def statsField(
        collation: String,
        dataSchema: StructType,
        statTypes: Seq[String]): StructField = {
      var structType = new StructType()
      statTypes.foreach { stat =>
        structType = structType.add(stat, dataSchema, true)
      }
      new StructField(collation, structType, true)
    }

    val utf8LcaseStatsSchema = new StructField(
      utf8Lcase.toString,
      new StructType().add(
        StatsSchemaHelper.MIN,
        new StructType().add("a", StringType.STRING, true),
        true),
      true)

    val unicodeStatsSchemas = Seq(
      statsField(
        unicode.toString,
        new StructType().add("b", new StringType(utf8Lcase)),
        Seq(StatsSchemaHelper.MAX, StatsSchemaHelper.MIN)),
      statsField(
        unicode.toString,
        new StructType().add("b", new StringType(utf8Lcase)),
        Seq(StatsSchemaHelper.MIN, StatsSchemaHelper.MAX)))

    // Since we use HashSet to collect collations from predicates, the order is not guaranteed.
    val expectedSchemaStats =
      Seq(utf8LcaseStatsSchema).flatMap { utf8LcaseStats =>
        unicodeStatsSchemas.flatMap { unicodeStats =>
          Seq(
            commonStatsSchema.add(
              StatsSchemaHelper.STATS_WITH_COLLATION,
              new StructType().add(utf8LcaseStats).add(unicodeStats),
              true),
            commonStatsSchema.add(
              StatsSchemaHelper.STATS_WITH_COLLATION,
              new StructType().add(unicodeStats).add(utf8LcaseStats),
              true))
        }
      }

    val minAUtf8Lcase = collatedStatsCol(utf8Lcase, StatsSchemaHelper.MIN, "a")
    val maxBUnicode = collatedStatsCol(unicode, StatsSchemaHelper.MAX, "b")
    val minBUnicode = collatedStatsCol(unicode, StatsSchemaHelper.MIN, "b")

    val leftAnd = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        "<",
        java.util.Arrays.asList[Expression](minAUtf8Lcase, literal("aaa")),
        utf8Lcase,
        new java.util.HashSet[Column](java.util.Arrays.asList(minAUtf8Lcase))),
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxBUnicode, literal("ccc")),
        unicode,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxBUnicode))))

    val rightAnd = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxBUnicode, literal("bbb")),
        unicode,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxBUnicode))),
      new DataSkippingPredicate(
        "<",
        java.util.Arrays.asList[Expression](minBUnicode, literal("bbb")),
        unicode,
        new java.util.HashSet[Column](java.util.Arrays.asList(minBUnicode))))

    val predicate = new DataSkippingPredicate(
      "AND",
      leftAnd,
      rightAnd)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, predicate)
    assert(expectedSchemaStats.contains(statsSchema))
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

    val xField = new StructType()
      .add(
        "s",
        new StructType()
          .add("x", StringType.STRING, true))
    val pField = new StructType()
      .add(
        "s",
        new StructType()
          .add(
            "z",
            new StructType()
              .add("p", StringType.STRING, true),
            true))

    val commonStatsSchema = new StructType()
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

    val utf8LcaseStatsSchema = new StructField(
      utf8Lcase.toString,
      new StructType()
        .add(StatsSchemaHelper.MIN, xField, true),
      true)
    val unicodeStatsSchema = new StructField(
      unicode.toString,
      new StructType()
        .add(StatsSchemaHelper.MAX, pField, true),
      true)

    // Since we use HashSet to collect collations from predicates, the order is not guaranteed.
    val expectedStatsSchema1 = commonStatsSchema
      .add(
        StatsSchemaHelper.STATS_WITH_COLLATION,
        new StructType().add(utf8LcaseStatsSchema).add(unicodeStatsSchema),
        true)
    val expectedStatsSchema2 = commonStatsSchema
      .add(
        StatsSchemaHelper.STATS_WITH_COLLATION,
        new StructType().add(unicodeStatsSchema).add(utf8LcaseStatsSchema),
        true)

    val minXUTF8Lcase = collatedStatsCol(utf8Lcase, StatsSchemaHelper.MIN, "s.x")
    val maxPUnicode =
      collatedStatsCol(unicode, StatsSchemaHelper.MAX, "s.z.p")

    val left = new DataSkippingPredicate(
      "<",
      java.util.Arrays.asList[Expression](minXUTF8Lcase, literal("m")),
      utf8Lcase,
      new java.util.HashSet[Column](java.util.Arrays.asList(minXUTF8Lcase)))

    val right = new DataSkippingPredicate(
      "AND",
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxPUnicode, literal("t")),
        unicode,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxPUnicode))),
      new DataSkippingPredicate(
        ">",
        java.util.Arrays.asList[Expression](maxPUnicode, literal("s")),
        unicode,
        new java.util.HashSet[Column](java.util.Arrays.asList(maxPUnicode))))

    val predicate = new DataSkippingPredicate("AND", left, right)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, predicate)
    assert(statsSchema == expectedStatsSchema1 || statsSchema == expectedStatsSchema2)
  }

  test("check getStatsSchema - data skipping predicate with both collated and" +
    " non-collated parts") {
    val dataSchema = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", StringType.STRING)

    val minC1 = nestedCol(s"${StatsSchemaHelper.MIN}.c1")
    val maxC2Utf8 = collatedStatsCol(utf8Lcase, StatsSchemaHelper.MAX, "c2")

    val left = new DataSkippingPredicate(
      "<",
      java.util.Arrays.asList[Expression](minC1, literal(1)),
      new java.util.HashSet[Column](java.util.Arrays.asList(minC1)))

    val right = new DataSkippingPredicate(
      ">",
      java.util.Arrays.asList[Expression](maxC2Utf8, literal("a")),
      utf8Lcase,
      new java.util.HashSet[Column](java.util.Arrays.asList(maxC2Utf8)))

    val predicate = new DataSkippingPredicate("AND", left, right)

    val expectedStatsSchema = new StructType()
      .add(StatsSchemaHelper.NUM_RECORDS, LongType.LONG, true)
      .add(
        StatsSchemaHelper.MIN,
        new StructType()
          .add("c1", IntegerType.INTEGER, true)
          .add("c2", StringType.STRING, true),
        true)
      .add(
        StatsSchemaHelper.MAX,
        new StructType()
          .add("c1", IntegerType.INTEGER, true)
          .add("c2", StringType.STRING, true),
        true)
      .add(
        StatsSchemaHelper.NULL_COUNT,
        new StructType()
          .add("c1", LongType.LONG, true)
          .add("c2", LongType.LONG, true),
        true)
      .add(StatsSchemaHelper.TIGHT_BOUNDS, BooleanType.BOOLEAN, true)
      .add(
        StatsSchemaHelper.STATS_WITH_COLLATION,
        new StructType().add(
          utf8Lcase.toString,
          new StructType().add(
            StatsSchemaHelper.MAX,
            new StructType().add("c2", StringType.STRING, true),
            true),
          true),
        true)

    val statsSchema = StatsSchemaHelper.getStatsSchema(dataSchema, predicate)
    assert(statsSchema == expectedStatsSchema)
  }

}
