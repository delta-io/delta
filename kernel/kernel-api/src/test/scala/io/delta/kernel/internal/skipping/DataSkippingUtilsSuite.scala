/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.expressions.{Column, Expression, Predicate}
import io.delta.kernel.internal.skipping.DataSkippingUtils.constructDataSkippingFilter
import io.delta.kernel.internal.skipping.StatsSchemaHelper.{MAX, MIN, STATS_WITH_COLLATION}
import io.delta.kernel.internal.util.ExpressionUtils.createPredicate
import io.delta.kernel.test.TestUtils
import io.delta.kernel.types._
import io.delta.kernel.types.IntegerType.INTEGER

import org.scalatest.funsuite.AnyFunSuite

class DataSkippingUtilsSuite extends AnyFunSuite with TestUtils {

  def dataSkippingPredicate(
      operator: String,
      children: Seq[Expression],
      referencedColumns: Set[Column]): DataSkippingPredicate = {
    new DataSkippingPredicate(operator, children.asJava, referencedColumns.asJava)
  }

  def dataSkippingPredicate(
      operator: String,
      left: DataSkippingPredicate,
      right: DataSkippingPredicate): DataSkippingPredicate = {
    new DataSkippingPredicate(operator, left, right)
  }

  def dataSkippingPredicateWithCollation(
      operator: String,
      children: Seq[Expression],
      collation: CollationIdentifier,
      referencedColumns: Set[Column]): DataSkippingPredicate = {
    new DataSkippingPredicate(operator, children.asJava, collation, referencedColumns.asJava)
  }

  private def collatedStatsCol(
      collation: CollationIdentifier,
      statName: String,
      fieldName: String): Column = {
    val columnPath =
      Array(STATS_WITH_COLLATION, collation.toString, statName) ++ fieldName.split('.')
    new Column(columnPath)
  }

  /* For struct type checks for equality based on field names & data type only */
  def compareDataTypeUnordered(type1: DataType, type2: DataType): Boolean = (type1, type2) match {
    case (schema1: StructType, schema2: StructType) =>
      val fields1 = schema1.fields().asScala.sortBy(_.getName)
      val fields2 = schema2.fields().asScala.sortBy(_.getName)
      if (fields1.length != fields2.length) {
        false
      } else {
        fields1.zip(fields2).forall { case (field1: StructField, field2: StructField) =>
          field1.getName == field2.getName &&
          compareDataTypeUnordered(field1.getDataType, field2.getDataType)
        }
      }
    case _ =>
      type1 == type2
  }

  def checkPruneStatsSchema(
      inputSchema: StructType,
      referencedCols: Set[Column],
      expectedSchema: StructType): Unit = {
    val prunedSchema = DataSkippingUtils.pruneStatsSchema(inputSchema, referencedCols.asJava)
    assert(
      compareDataTypeUnordered(expectedSchema, prunedSchema),
      s"expected=$expectedSchema\nfound=$prunedSchema")
  }

  test("pruneStatsSchema - multiple basic cases one level of nesting") {
    val nestedField = new StructField(
      "nested",
      new StructType()
        .add("col1", INTEGER)
        .add("col2", INTEGER),
      true)
    val testSchema = new StructType()
      .add(nestedField)
      .add("top_level_col", INTEGER)
    // no columns pruned
    checkPruneStatsSchema(
      testSchema,
      Set(col("top_level_col"), nestedCol("nested.col1"), nestedCol("nested.col2")),
      testSchema)
    // top level column pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("nested.col1"), nestedCol("nested.col2")),
      new StructType().add(nestedField))
    // nested column only one field pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("top_level_col"), nestedCol("nested.col1")),
      new StructType()
        .add("nested", new StructType().add("col1", INTEGER))
        .add("top_level_col", INTEGER))
    // nested column completely pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("top_level_col")),
      new StructType().add("top_level_col", INTEGER))
    // prune all columns
    checkPruneStatsSchema(
      testSchema,
      Set(),
      new StructType())
  }

  test("pruneStatsSchema - 3 levels of nesting") {
    /*
    |--level1: struct
    |   |--level2: struct
    |       |--level3: struct
    |           |--level_4_col: int
    |       |--level_3_col: int
    |   |--level_2_col: int
     */
    val testSchema = new StructType()
      .add(
        "level1",
        new StructType()
          .add(
            "level2",
            new StructType()
              .add(
                "level3",
                new StructType().add("level_4_col", INTEGER))
              .add("level_3_col", INTEGER))
          .add("level_2_col", INTEGER))
    // prune only 4th level col
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level2.level_3_col"), nestedCol("level1.level_2_col")),
      new StructType()
        .add(
          "level1",
          new StructType()
            .add("level2", new StructType().add("level_3_col", INTEGER))
            .add("level_2_col", INTEGER)))
    // prune only 3rd level column
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level2.level3.level_4_col"), nestedCol("level1.level_2_col")),
      new StructType()
        .add(
          "level1",
          new StructType()
            .add(
              "level2",
              new StructType()
                .add(
                  "level3",
                  new StructType().add("level_4_col", INTEGER)))
            .add("level_2_col", INTEGER)))
    // prune 4th and 3rd level column
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level_2_col")),
      new StructType()
        .add(
          "level1",
          new StructType()
            .add("level_2_col", INTEGER)))
    // prune all columns
    checkPruneStatsSchema(
      testSchema,
      Set(),
      new StructType())
  }

  test("pruneStatsSchema - collated min/max columns") {
    val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val unicode = CollationIdentifier.fromString("ICU.UNICODE")
    val testSchema = new StructType()
      .add(
        MIN,
        new StructType()
          .add("s1", StringType.STRING)
          .add("i1", INTEGER)
          .add("i2", INTEGER)
          .add("nested", new StructType().add("s2", StringType.STRING)))
      .add(
        MAX,
        new StructType()
          .add("s1", StringType.STRING)
          .add("i1", INTEGER)
          .add("i2", INTEGER)
          .add("nested", new StructType().add("s2", StringType.STRING)))
      .add(
        STATS_WITH_COLLATION,
        new StructType()
          .add(
            utf8Lcase.toString,
            new StructType()
              .add(
                MIN,
                new StructType()
                  .add("s1", StringType.STRING)
                  .add("nested", new StructType().add("s2", StringType.STRING)))
              .add(
                MAX,
                new StructType()
                  .add("s1", StringType.STRING)
                  .add("nested", new StructType().add("s2", StringType.STRING))))
          .add(
            unicode.toString,
            new StructType()
              .add(
                MIN,
                new StructType()
                  .add("s1", StringType.STRING)
                  .add("nested", new StructType().add("s2", StringType.STRING)))
              .add(
                MAX,
                new StructType()
                  .add("s1", StringType.STRING)
                  .add("nested", new StructType().add("s2", StringType.STRING)))))

    val testCases = Seq(
      (
        Set(nestedCol(s"$MIN.nested.s2"), nestedCol(s"$MAX.i1")),
        new StructType()
          .add(
            MIN,
            new StructType()
              .add("nested", new StructType().add("s2", StringType.STRING)))
          .add(
            MAX,
            new StructType()
              .add("i1", INTEGER))),
      (
        Set(
          collatedStatsCol(utf8Lcase, MIN, "s1"),
          collatedStatsCol(unicode, MAX, "nested.s2")),
        new StructType()
          .add(
            STATS_WITH_COLLATION,
            new StructType()
              .add(
                utf8Lcase.toString,
                new StructType()
                  .add(
                    MIN,
                    new StructType().add("s1", StringType.STRING)))
              .add(
                unicode.toString,
                new StructType()
                  .add(
                    MAX,
                    new StructType().add(
                      "nested",
                      new StructType().add("s2", StringType.STRING)))))),
      (
        Set(
          nestedCol(s"$MIN.i2"),
          collatedStatsCol(utf8Lcase, MAX, "nested.s2"),
          collatedStatsCol(utf8Lcase, MIN, "nested.s2")),
        new StructType()
          .add(
            MIN,
            new StructType()
              .add("i2", INTEGER))
          .add(
            STATS_WITH_COLLATION,
            new StructType()
              .add(
                utf8Lcase.toString,
                new StructType()
                  .add(
                    MIN,
                    new StructType()
                      .add("nested", new StructType().add("s2", StringType.STRING)))
                  .add(
                    MAX,
                    new StructType()
                      .add("nested", new StructType().add("s2", StringType.STRING)))))))

    testCases.foreach { case (referencedCols, expectedSchema) =>
      checkPruneStatsSchema(testSchema, referencedCols, expectedSchema)
    }
  }

  // TODO: add tests for remaining operators
  test("check constructDataSkippingFilter") {
    val testCases = Seq(
      // (schema, predicate, expectedDataSkippingPredicateOpt)
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        createPredicate("<", col("a"), col("b"), Optional.empty[CollationIdentifier]),
        None),
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", StringType.STRING),
        createPredicate("<", col("a"), literal("x"), Optional.empty[CollationIdentifier]),
        Some(dataSkippingPredicate(
          "<",
          Seq(nestedCol(s"$MIN.a"), literal("x")),
          Set(nestedCol(s"$MIN.a"))))),
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", StringType.STRING),
        createPredicate("<", literal("x"), col("a"), Optional.empty[CollationIdentifier]),
        Some(dataSkippingPredicate(
          ">",
          Seq(nestedCol(s"$MAX.a"), literal("x")),
          Set(nestedCol(s"$MAX.a"))))),
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", StringType.STRING),
        createPredicate(">", col("a"), literal("x"), Optional.empty[CollationIdentifier]),
        Some(dataSkippingPredicate(
          ">",
          Seq(nestedCol(s"$MAX.a"), literal("x")),
          Set(nestedCol(s"$MAX.a"))))),
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        createPredicate("=", col("a"), literal(10), Optional.empty[CollationIdentifier]),
        Some(dataSkippingPredicate(
          "AND",
          dataSkippingPredicate(
            "<=",
            Seq(nestedCol(s"$MIN.a"), literal(10)),
            Set(nestedCol(s"$MIN.a"))),
          dataSkippingPredicate(
            ">=",
            Seq(nestedCol(s"$MAX.a"), literal(10)),
            Set(nestedCol(s"$MAX.a")))))),
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate("<", col("a"), literal(10), Optional.empty[CollationIdentifier])),
        Some(dataSkippingPredicate(
          ">=",
          Seq(nestedCol(s"$MAX.a"), literal(10)),
          Set(nestedCol(s"$MAX.a"))))),
      // NOT over AND: NOT(a < 5 AND a > 10) => (max.a >= 5) OR (min.a <= 10)
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate(
            "AND",
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]),
            createPredicate(">", col("a"), literal(10), Optional.empty[CollationIdentifier]),
            Optional.empty[CollationIdentifier])),
        Some(dataSkippingPredicate(
          "OR",
          dataSkippingPredicate(
            ">=",
            Seq(nestedCol(s"$MAX.a"), literal(5)),
            Set(nestedCol(s"$MAX.a"))),
          dataSkippingPredicate(
            "<=",
            Seq(nestedCol(s"$MIN.a"), literal(10)),
            Set(nestedCol(s"$MIN.a")))))),
      // NOT over OR: NOT(a < 5 OR a > 10) => (max.a >= 5) AND (min.a <= 10)
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate(
            "OR",
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]),
            createPredicate(">", col("a"), literal(10), Optional.empty[CollationIdentifier]),
            Optional.empty[CollationIdentifier])),
        Some(dataSkippingPredicate(
          "AND",
          dataSkippingPredicate(
            ">=",
            Seq(nestedCol(s"$MAX.a"), literal(5)),
            Set(nestedCol(s"$MAX.a"))),
          dataSkippingPredicate(
            "<=",
            Seq(nestedCol(s"$MIN.a"), literal(10)),
            Set(nestedCol(s"$MIN.a")))))),
      // NOT over OR with one ineligible leg: NOT(a < b OR a < 5) => NOT(a < b) AND NOT(a < 5)
      // The first leg is ineligible; AND with single leg should return that leg only
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate(
            "OR",
            createPredicate("<", col("a"), col("b"), Optional.empty[CollationIdentifier]),
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]),
            Optional.empty[CollationIdentifier])),
        Some(dataSkippingPredicate(
          ">=",
          Seq(nestedCol(s"$MAX.a"), literal(5)),
          Set(nestedCol(s"$MAX.a"))))),
      // NOT over AND with one ineligible leg: NOT(a < 5 AND a < b)
      // => NOT(a < 5) OR NOT(a < b); since OR needs both legs, expect None
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate(
            "AND",
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]),
            createPredicate("<", col("a"), col("b"), Optional.empty[CollationIdentifier]),
            Optional.empty[CollationIdentifier])),
        None),
      // Double NOT elimination: NOT(NOT(a < 5)) => a < 5 => min.a < 5
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          new Predicate(
            "NOT",
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]))),
        Some(dataSkippingPredicate(
          "<",
          Seq(nestedCol(s"$MIN.a"), literal(5)),
          Set(nestedCol(s"$MIN.a"))))),
      // Cross-column case: NOT(a < 5 OR b > 7) => (max.a >= 5) AND (min.b <= 7)
      (
        new StructType()
          .add("a", IntegerType.INTEGER)
          .add("b", IntegerType.INTEGER),
        new Predicate(
          "NOT",
          createPredicate(
            "OR",
            createPredicate("<", col("a"), literal(5), Optional.empty[CollationIdentifier]),
            createPredicate(">", col("b"), literal(7), Optional.empty[CollationIdentifier]),
            Optional.empty[CollationIdentifier])),
        Some(dataSkippingPredicate(
          "AND",
          dataSkippingPredicate(
            ">=",
            Seq(nestedCol(s"$MAX.a"), literal(5)),
            Set(nestedCol(s"$MAX.a"))),
          dataSkippingPredicate(
            "<=",
            Seq(nestedCol(s"$MIN.b"), literal(7)),
            Set(nestedCol(s"$MIN.b")))))))

    testCases.foreach { case (schema, predicate, expectedDataSkippingPredicateOpt) =>
      val dataSkippingPredicateOpt =
        JavaOptionalOps(constructDataSkippingFilter(predicate, schema)).toScala
      (dataSkippingPredicateOpt, expectedDataSkippingPredicateOpt) match {
        case (Some(dataSkippingPredicate), Some(expectedDataSkippingPredicate)) =>
          assert(dataSkippingPredicate == expectedDataSkippingPredicate)
        case (None, None) => // pass
        case _ =>
          fail(s"Expected $expectedDataSkippingPredicateOpt, found $dataSkippingPredicateOpt")
      }
    }
  }

  test("check constructDataSkippingFilter with collations") {
    val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val unicode = CollationIdentifier.fromString("ICU.UNICODE")

    val testCases = Seq(
      // (schema, predicate, expectedDataSkippingPredicateOpt)
      // Ineligible: both sides are columns
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        createPredicate("<", col("a"), col("b"), Optional.of(utf8Lcase)),
        None),
      // Eligible: a < "m" with collation -> min(a, collation) < "m"
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        createPredicate("<", col("a"), literal("m"), Optional.of(utf8Lcase)), {
          val minA = collatedStatsCol(utf8Lcase, MIN, "a")
          Some(dataSkippingPredicateWithCollation(
            "<",
            Seq(minA, literal("m")),
            utf8Lcase,
            Set(minA)))
        }),
      // Reversed comparator: "m" < a -> max(a, collation) > "m"
      (
        new StructType()
          .add("a", StringType.STRING),
        createPredicate("<", literal("m"), col("a"), Optional.of(utf8Lcase)), {
          val maxA = collatedStatsCol(utf8Lcase, MAX, "a")
          Some(dataSkippingPredicateWithCollation(
            ">",
            Seq(maxA, literal("m")),
            utf8Lcase,
            Set(maxA)))
        }),
      // Direct ">": a > "m" -> max(a, collation) > "m"
      (
        new StructType()
          .add("a", StringType.STRING),
        createPredicate(">", col("a"), literal("m"), Optional.of(utf8Lcase)), {
          val maxA = collatedStatsCol(utf8Lcase, MAX, "a")
          Some(dataSkippingPredicateWithCollation(
            ">",
            Seq(maxA, literal("m")),
            utf8Lcase,
            Set(maxA)))
        }),
      // Equality
      (
        new StructType()
          .add("a", StringType.STRING),
        createPredicate("=", col("a"), literal("abc"), Optional.of(unicode)), {
          val minA = collatedStatsCol(unicode, MIN, "a")
          val maxA = collatedStatsCol(unicode, MAX, "a")
          Some(dataSkippingPredicate(
            "AND",
            dataSkippingPredicateWithCollation("<=", Seq(minA, literal("abc")), unicode, Set(minA)),
            dataSkippingPredicateWithCollation(
              ">=",
              Seq(maxA, literal("abc")),
              unicode,
              Set(maxA))))
        }),
      // NOT over comparator: NOT(a < "m") -> max(a, collation) >= "m"
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate(
          "NOT",
          createPredicate("<", col("a"), literal("m"), Optional.of(utf8Lcase))), {
          val maxA = collatedStatsCol(utf8Lcase, MAX, "a")
          Some(dataSkippingPredicateWithCollation(
            ">=",
            Seq(maxA, literal("m")),
            utf8Lcase,
            Set(maxA)))
        }),
      // NOT over AND
      // NOT(a < "m" AND a > "t") => (max.a >= "m") OR (min.a <= "t")
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate(
          "NOT",
          createPredicate(
            "AND",
            createPredicate("<", col("a"), literal("m"), Optional.of(unicode)),
            createPredicate(">", col("a"), literal("t"), Optional.of(utf8Lcase)),
            Optional.empty[CollationIdentifier])), {
          val unicodeMaxA = collatedStatsCol(unicode, MAX, "a")
          val utf8LcaseMinA = collatedStatsCol(utf8Lcase, MIN, "a")
          Some(dataSkippingPredicate(
            "OR",
            dataSkippingPredicateWithCollation(
              ">=",
              Seq(unicodeMaxA, literal("m")),
              unicode,
              Set(unicodeMaxA)),
            dataSkippingPredicateWithCollation(
              "<=",
              Seq(utf8LcaseMinA, literal("t")),
              utf8Lcase,
              Set(utf8LcaseMinA))))
        }),
      // AND(a < "m" COLLATE UTF8_LCASE, b < 1)
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", IntegerType.INTEGER),
        createPredicate(
          "AND",
          createPredicate("<", col("a"), literal("m"), Optional.of(utf8Lcase)),
          createPredicate("<", col("b"), literal(1), Optional.empty[CollationIdentifier]),
          Optional.empty[CollationIdentifier]), {
          val minA = collatedStatsCol(utf8Lcase, MIN, "a")
          val minB = nestedCol(s"$MIN.b")
          Some(dataSkippingPredicate(
            "AND",
            dataSkippingPredicateWithCollation("<", Seq(minA, literal("m")), utf8Lcase, Set(minA)),
            dataSkippingPredicate("<", Seq(minB, literal(1)), Set(minB))))
        }),
      // Ineligible: non-string column with collation
      (
        new StructType()
          .add("a", IntegerType.INTEGER),
        createPredicate("<", col("a"), literal("m"), Optional.of(utf8Lcase)),
        None))

    testCases.foreach { case (schema, predicate, expectedDataSkippingPredicateOpt) =>
      val dataSkippingPredicateOpt =
        JavaOptionalOps(constructDataSkippingFilter(predicate, schema)).toScala
      (dataSkippingPredicateOpt, expectedDataSkippingPredicateOpt) match {
        case (Some(dataSkippingPredicate), Some(expectedDataSkippingPredicate)) =>
          assert(dataSkippingPredicate == expectedDataSkippingPredicate)
        case (None, None) => // pass
        case _ =>
          fail(s"Expected $expectedDataSkippingPredicateOpt, found $dataSkippingPredicateOpt")
      }
    }
  }
}
