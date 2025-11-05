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
package io.delta.kernel.internal.util

import java.util

import scala.collection.JavaConverters._

import io.delta.kernel.expressions._
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.util.PartitionUtils._
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class PartitionUtilsSuite extends AnyFunSuite {
  private val utf8Lcase = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
  private val unicode = CollationIdentifier.fromString("ICU.UNICODE")

  // Table schema
  // Data columns: data1: int, data2: string, date3: struct(data31: boolean, data32: long)
  // Partition columns: part1: int, part2: date, part3: string
  val tableSchema = new StructType()
    .add("data1", IntegerType.INTEGER)
    .add("data2", StringType.STRING)
    .add(
      "data3",
      new StructType()
        .add("data31", BooleanType.BOOLEAN)
        .add("data32", LongType.LONG))
    .add("part1", IntegerType.INTEGER)
    .add("part2", DateType.DATE)
    .add("part3", StringType.STRING)

  private val partitionColsMetadata = new util.HashMap[String, StructField]() {
    {
      put("part1", tableSchema.get("part1"))
      put("part2", tableSchema.get("part2"))
      put("part3", tableSchema.get("part3"))
    }
  }

  private val partitionCols: java.util.Set[String] = partitionColsMetadata.keySet()

  // Test cases for verifying partition of predicate into data and partition predicates
  // Map entry format (predicate -> (partition predicate, data predicate)
  val partitionTestCases = Map[Predicate, (String, String)](
    // single predicate on a data column
    predicate("=", col("data1"), ofInt(12)) ->
      ("ALWAYS_TRUE()", "(column(`data1`) = 12)"),
    // single predicate with default collation on a data column
    predicate("=", col("data2"), ofString("12"), CollationIdentifier.SPARK_UTF8_BINARY) ->
      ("ALWAYS_TRUE()", "(column(`data2`) = 12 COLLATE SPARK.UTF8_BINARY)"),
    // single predicate with non-default collation on a data column
    predicate("=", col("data2"), ofString("12"), utf8Lcase) ->
      ("ALWAYS_TRUE()", "(column(`data2`) = 12 COLLATE SPARK.UTF8_LCASE)"),
    predicate("=", col("data2"), ofString("12"), unicode) ->
      ("ALWAYS_TRUE()", "(column(`data2`) = 12 COLLATE ICU.UNICODE)"),
    // multiple predicates on data columns joined with AND
    predicate(
      "AND",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("data2"), ofString("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`data1`) = 12) AND (column(`data2`) >= sss))"),
    // multiple predicates with collation on data columns joined with AND
    predicate(
      "AND",
      predicate("=", col("data2"), ofString("12")),
      predicate(">=", col("data2"), ofString("sss"), utf8Lcase)) ->
      (
        "ALWAYS_TRUE()",
        "((column(`data2`) = 12) AND (column(`data2`) >= sss COLLATE SPARK.UTF8_LCASE))"),
    // multiple predicates with collation on data columns joined with AND
    predicate(
      "AND",
      predicate("=", col("data2"), ofString("12"), utf8Lcase),
      predicate(">=", col("data2"), ofString("sss"), unicode)) ->
      (
        "ALWAYS_TRUE()",
        """((column(`data2`) = 12 COLLATE SPARK.UTF8_LCASE) AND
          |(column(`data2`) >= sss COLLATE ICU.UNICODE))""".stripMargin.replaceAll("\n", " ")),
    // multiple predicates on data columns joined with OR
    predicate(
      "OR",
      predicate("<=", col("data2"), ofString("sss")),
      predicate("=", col("data3", "data31"), ofBoolean(true))) ->
      ("ALWAYS_TRUE()", "((column(`data2`) <= sss) OR (column(`data3`.`data31`) = true))"),
    predicate(
      "OR",
      predicate("<=", col("data2"), ofString("sss"), utf8Lcase),
      predicate("=", col("data3", "data31"), ofBoolean(true))) ->
      (
        "ALWAYS_TRUE()",
        "((column(`data2`) <= sss COLLATE SPARK.UTF8_LCASE) OR (column(`data3`.`data31`) = true))"),
    // single predicate on a partition column
    predicate("=", col("part1"), ofInt(12)) ->
      ("(column(`part1`) = 12)", "ALWAYS_TRUE()"),
    // single predicate with default collation on partition column
    predicate("=", col("part3"), ofString("12"), CollationIdentifier.SPARK_UTF8_BINARY) ->
      ("(column(`part3`) = 12 COLLATE SPARK.UTF8_BINARY)", "ALWAYS_TRUE()"),
    // single predicate with non-default collation on partition column
    predicate("=", col("part3"), ofString("12"), utf8Lcase) ->
      ("(column(`part3`) = 12 COLLATE SPARK.UTF8_LCASE)", "ALWAYS_TRUE()"),
    predicate("=", col("part3"), ofString("12"), unicode) ->
      ("(column(`part3`) = 12 COLLATE ICU.UNICODE)", "ALWAYS_TRUE()"),
    // multiple predicates on partition columns joined with AND
    predicate(
      "AND",
      predicate("=", col("part1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("((column(`part1`) = 12) AND (column(`part3`) >= sss))", "ALWAYS_TRUE()"),
    // multiple predicates with collation on partition columns joined with AND
    predicate(
      "AND",
      predicate("=", col("part3"), ofString("sss"), utf8Lcase),
      predicate(">=", col("part3"), ofString("sss"), CollationIdentifier.SPARK_UTF8_BINARY)) ->
      (
        """((column(`part3`) = sss COLLATE SPARK.UTF8_LCASE) AND (column(`part3`)
          |>= sss COLLATE SPARK.UTF8_BINARY))""".stripMargin.replaceAll("\n", " "),
        "ALWAYS_TRUE()"),
    // multiple predicates on partition columns joined with OR
    predicate(
      "OR",
      predicate("<=", col("part3"), ofString("sss")),
      predicate("=", col("part1"), ofInt(2781))) ->
      ("((column(`part3`) <= sss) OR (column(`part1`) = 2781))", "ALWAYS_TRUE()"),

    // predicates (each on data and partition column) joined with AND
    predicate(
      "AND",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("(column(`part3`) >= sss)", "(column(`data1`) = 12)"),

    // predicates with collation (each on data and partition column) joined with AND
    predicate(
      "AND",
      predicate("=", col("data2"), ofString("12"), utf8Lcase),
      predicate(">=", col("part3"), ofString("sss"), unicode)) ->
      (
        "(column(`part3`) >= sss COLLATE ICU.UNICODE)",
        "(column(`data2`) = 12 COLLATE SPARK.UTF8_LCASE)"),

    // predicates (each on data and partition column) joined with OR
    predicate(
      "OR",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`data1`) = 12) OR (column(`part3`) >= sss))"),

    // predicates with collation (each on data and partition column) joined with OR
    predicate(
      "OR",
      predicate("=", col("data2"), ofString("12"), unicode),
      predicate(">=", col("part3"), ofString("sss"), unicode)) ->
      (
        "ALWAYS_TRUE()",
        """((column(`data2`) = 12 COLLATE ICU.UNICODE) OR (column(`part3`)
          |>= sss COLLATE ICU.UNICODE))""".stripMargin.replaceAll("\n", " ")),

    // predicates (multiple on data and partition columns) joined with AND
    predicate(
      "AND",
      predicate(
        "AND",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate(
        "AND",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "((column(`part1`) = 12) AND (column(`part3`) >= sss))",
        "((column(`data1`) = 12) AND (column(`data2`) >= sss))"),

    // predicates (multiple on data and partition columns joined with OR) joined with AND
    predicate(
      "AND",
      predicate(
        "OR",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate(
        "OR",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "((column(`part1`) = 12) OR (column(`part3`) >= sss))",
        "((column(`data1`) = 12) OR (column(`data2`) >= sss))"),

    // predicates (multiple on data and partition columns joined with OR) joined with OR
    predicate(
      "OR",
      predicate(
        "OR",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate(
        "OR",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "ALWAYS_TRUE()",
        "(((column(`data1`) = 12) OR (column(`data2`) >= sss)) OR " +
          "((column(`part1`) = 12) OR (column(`part3`) >= sss)))"),

    // predicates (data and partitions compared in the same expression)
    predicate(
      "AND",
      predicate("=", col("data1"), col("part1")),
      predicate(">=", col("part3"), ofString("sss"))) ->
      (
        "(column(`part3`) >= sss)",
        "(column(`data1`) = column(`part1`))"),

    // predicates with collation (data and partitions compared in the same expression)
    predicate(
      "AND",
      predicate("=", col("data2"), col("part3"), utf8Lcase),
      predicate(">=", col("part3"), ofString("sss"), unicode)) ->
      (
        "(column(`part3`) >= sss COLLATE ICU.UNICODE)",
        "(column(`data2`) = column(`part3`) COLLATE SPARK.UTF8_LCASE)"),

    // predicate only on data column but reverse order of literal and column
    predicate("=", ofInt(12), col("data1")) ->
      ("ALWAYS_TRUE()", "(12 = column(`data1`))"),

    // predicate with collation only on data column but reverse order of literal and column
    predicate("=", ofString("12"), col("data2"), utf8Lcase) ->
      ("ALWAYS_TRUE()", "(12 = column(`data2`) COLLATE SPARK.UTF8_LCASE)"))

  partitionTestCases.foreach {
    case (predicate, (partitionPredicate, dataPredicate)) =>
      test(s"split predicate into data and partition predicates: $predicate") {
        val metadataAndDataPredicates = splitMetadataAndDataPredicates(predicate, partitionCols)
        assert(metadataAndDataPredicates._1.toString === partitionPredicate)
        assert(metadataAndDataPredicates._2.toString === dataPredicate)
      }
  }

  // Map entry format: (given predicate -> \
  // (exp predicate for partition pruning, exp predicate for checkpoint reader pushdown))
  val rewriteTestCases = Map(
    // single predicate on a partition column
    predicate("=", col("part2"), ofTimestamp(12)) ->
      (
        // exp predicate for partition pruning
        "(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part2), date) = 12)",

        // exp predicate for checkpoint reader pushdown
        "(column(`add`.`partitionValues_parsed`.`part2`) = 12)"),
    // single predicate with collation on a partition column
    predicate("=", col("part3"), ofString("sss"), utf8Lcase) ->
      (
        // exp predicate for partition pruning
        "(ELEMENT_AT(column(`add`.`partitionValues`), part3) = sss COLLATE SPARK.UTF8_LCASE)",

        // exp predicate for checkpoint reader pushdown
        "(column(`add`.`partitionValues_parsed`.`part3`) = sss COLLATE SPARK.UTF8_LCASE)"),
    // multiple predicates on partition columns joined with AND
    predicate(
      "AND",
      predicate("=", col("part1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      (
        // exp predicate for partition pruning
        """((partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part1), integer) = 12) AND
          |(ELEMENT_AT(column(`add`.`partitionValues`), part3) >= sss))"""
          .stripMargin.replaceAll("\n", " "),

        // exp predicate for checkpoint reader pushdown
        """((column(`add`.`partitionValues_parsed`.`part1`) = 12) AND
          |(column(`add`.`partitionValues_parsed`.`part3`) >= sss))"""
          .stripMargin.replaceAll("\n", " ")),
    // multiple predicates with collation on partition columns joined with AND
    predicate(
      "AND",
      predicate("=", col("part3"), ofString("sss"), utf8Lcase),
      predicate(">=", col("part3"), ofString("sss"), CollationIdentifier.SPARK_UTF8_BINARY)) ->
      (
        // exp predicate for partition pruning
        """((ELEMENT_AT(column(`add`.`partitionValues`), part3) = sss COLLATE SPARK.UTF8_LCASE) AND
          |(ELEMENT_AT(column(`add`.`partitionValues`), part3) >= sss COLLATE SPARK.UTF8_BINARY))"""
          .stripMargin.replaceAll("\n", " "),

        // exp predicate for checkpoint reader pushdown
        """((column(`add`.`partitionValues_parsed`.`part3`) = sss COLLATE SPARK.UTF8_LCASE) AND
          |(column(`add`.`partitionValues_parsed`.`part3`) >= sss COLLATE SPARK.UTF8_BINARY))"""
          .stripMargin.replaceAll("\n", " ")),
    // multiple predicates on partition columns joined with OR
    predicate(
      "OR",
      predicate("<=", col("part3"), ofString("sss")),
      predicate("=", col("part1"), ofInt(2781))) ->
      (
        // exp predicate for partition pruning
        """((ELEMENT_AT(column(`add`.`partitionValues`), part3) <= sss) OR
          |(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part1), integer) = 2781))"""
          .stripMargin.replaceAll("\n", " "),

        // exp predicate for checkpoint reader pushdown
        """((column(`add`.`partitionValues_parsed`.`part3`) <= sss) OR
          |(column(`add`.`partitionValues_parsed`.`part1`) = 2781))"""
          .stripMargin.replaceAll("\n", " ")))
  rewriteTestCases.foreach {
    case (predicate, (expPartitionPruningPredicate, expCheckpointReaderPushdownPredicate)) =>
      test(s"rewrite partition predicate on scan file schema: $predicate") {
        val actPartitionPruningPredicate =
          rewritePartitionPredicateOnScanFileSchema(predicate, partitionColsMetadata)
        assert(actPartitionPruningPredicate.toString === expPartitionPruningPredicate)

        val actCheckpointReaderPushdownPredicate =
          rewritePartitionPredicateOnCheckpointFileSchema(predicate, partitionColsMetadata)
        assert(actCheckpointReaderPushdownPredicate.toString ===
          expCheckpointReaderPushdownPredicate)
      }
  }

  private val nullFileName = "__HIVE_DEFAULT_PARTITION__"
  Seq(
    ofBoolean(true) -> ("true", "true"),
    ofBoolean(false) -> ("false", "false"),
    ofNull(BooleanType.BOOLEAN) -> (null, nullFileName),
    ofByte(24.toByte) -> ("24", "24"),
    ofNull(ByteType.BYTE) -> (null, nullFileName),
    ofShort(876.toShort) -> ("876", "876"),
    ofNull(ShortType.SHORT) -> (null, nullFileName),
    ofInt(2342342) -> ("2342342", "2342342"),
    ofNull(IntegerType.INTEGER) -> (null, nullFileName),
    ofLong(234234223L) -> ("234234223", "234234223"),
    ofNull(LongType.LONG) -> (null, nullFileName),
    ofFloat(23423.4223f) -> ("23423.422", "23423.422"),
    ofNull(FloatType.FLOAT) -> (null, nullFileName),
    ofDouble(23423.422233d) -> ("23423.422233", "23423.422233"),
    ofNull(DoubleType.DOUBLE) -> (null, nullFileName),
    ofString("string_val") -> ("string_val", "string_val"),
    ofString("string_\nval") -> ("string_\nval", "string_%0Aval"),
    ofString("str=ing_\u0001val") -> ("str=ing_\u0001val", "str%3Ding_%01val"),
    ofNull(StringType.STRING) -> (null, nullFileName),
    ofDecimal(new java.math.BigDecimal("23423.234234"), 15, 7) ->
      ("23423.2342340", "23423.2342340"),
    ofNull(new DecimalType(15, 7)) -> (null, nullFileName),
    ofBinary("binary_val".getBytes) -> ("binary_val", "binary_val"),
    ofNull(BinaryType.BINARY) -> (null, nullFileName),
    ofDate(4234) -> ("1981-08-05", "1981-08-05"),
    ofNull(DateType.DATE) -> (null, nullFileName),
    ofTimestamp(2342342342232L) ->
      ("1970-01-28 02:39:02.342232", "1970-01-28 02%3A39%3A02.342232"),
    ofNull(TimestampType.TIMESTAMP) -> (null, nullFileName),
    ofTimestampNtz(-2342342342L) ->
      ("1969-12-31 23:20:58.657658", "1969-12-31 23%3A20%3A58.657658"),
    ofNull(TimestampNTZType.TIMESTAMP_NTZ) -> (null, nullFileName)).foreach {
    case (literal, (expSerializedValue, expFileName)) =>
      test(s"serialize partition value literal as string: ${literal.getDataType}($literal)") {
        val result = serializePartitionValue(literal)
        assert(result === expSerializedValue)
      }

      test(s"construct partition data output directory: ${literal.getDataType}($literal)") {
        val result = getTargetDirectory(
          "/tmp/root",
          Seq("part1").asJava,
          Map("part1" -> literal).asJava)
        assert(result === s"/tmp/root/part1=$expFileName")
      }
  }

  test("construct partition data output directory with multiple partition columns") {
    val result = getTargetDirectory(
      "/tmp/root",
      Seq("part1", "part2", "part3").asJava,
      Map(
        "part1" -> ofInt(12),
        "part3" -> ofTimestamp(234234234L),
        "part2" -> ofString("sss")).asJava)
    assert(result === "/tmp/root/part1=12/part2=sss/part3=1970-01-01 00%3A03%3A54.234234")
  }

  // Test cases for verifying if timestamp can be parsed correctly.
  test("parse valid standard timestamp") {
    val result1 = PartitionUtils.tryParseTimestamp("2024-01-01 10:00:00")
    assert(result1 == 1704103200000000L)
    val result2 = PartitionUtils.tryParseTimestamp("2024-01-01 10:00:00.123456")
    assert(result2 == 1704103200123456L)
  }

  test("parse valid ISO8601 timestamp") {
    val result = PartitionUtils.tryParseTimestamp("2024-01-01T10:00:00Z")
    assert(result == 1704103200000000L)
  }

  test("parse valid ISO8601 timestamp with microsecond precision") {
    val result = PartitionUtils.tryParseTimestamp("1970-01-01T00:00:00.123456Z")
    assert(result == 123456L)
  }

  test("throw on invalid timestamp") {
    val thrown = intercept[IllegalStateException] {
      PartitionUtils.tryParseTimestamp("not-a-timestamp")
    }
    assert(thrown.getMessage.contains("Invalid timestamp format for value"))
  }

  private def col(names: String*): Column = {
    new Column(names.toArray)
  }

  private def predicate(name: String, children: Expression*): Predicate = {
    new Predicate(name, children.asJava)
  }

  private def predicate(
      name: String,
      left: Expression,
      right: Expression,
      collationIdentifier: CollationIdentifier) = {
    new Predicate(name, left, right, collationIdentifier)
  }
}
