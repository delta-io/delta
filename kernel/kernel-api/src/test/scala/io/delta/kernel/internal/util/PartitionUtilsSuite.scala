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

import io.delta.kernel.expressions.Literal._
import io.delta.kernel.expressions._
import io.delta.kernel.internal.util.PartitionUtils._
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.JavaConverters._

class PartitionUtilsSuite extends AnyFunSuite {
  // Table schema
  // Data columns: data1: int, data2: string, date3: struct(data31: boolean, data32: long)
  // Partition columns: part1: int, part2: date, part3: string
  val tableSchema = new StructType()
    .add("data1", IntegerType.INTEGER)
    .add("data2", StringType.STRING)
    .add("data3", new StructType()
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
    // multiple predicates on data columns joined with AND
    predicate("AND",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("data2"), ofString("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`data1`) = 12) AND (column(`data2`) >= sss))"),
    // multiple predicates on data columns joined with OR
    predicate("OR",
      predicate("<=", col("data2"), ofString("sss")),
      predicate("=", col("data3", "data31"), ofBoolean(true))) ->
      ("ALWAYS_TRUE()", "((column(`data2`) <= sss) OR (column(`data3`.`data31`) = true))"),
    // single predicate on a partition column
    predicate("=", col("part1"), ofInt(12)) ->
      ("(column(`part1`) = 12)", "ALWAYS_TRUE()"),
    // multiple predicates on partition columns joined with AND
    predicate("AND",
      predicate("=", col("part1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("((column(`part1`) = 12) AND (column(`part3`) >= sss))", "ALWAYS_TRUE()"),
    // multiple predicates on partition columns joined with OR
    predicate("OR",
      predicate("<=", col("part3"), ofString("sss")),
      predicate("=", col("part1"), ofInt(2781))) ->
      ("((column(`part3`) <= sss) OR (column(`part1`) = 2781))", "ALWAYS_TRUE()"),

    // predicates (each on data and partition column) joined with AND
    predicate("AND",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("(column(`part3`) >= sss)", "(column(`data1`) = 12)"),

    // predicates (each on data and partition column) joined with OR
    predicate("OR",
      predicate("=", col("data1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`data1`) = 12) OR (column(`part3`) >= sss))"),

    // predicates (multiple on data and partition columns) joined with AND
    predicate("AND",
      predicate("AND",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate("AND",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "((column(`part1`) = 12) AND (column(`part3`) >= sss))",
        "((column(`data1`) = 12) AND (column(`data2`) >= sss))"
      ),

    // predicates (multiple on data and partition columns joined with OR) joined with AND
    predicate("AND",
      predicate("OR",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate("OR",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "((column(`part1`) = 12) OR (column(`part3`) >= sss))",
        "((column(`data1`) = 12) OR (column(`data2`) >= sss))"
      ),

    // predicates (multiple on data and partition columns joined with OR) joined with OR
    predicate("OR",
      predicate("OR",
        predicate("=", col("data1"), ofInt(12)),
        predicate(">=", col("data2"), ofString("sss"))),
      predicate("OR",
        predicate("=", col("part1"), ofInt(12)),
        predicate(">=", col("part3"), ofString("sss")))) ->
      (
        "ALWAYS_TRUE()",
        "(((column(`data1`) = 12) OR (column(`data2`) >= sss)) OR " +
          "((column(`part1`) = 12) OR (column(`part3`) >= sss)))"
      ),

    // predicates (data and partitions compared in the same expression)
    predicate("AND",
      predicate("=", col("data1"), col("part1")),
      predicate(">=", col("part3"), ofString("sss"))) ->
      (
        "(column(`part3`) >= sss)",
        "(column(`data1`) = column(`part1`))"
      ),

    // predicate only on data column but reverse order of literal and column
    predicate("=", ofInt(12), col("data1")) ->
      ("ALWAYS_TRUE()", "(12 = column(`data1`))")
  )

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
        "(column(`add`.`partitionValues_parsed`.`part2`) = 12)"
      ),
    // multiple predicates on partition columns joined with AND
    predicate("AND",
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
          .stripMargin.replaceAll("\n", " ")
      ),
    // multiple predicates on partition columns joined with OR
    predicate("OR",
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
          .stripMargin.replaceAll("\n", " ")
      )
  )
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

  Seq(
    ofBoolean(true) -> "true",
    ofBoolean(false) -> "false",
    ofNull(BooleanType.BOOLEAN) -> null,
    ofByte(24.toByte) -> "24",
    ofNull(ByteType.BYTE) -> null,
    ofShort(876.toShort) -> "876",
    ofNull(ShortType.SHORT) -> null,
    ofInt(2342342) -> "2342342",
    ofNull(IntegerType.INTEGER) -> null,
    ofLong(234234223L) -> "234234223",
    ofNull(LongType.LONG) -> null,
    ofFloat(23423.4223f) -> "23423.422",
    ofNull(FloatType.FLOAT) -> null,
    ofDouble(23423.422233d) -> "23423.422233",
    ofNull(DoubleType.DOUBLE) -> null,
    ofString("string_val") -> "string_val",
    ofDecimal(new java.math.BigDecimal("23423.234234"), 15, 7) -> "23423.2342340",
    ofNull(new DecimalType(15, 7)) -> null,
    ofNull(StringType.STRING) -> null,
    ofBinary("binary_val".getBytes) -> "binary_val",
    ofNull(BinaryType.BINARY) -> null,
    ofDate(4234)  -> "1981-08-05",
    ofNull(DateType.DATE) -> null,
    ofTimestamp(2342342342232L) -> "1970-01-28 02:39:02.342232",
    ofNull(TimestampType.TIMESTAMP) -> null,
    ofTimestampNtz(-2342342342L) -> "1969-12-31 23:20:58.657658",
    ofNull(TimestampNTZType.TIMESTAMP_NTZ) -> null
  ).foreach { case (literal, expStr) =>
    test(s"serialize partition value literal as string: ${literal.getDataType}($literal)") {
      val result = serializePartitionValue(literal)
      assert(result === expStr)
    }
  }

  private def col(names: String*): Column = {
    new Column(names.toArray)
  }

  private def predicate(name: String, children: Expression*): Predicate = {
    new Predicate(name, children.asJava)
  }
}

