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
import io.delta.kernel.internal.util.PartitionUtils.{rewritePartitionPredicateOnScanFileSchema, splitMetadataAndDataPredicates}
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

class PartitionUtilsSuite extends AnyFunSuite {
  // Table schema
  // Data columns: data1: int, data2: string, date3: struct(data31: boolean, data32: long)
  // Partition columns: part1: int, part2: date, part3: string
  private val partitionColsToType = new util.HashMap[String, DataType]() {
    {
      put("part1", IntegerType.INSTANCE)
      put("part2", DateType.INSTANCE)
      put("part3", StringType.INSTANCE)
    }
  }

  private val partitionCols: java.util.Set[String] = partitionColsToType.keySet()

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
      )
  )

  partitionTestCases.foreach {
    case (predicate, (partitionPredicate, dataPredicate)) =>
      test(s"split predicate into data and partition predicates: $predicate") {
        val metadataAndDataPredicates = splitMetadataAndDataPredicates(predicate, partitionCols)
        assert(metadataAndDataPredicates._1.toString === partitionPredicate)
        assert(metadataAndDataPredicates._2.toString === dataPredicate)
      }
  }

  // Map entry format: (given predicate -> expected rewritten predicate)
  val rewriteTestCases = Map(
    // single predicate on a partition column
    predicate("=", col("part2"), ofTimestamp(12)) ->
      "(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part2), date) = 12)",
    // multiple predicates on partition columns joined with AND
    predicate("AND",
      predicate("=", col("part1"), ofInt(12)),
      predicate(">=", col("part3"), ofString("sss"))) ->
      """((partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part1), integer) = 12) AND
        |(ELEMENT_AT(column(`add`.`partitionValues`), part3) >= sss))"""
        .stripMargin.replaceAll("\n", " "),
    // multiple predicates on partition columns joined with OR
    predicate("OR",
      predicate("<=", col("part3"), ofString("sss")),
      predicate("=", col("part1"), ofInt(2781))) ->
      """((ELEMENT_AT(column(`add`.`partitionValues`), part3) <= sss) OR
        |(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), part1), integer) = 2781))"""
        .stripMargin.replaceAll("\n", " "),
  )

  rewriteTestCases.foreach {
    case (predicate, expRewrittenPredicate) =>
      test(s"rewrite partition predicate on scan file schema: $predicate") {
        val actRewrittenPredicate =
          rewritePartitionPredicateOnScanFileSchema(predicate, partitionColsToType)
        assert(actRewrittenPredicate.toString === expRewrittenPredicate)
      }
  }

  private def col(names: String*): Column = {
    new Column(names.toArray)
  }

  private def predicate(name: String, children: Expression*): Predicate = {
    new Predicate(name, children.asJava)
  }
}

