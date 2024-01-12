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

import io.delta.kernel.client.ExpressionHandler
import io.delta.kernel.data.ColumnVector

import java.util
import scala.collection.JavaConverters._
import io.delta.kernel.expressions._
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.util.PartitionUtils.{rewritePartitionPredicateOnScanFileSchema, splitPredicates}
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

class PartitionUtilsSuite extends AnyFunSuite {
  // Table schema
  // Data columns: d1: int, d2: string, date3: struct(d31: boolean, d32: long)
  // Partition columns: p1: int, p2: date, p3: string
  val tableSchema = new StructType()
    .add("d1", IntegerType.INTEGER)
    .add("d2", StringType.STRING)
    .add("d3", new StructType()
      .add("d31", BooleanType.BOOLEAN)
      .add("d32", LongType.LONG))
    .add("p1", IntegerType.INTEGER)
    .add("p2", DateType.DATE)
    .add("p3", StringType.STRING)

  private val partitionColsMetadata = new util.HashMap[String, StructField]() {
    {
      put("p1", tableSchema.get("p1"))
      put("p2", tableSchema.get("p2"))
      put("p3", tableSchema.get("p3"))
    }
  }

  private val partitionCols: java.util.Set[String] = partitionColsMetadata.keySet()

  // Test cases for verifying query predicate is split into guaranteed and best effort predicates
  // Map entry format (predicate -> (guaranteed predicate, best effort predicate)
  val partitionTestCases = Map[Predicate, (String, String)](
    // single predicate on a data column
    eq(col("d1"), int(12)) -> ("ALWAYS_TRUE()", "(column(`d1`) = 12)"),

    // multiple predicates on data columns joined with AND
    and(eq(col("d1"), int(12)), gte(col("d2"), str("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`d1`) = 12) AND (column(`d2`) >= sss))"),

    // multiple predicates on data columns joined with OR
    or(lte(col("d2"), str("sss")), eq(col("d3", "d31"), ofBoolean(true))) ->
      ("ALWAYS_TRUE()", "((column(`d2`) <= sss) OR (column(`d3`.`d31`) = true))"),

    // single predicate on a partition column
    eq(col("p1"), int(12)) -> ("(column(`p1`) = 12)", "ALWAYS_TRUE()"),

    // multiple predicates on partition columns joined with AND
    and(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))) ->
      ("((column(`p1`) = 12) AND (column(`p3`) >= sss))", "ALWAYS_TRUE()"),

    // multiple predicates on partition columns joined with OR
    or(lte(col("p3"), str("sss")), eq(col("p1"), int(2781))) ->
      ("((column(`p3`) <= sss) OR (column(`p1`) = 2781))", "ALWAYS_TRUE()"),

    // predicates (each on data and partition column) joined with AND
    and(eq(col("d1"), int(12)), gte(col("p3"), str("sss"))) ->
      ("(column(`p3`) >= sss)", "(column(`d1`) = 12)"),

    // predicates (each on data and partition column) joined with OR
    or(eq(col("d1"), int(12)), gte(col("p3"), str("sss"))) ->
      ("ALWAYS_TRUE()", "((column(`d1`) = 12) OR (column(`p3`) >= sss))"),

    // predicates (multiple on data and partition columns) joined with AND
    and(
      and(eq(col("d1"), int(12)), gte(col("d2"), str("sss"))),
      and(eq(col("p1"), int(12)), gte(col("p3"), str("sss")))) ->
      (
        "((column(`p1`) = 12) AND (column(`p3`) >= sss))",
        "((column(`d1`) = 12) AND (column(`d2`) >= sss))"
      ),

    // predicates (multiple on data and partition columns joined with OR) joined with AND
    and(
      or(eq(col("d1"), int(12)), gte(col("d2"), str("sss"))),
      or(eq(col("p1"), int(12)), gte(col("p3"), str("sss")))) ->
      (
        "((column(`p1`) = 12) OR (column(`p3`) >= sss))",
        "((column(`d1`) = 12) OR (column(`d2`) >= sss))"
      ),

    // predicates (multiple on data and partition columns joined with OR) joined with OR
    or(
      or(eq(col("d1"), int(12)), gte(col("d2"), str("sss"))),
      or(eq(col("p1"), int(12)), gte(col("p3"), str("sss")))) ->
      (
        "ALWAYS_TRUE()",
        "(((column(`d1`) = 12) OR (column(`d2`) >= sss)) OR " +
          "((column(`p1`) = 12) OR (column(`p3`) >= sss)))"
      ),

    // predicates (data and partitions compared in the same expression)
    and(eq(col("d1"), col("p1")), gte(col("p3"), str("sss"))) ->
      (
        "(column(`p3`) >= sss)",
        "(column(`d1`) = column(`p1`))"
      ),

    // predicate only on data column but reverse order of literal and column
    eq(int(12), col("d1")) -> ("ALWAYS_TRUE()", "(12 = column(`d1`))"),

    // just an unsupported predicate
    unsupported("p1") -> ("ALWAYS_TRUE()", "UNSUPPORTED(column(`p1`))"),

    // two unsupported predicates combined with AND and OR
    and(unsupported("p1"), unsupported("d1")) ->
      ("ALWAYS_TRUE()", "(UNSUPPORTED(column(`p1`)) AND UNSUPPORTED(column(`d1`)))"),
    or(unsupported("p1"), unsupported("d1")) ->
      ("ALWAYS_TRUE()", "(UNSUPPORTED(column(`p1`)) OR UNSUPPORTED(column(`d1`)))"),

    // supported and unsupported predicates combined with a AND
    and(unsupported("p1"), gte(col("p3"), str("sss"))) ->
      ("(column(`p3`) >= sss)", "UNSUPPORTED(column(`p1`))"),
    and(unsupported("p1"), gte(col("d3"), str("sss"))) ->
      ("ALWAYS_TRUE()", "(UNSUPPORTED(column(`p1`)) AND (column(`d3`) >= sss))"),

    // predicates (multiple supported and unsupported joined with AND) joined with AND
    and(
      and(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))),
      and(unsupported("p1"), unsupported("p3"))) ->
      (
        "((column(`p1`) = 12) AND (column(`p3`) >= sss))",
        "(UNSUPPORTED(column(`p1`)) AND UNSUPPORTED(column(`p3`)))"
      ),

    // predicates (multiple supported and unsupported joined with AND) joined with AND
    and(
      and(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))),
      and(unsupported("d1"), unsupported("p3"))) ->
      (
        "((column(`p1`) = 12) AND (column(`p3`) >= sss))",
        "(UNSUPPORTED(column(`d1`)) AND UNSUPPORTED(column(`p3`)))"
      ),

    // predicates (multiple supported and unsupported joined with AND) joined with AND
    and(
      and(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))),
      and(eq(col("p1"), int(12)), unsupported("p3"))) ->
      (
        "(((column(`p1`) = 12) AND (column(`p3`) >= sss)) AND (column(`p1`) = 12))",
        "UNSUPPORTED(column(`p3`))"
      ),

    // predicates (multiple supported and unsupported joined with AND) joined with AND
    and(
      and(eq(col("p1"), int(14)), gte(col("p3"), str("sss"))),
      and(eq(col("d1"), int(12)), unsupported("p3"))) ->
      (
        "((column(`p1`) = 14) AND (column(`p3`) >= sss))",
        "((column(`d1`) = 12) AND UNSUPPORTED(column(`p3`)))"
      ),

    // predicates (multiple supported and unsupported joined with OR) joined with AND
    and(
      or(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))),
      or(unsupported("p1"), unsupported("p3"))) ->
      (
        "((column(`p1`) = 12) OR (column(`p3`) >= sss))",
        "(UNSUPPORTED(column(`p1`)) OR UNSUPPORTED(column(`p3`)))"
      ),

    // predicates (multiple supported and unsupported joined with OR) joined with OR
    or(
      or(eq(col("p1"), int(12)), gte(col("p3"), str("sss"))),
      or(unsupported("p1"), unsupported("p3"))) ->
      (
        "ALWAYS_TRUE()",
        "(((column(`p1`) = 12) OR (column(`p3`) >= sss)) OR " +
          "(UNSUPPORTED(column(`p1`)) OR UNSUPPORTED(column(`p3`))))"
      )
  )

  partitionTestCases.foreach {
    case (predicate, (partitionPredicate, dataPredicate)) =>
      test(s"split predicate into guaranteed and best-effort predicates: $predicate") {
        val metadataAndDataPredicates =
          splitPredicates(defaultExprHandler, tableSchema, predicate, partitionCols)
        assert(metadataAndDataPredicates._1.toString === partitionPredicate)
        assert(metadataAndDataPredicates._2.toString === dataPredicate)
      }
  }

  // Map entry format: (given predicate -> expected rewritten predicate)
  val rewriteTestCases = Map(
    // single predicate on a partition column
    eq(col("p2"), ofTimestamp(12)) ->
      "(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), p2), date) = 12)",
    // multiple predicates on partition columns joined with AND
    and(
      eq(col("p1"), int(12)),
      gte(col("p3"), str("sss"))) ->
      """((partition_value(ELEMENT_AT(column(`add`.`partitionValues`), p1), integer) = 12) AND
        |(ELEMENT_AT(column(`add`.`partitionValues`), p3) >= sss))"""
        .stripMargin.replaceAll("\n", " "),
    // multiple predicates on partition columns joined with OR
    or(
      lte(col("p3"), str("sss")),
      eq(col("p1"), int(2781))) ->
      """((ELEMENT_AT(column(`add`.`partitionValues`), p3) <= sss) OR
        |(partition_value(ELEMENT_AT(column(`add`.`partitionValues`), p1), integer) = 2781))"""
        .stripMargin.replaceAll("\n", " ")
  )

  rewriteTestCases.foreach {
    case (predicate, expRewrittenPredicate) =>
      test(s"rewrite partition predicate on scan file schema: $predicate") {
        val actRewrittenPredicate =
          rewritePartitionPredicateOnScanFileSchema(predicate, partitionColsMetadata)
        assert(actRewrittenPredicate.toString === expRewrittenPredicate)
      }
  }

  val defaultExprHandler = new ExpressionHandler {
    override def getEvaluator(inputSchema: StructType, expression: Expression, outputType: DataType)
    : ExpressionEvaluator =
      throw new UnsupportedOperationException("Not implemented")

    override def isSupported(
        inputSchema: StructType, expression: Expression, outputType: DataType): Boolean = {
      hasSupportedExpr(expression)
    }

    override def getPredicateEvaluator(inputSchema: StructType, predicate: Predicate)
    : PredicateEvaluator =
      throw new UnsupportedOperationException("Not implemented")

    override def createSelectionVector(values: Array[Boolean], from: Int, to: Int): ColumnVector =
      throw new UnsupportedOperationException("Not implemented")

    def hasSupportedExpr(expr: Expression): Boolean = {
      expr match {
        case _: Column | _: Literal => true
        case pred: Predicate =>
          pred.getName.toUpperCase() match {
            case "AND" | "OR" | "=" | "!=" | ">" | ">=" | "<" | "<=" =>
              !pred.getChildren.asScala.exists(!hasSupportedExpr(_))
            case _ => false
          }
        case _ => false
      }
    }
  }

  private def col(names: String*): Column = new Column(names.toArray)
  private def predicate(name: String, children: Expression*): Predicate =
    new Predicate(name, children.asJava)
  private def and(left: Predicate, right: Predicate): Predicate = predicate("AND", left, right)
  private def or(left: Predicate, right: Predicate): Predicate = predicate("OR", left, right)
  private def eq(left: Expression, right: Expression): Predicate = predicate("=", left, right)
  private def gte(column: Column, literal: Literal): Predicate = predicate(">=", column, literal)
  private def lte(column: Column, literal: Literal): Predicate = predicate("<=", column, literal)
  private def int(value: Int): Literal = Literal.ofInt(value)
  private def str(value: String): Literal = Literal.ofString(value)
  private def unsupported(colName: String): Predicate = predicate("UNSUPPORTED", col(colName));
}

