/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import scala.collection.JavaConverters._

import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions._
import io.delta.standalone.types.{IntegerType, StructField, StructType}
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.util.PartitionUtils

// scalastyle:off funsuite
import org.scalatest.FunSuite

// scalastyle:off println
class ExpressionSuite extends FunSuite {
  // scalastyle:on funsuite

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true)))

  private val dataSchema = new StructType(Array(
    new StructField("col3", new IntegerType(), true),
    new StructField("col4", new IntegerType(), true),
    new StructField("col5", new IntegerType(), true)))

  private def testPredicate(
      predicate: Expression,
      expectedResult: Boolean,
      record: RowRecord = null): Unit = {
    println(predicate.toString())
    println(predicate.eval(record))
    assert(predicate.eval(record) == expectedResult)
  }

  private def testPartitionFilter(
      partitionSchema: StructType,
      inputFiles: Seq[AddFile],
      filter: Expression,
      expectedMatchedFiles: Seq[AddFile]): Unit = {
    println("filter: " + filter.toString)
    val matchedFiles = PartitionUtils.filterFileList(partitionSchema, inputFiles, filter)
    assert(matchedFiles.length == expectedMatchedFiles.length)
    assert(matchedFiles.forall(expectedMatchedFiles.contains(_)))
  }

  test("basic predicate") {
    testPredicate(new And(Literal.False, Literal.False), expectedResult = false)
    testPredicate(new And(Literal.True, Literal.False), expectedResult = false)
    testPredicate(new And(Literal.False, Literal.True), expectedResult = false)
    testPredicate(new And(Literal.True, Literal.True), expectedResult = true)

    testPredicate(new Or(Literal.False, Literal.False), expectedResult = false)
    testPredicate(new Or(Literal.True, Literal.False), expectedResult = true)
    testPredicate(new Or(Literal.False, Literal.True), expectedResult = true)
    testPredicate(new Or(Literal.True, Literal.True), expectedResult = true)

    testPredicate(new Not(Literal.False), expectedResult = true)
    testPredicate(new Not(Literal.True), expectedResult = false)

    testPredicate(new EqualTo(Literal.of(1), Literal.of(1)), expectedResult = true)
    testPredicate(new EqualTo(Literal.of(1), Literal.of(2)), expectedResult = false)

    testPredicate(new LessThan(Literal.of(1), Literal.of(1)), expectedResult = false)
    testPredicate(new LessThan(Literal.of(1), Literal.of(2)), expectedResult = true)

    val inSet = (0 to 10).map(Literal.of).asJava
    testPredicate(new In(Literal.of(1), inSet), expectedResult = true)
    testPredicate(new In(Literal.of(100), inSet), expectedResult = false)
  }

  test("basic partition filter") {
    val add00 = AddFile("1", Map("col1" -> "0", "col2" -> "0"), 0, 0, dataChange = true)
    val add01 = AddFile("2", Map("col1" -> "0", "col2" -> "1"), 0, 0, dataChange = true)
    val add02 = AddFile("2", Map("col1" -> "0", "col2" -> "2"), 0, 0, dataChange = true)
    val add10 = AddFile("3", Map("col1" -> "1", "col2" -> "0"), 0, 0, dataChange = true)
    val add11 = AddFile("4", Map("col1" -> "1", "col2" -> "1"), 0, 0, dataChange = true)
    val add12 = AddFile("4", Map("col1" -> "1", "col2" -> "2"), 0, 0, dataChange = true)
    val add20 = AddFile("4", Map("col1" -> "2", "col2" -> "0"), 0, 0, dataChange = true)
    val add21 = AddFile("4", Map("col1" -> "2", "col2" -> "1"), 0, 0, dataChange = true)
    val add22 = AddFile("4", Map("col1" -> "2", "col2" -> "2"), 0, 0, dataChange = true)
    val inputFiles = Seq(add00, add01, add02, add10, add11, add12, add20, add21, add22)

    val f1Expr1 = new EqualTo(partitionSchema.column("col1"), Literal.of(0))
    val f1Expr2 = new EqualTo(partitionSchema.column("col2"), Literal.of(1))
    val f1 = new And(f1Expr1, f1Expr2)

    testPartitionFilter(partitionSchema, inputFiles, f1, add01 :: Nil)

    val f2Expr1 = new LessThan(partitionSchema.column("col1"), Literal.of(1))
    val f2Expr2 = new LessThan(partitionSchema.column("col2"), Literal.of(1))
    val f2 = new And(f2Expr1, f2Expr2)
    testPartitionFilter(partitionSchema, inputFiles, f2, add00 :: Nil)

    val f3Expr1 = new EqualTo(partitionSchema.column("col1"), Literal.of(2))
    val f3Expr2 = new LessThan(partitionSchema.column("col2"), Literal.of(1))
    val f3 = new Or(f3Expr1, f3Expr2)
    testPartitionFilter(
      partitionSchema, inputFiles, f3, Seq(add20, add21, add22, add00, add10))

    val inSet4 = (2 to 10).map(Literal.of).asJava
    val f4 = new In(partitionSchema.column("col1"), inSet4)
    testPartitionFilter(partitionSchema, inputFiles, f4, add20 :: add21 :: add22 :: Nil)

    val inSet5 = (100 to 110).map(Literal.of).asJava
    val f5 = new In(partitionSchema.column("col1"), inSet5)
    testPartitionFilter(partitionSchema, inputFiles, f5, Nil)
  }

  test("not null partition filter") {
    val add0Null = AddFile("1", Map("col1" -> "0", "col2" -> null), 0, 0, dataChange = true)
    val addNull1 = AddFile("1", Map("col1" -> null, "col2" -> "1"), 0, 0, dataChange = true)
    val inputFiles = Seq(add0Null, addNull1)

    val f1 = new IsNotNull(partitionSchema.column("col1"))
    testPartitionFilter(partitionSchema, inputFiles, f1, add0Null :: Nil)
  }

  test("Expr.references() and PredicateUtils.isPredicateMetadataOnly()") {
    val dataExpr = new And(
      new LessThan(dataSchema.column("col3"), Literal.of(5)),
      new Or(
        new EqualTo(dataSchema.column("col3"), dataSchema.column("col4")),
        new EqualTo(dataSchema.column("col3"), dataSchema.column("col5"))
      )
    )

    assert(dataExpr.references().size() == 3)

    val partitionExpr = new EqualTo(partitionSchema.column("col1"), partitionSchema.column("col2"))

    assert(
      !PartitionUtils.isPredicateMetadataOnly(dataExpr, partitionSchema.getFieldNames.toSeq))

    assert(
      PartitionUtils.isPredicateMetadataOnly(partitionExpr, partitionSchema.getFieldNames.toSeq))
  }

  test("expression equality") {
    // BinaryExpression
    val and = new And(partitionSchema.column("col1"), partitionSchema.column("col2"))
    val andCopy = new And(partitionSchema.column("col1"), partitionSchema.column("col2"))
    val and2 = new And(dataSchema.column("col3"), Literal.of(44))
    assert(and == andCopy)
    assert(and != and2)

    // UnaryExpression
    val not = new Not(new EqualTo(Literal.of(1), Literal.of(1)))
    val notCopy = new Not(new EqualTo(Literal.of(1), Literal.of(1)))
    val not2 = new Not(new EqualTo(Literal.of(45), dataSchema.column("col4")))
    assert(not == notCopy)
    assert(not != not2)

    // LeafExpression
    val col1 = partitionSchema.column("col1")
    val col1Copy = partitionSchema.column("col1")
    val col2 = partitionSchema.column("col2")
    assert(col1 == col1Copy)
    assert(col1 != col2)
  }
}
