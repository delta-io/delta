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

package org.apache.spark.sql.delta.expressions

import scala.reflect.ClassTag

import org.apache.spark.{Partitioner, RangePartitioner, SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSparkSession


class RangePartitionIdSuite
  extends SparkFunSuite with ExpressionEvalHelper with SharedSparkSession {

  def getPartitioner[T : Ordering : ClassTag](data: Seq[T], partitions: Int): Partitioner = {
    implicit val ordering = new Ordering[GenericInternalRow] {
      override def compare(x: GenericInternalRow, y: GenericInternalRow): Int = {
        def getValue0AsT(row: GenericInternalRow): T = row.values.head.asInstanceOf[T]
        val orderingT = implicitly[Ordering[T]]
        orderingT.compare(getValue0AsT(x), getValue0AsT(y))
      }
    }

    val rdd =
      spark.sparkContext.parallelize(data).filter(_ != null)
        .map(key => (new GenericInternalRow(Array[Any](key)), null))

    new RangePartitioner(partitions, rdd)
  }

  def testRangePartitionerExpr[T : Ordering : ClassTag](
    data: Seq[T], partitions: Int, childExpr: Expression, expected: Any): Unit = {
    val rangePartitioner = getPartitioner(data, partitions)
    checkEvaluation(PartitionerExpr(childExpr, rangePartitioner), expected)
  }

  test("RangePartitionerExpr: test basic") {
    val data = 0.until(12)
    for { numPartitions <- Seq(2, 3, 4, 6) } {
      val rangePartitioner = getPartitioner(data, numPartitions)
      data.foreach { i =>
        val expected = i / (data.size / numPartitions)
        checkEvaluation(PartitionerExpr(Literal(i), rangePartitioner), expected)
      }
    }
  }

  test("RangePartitionerExpr: null values") {
    testRangePartitionerExpr(
      data = 0.until(10),
      partitions = 2,
      childExpr = Literal(null),
      expected = 0)
  }

  test("RangePartitionerExpr: null data") {
    testRangePartitionerExpr(
      data = 0.until(10).map(_ => null),
      partitions = 2,
      childExpr = Literal("asd"),
      expected = 0)
  }

  test("RangePartitionId: unevaluable") {
    intercept[Exception with SparkThrowable] {
      evaluateWithoutCodegen(RangePartitionId(Literal(2), 10))
    }
  }
}
