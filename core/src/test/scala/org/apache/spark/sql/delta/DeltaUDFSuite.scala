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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.{Encoder, QueryTest, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class DeltaUDFSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def testUDF(
      name: String,
      testResultFunc: => Unit): Unit = {
    test(name) {
      // Verify the returned UDF function is working correctly
      testResultFunc
    }
  }

  private def testUDF(
      name: String,
      func: => UserDefinedFunction,
      expected: Any): Unit = {
    testUDF(
      name,
      checkAnswer(Seq("foo").toDF.select(func()), Row(expected))
    )
  }

  private def testUDF[T: Encoder](
      name: String,
      func: => UserDefinedFunction,
      input: T,
      expected: Any): Unit = {
    testUDF(
      name,
      checkAnswer(Seq(input).toDF.select(func(col("value"))), Row(expected))
    )
  }

  private def testUDF[T1: Encoder, T2: Encoder](
      name: String,
      func: => UserDefinedFunction,
      input1: T1,
      input2: T2,
      expected: Any): Unit = {
    testUDF(
      name,
      {
        val df = Seq(input1)
          .toDF("value1")
          .withColumn("value2", lit(input2).as[T2])
          .select(func(col("value1"), col("value2")))
        checkAnswer(df, Row(expected))
      }
    )
  }

  testUDF(
    name = "stringFromString",
    func = DeltaUDF.stringFromString(x => x),
    input = "foo",
    expected = "foo")
  testUDF(
    name = "intFromString",
    func = DeltaUDF.intFromString(x => x.toInt),
    input = "100",
    expected = 100)
  testUDF(
    name = "intFromStringBoolean",
    func = DeltaUDF.intFromStringBoolean((x, y) => 1),
    input1 = "foo",
    input2 = true,
    expected = 1)
  testUDF(name = "boolean", func = DeltaUDF.boolean(() => true), expected = true)
  testUDF(
    name = "stringFromMap",
    func = DeltaUDF.stringFromMap(x => x.toString),
    input = Map("foo" -> "bar"),
    expected = "Map(foo -> bar)")
  testUDF(
    name = "booleanFromMap",
    func = DeltaUDF.booleanFromMap(x => x.isEmpty),
    input = Map("foo" -> "bar"),
    expected = false)
}
