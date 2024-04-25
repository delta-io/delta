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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.IdentityColumn.IdentityInfo

import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, If, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class GenerateIdentityValuesSuite extends QueryTest with SharedSparkSession {
  private val colName = "id"

  /**
   * Verify the generated IDENTITY values are correct.
   *
   * @param df A DataFrame with a single column containing all the generated IDENTITY values.
   * @param identityInfo IDENTITY information used for verification.
   * @param rowCount Expected row count.
   */
  private def verifyIdentityValues(
      df: => DataFrame,
      identityInfo: IdentityInfo,
      rowCount: Long): Unit = {

    // Check row count is expected.
    checkAnswer(df.select(count(col(colName))), Row(rowCount))

    // Check there is no duplicate.
    checkAnswer(df.select(count_distinct(new Column(colName))), Row(rowCount))

    // Check every value follows start and step configuration
    val condViolateConfig = s"($colName - ${identityInfo.start}) % ${identityInfo.step} != 0"
    checkAnswer(df.where(condViolateConfig), Seq.empty)

    // Check every value is after high watermark OR >= start.
    val highWaterMark = identityInfo.highWaterMark.getOrElse(identityInfo.start - identityInfo.step)
    val condViolateHighWaterMark = s"(($colName - $highWaterMark)/${identityInfo.step}) < 0"
    checkAnswer(df.where(condViolateHighWaterMark), Seq.empty)

    // When high watermark is empty, the first value should be start.
    if (identityInfo.highWaterMark.isEmpty) {
      val agg = if (identityInfo.step > 0) min(new Column(colName)) else max(new Column(colName))
      checkAnswer(df.select(agg), Row(identityInfo.start))
    }
  }

  test("basic") {
    val sizes = Seq(100, 1000, 10000)
    val slices = Seq(2, 7, 15)
    val starts = Seq(-3, 0, 1, 5, 43)
    val steps = Seq(-3, -2, -1, 1, 2, 3)
    for (size <- sizes; slice <- slices; start <- starts; step <- steps) {
      val highWaterMarks = Seq(None, Some((start + 100 * step).toLong))
      val df = spark.range(1, size + 1, 1, slice).toDF(colName)
      highWaterMarks.foreach { highWaterMark =>
        verifyIdentityValues(
          df.select(new Column(GenerateIdentityValues(start, step, highWaterMark)).alias(colName)),
          IdentityInfo(start, step, highWaterMark),
          size
        )
      }
    }
  }

  test("shared state") {
    val size = 10000
    val slice = 7
    val start = -1
    val step = 3
    val highWaterMarks = Seq(None, Some((start + 100 * step).toLong))
    val df = spark.range(1, size + 1, 1, slice).toDF(colName)
    highWaterMarks.foreach { highWaterMark =>
      // Create two GenerateIdentityValues expressions that share the same state. They should
      // generate distinct values.
      val gev = GenerateIdentityValues(start, step, highWaterMark)
      val gev2 = gev.copy()
      verifyIdentityValues(
        df.select(new Column(
            If(GreaterThan(col(colName).expr, right = Literal(10)), gev, gev2)).alias(colName)),
        IdentityInfo(start, step, highWaterMark),
        size
      )
    }
  }

  test("bigint value range") {
    val size = 1000
    val slice = 32
    val start = Integer.MAX_VALUE.toLong + 1
    val step = 10
    val highWaterMark = start - step
    val df = spark.range(1, size + 1, 1, slice).toDF(colName)
    verifyIdentityValues(
      df.select(
        new Column(GenerateIdentityValues(start, step, Some(highWaterMark))).alias(colName)),
      IdentityInfo(start, step, Some(highWaterMark)),
      size
    )
  }

  test("overflow initial value") {
    val events = Log4jUsageLogger.track {
      val df = spark.range(1, 10, 1, 5).toDF(colName)
        .select(new Column(GenerateIdentityValues(
          start = 2,
          step = Long.MaxValue,
          highWaterMarkOpt = Some(2 - Long.MaxValue))))
      val ex = intercept[SparkException] {
        df.collect()
      }
      assert(ex.getMessage.contains("java.lang.ArithmeticException: long overflow"))
    }
    val filteredEvents = events.filter { e =>
      e.tags.get("opType").exists(_ == "delta.identityColumn.overflow")
    }
    assert(filteredEvents.size > 0)
  }

  test("overflow next") {
    val events = Log4jUsageLogger.track {
      val df = spark.range(1, 10, 1, 5).toDF(colName)
        .select(new Column(GenerateIdentityValues(
          start = Long.MaxValue - 1,
          step = 2,
          highWaterMarkOpt = Some(Long.MaxValue - 3))))
      val ex = intercept[SparkException] {
        df.collect()
      }
      assert(ex.getMessage.contains("java.lang.ArithmeticException: long overflow"))
    }
    val filteredEvents = events.filter { e =>
      e.tags.get("opType").exists(_ == "delta.identityColumn.overflow")
    }
    assert(filteredEvents.size > 0)
  }

  test("invalid high water mark") {
    val df = spark.range(1, 10, 1, 5).toDF(colName)
    intercept[IllegalArgumentException] {
      df.select(new Column(GenerateIdentityValues(
        start = 1,
        step = 2,
        highWaterMarkOpt = Some(4)))
      ).collect()
    }
  }

  test("invalid step") {
    val df = spark.range(1, 10, 1, 5).toDF(colName)
    intercept[IllegalArgumentException] {
      df.select(new Column(GenerateIdentityValues(
        start = 1,
        step = 0,
        highWaterMarkOpt = Some(4)))
      ).collect()
    }
  }
}
