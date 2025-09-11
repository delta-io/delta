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

package org.apache.spark.sql.delta.metric

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

abstract class IncrementMetricSuiteBase
  extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._
  import SQLMetrics._

  val ROWS_IN_DF = 1000

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.range(ROWS_IN_DF).toDF("a")
      .withColumn("gb", rand(0).multiply(10).cast("integer"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save("test-df")
  }

  def testDf: DataFrame = spark.read.format("parquet").load("test-df")

  test("Increment the same metric") {
    val metric = createMetric(sparkContext, "metric")
    val increment = IncrementMetric(Literal(true), metric)
    val groupByKey = IncrementMetric(UnresolvedAttribute("gb"), metric)
    val havingIncrement = IncrementMetric(
      GreaterThan(UnresolvedAttribute("s"), Literal(10)), metric)
    val df = testDf
      .filter(Column(increment))
      .groupBy(Column(groupByKey).as("gby"))
      .agg(sum("a").as("s"))
      .filter(Column(havingIncrement))
    val numGroups = df.collect().size
    validatePlan(df.queryExecution.executedPlan)

    assert(metric.value === 2 * ROWS_IN_DF + numGroups)
  }

  test("Increment with filter and conditional") {
    val trueBranchCount = createMetric(sparkContext, "true")
    val falseBranchCount = createMetric(sparkContext, "false")
    val incrementTrueBranch = IncrementMetric(Literal(true), trueBranchCount)
    val incrementFalseBranch = IncrementMetric(Literal(false), falseBranchCount)
    val incrementMetric = createMetric(sparkContext, "increment")
    val increment = IncrementMetric(Literal(true), incrementMetric)
    val incrementPreFilterMetric = createMetric(sparkContext, "incrementPreFilter")
    val incrementPreFilter = IncrementMetric(Literal(true), incrementPreFilterMetric)
    val ifCondition: Expression = ('a < Literal(20)).expr
    val conditional = If(ifCondition, incrementTrueBranch, incrementFalseBranch)
    val df = testDf
      .filter(Column(incrementPreFilter))
      .filter('a < 25)
      .filter(Column(increment))
      .filter(Column(conditional))
    val numRows = df.collect().size
    validatePlan(df.queryExecution.executedPlan)

    assert(incrementPreFilterMetric.value === ROWS_IN_DF)
    assert(trueBranchCount.value === numRows)
    assert(falseBranchCount.value + numRows === incrementMetric.value)
  }

  test("ConditionalIncrementMetric with mixed conditions in filters") {
    val divisibleBy3Metric = createMetric(sparkContext, "divisibleBy3")
    val oneMod7Metric = createMetric(sparkContext, "divisibleBy7")
    val rangeMetric = createMetric(sparkContext, "inRange")
    val largeEvenMetric = createMetric(sparkContext, "largeEven")

    // Count rows divisible by 3 (metric condition) while filtering for divisible by 5
    // (filter inside ConditionalIncrementMetric).
    val divisibleBy5Filter =
      ConditionalIncrementMetric(
        EqualTo(Pmod(UnresolvedAttribute("a"), Literal(5)), Literal(0)),
        EqualTo(Pmod(UnresolvedAttribute("a"), Literal(3)), Literal(0)),
        divisibleBy3Metric)

    // Count numbers where a % 7 == 1 while filtering for a > 10
    // (filter outside ConditionalIncrementMetric).
    val divisibleBy7Condition = EqualTo(Pmod(UnresolvedAttribute("a"), Literal(7)), Literal(1))
    val divisibleBy7Increment =
      ConditionalIncrementMetric(
        UnresolvedAttribute("a"),
        EqualTo(Pmod(UnresolvedAttribute("a"), Literal(7)), Literal(1)),
        oneMod7Metric)
    val greaterThan10Filter = GreaterThan(divisibleBy7Increment, Literal(10))

    // Count rows in range 30-70 while filtering for < 50.
    val rangeCondition = And(
      GreaterThanOrEqual(UnresolvedAttribute("a"), Literal(30)),
      LessThanOrEqual(UnresolvedAttribute("a"), Literal(70))
    )
    val rangeIncrement =
      ConditionalIncrementMetric(
        UnresolvedAttribute("a"),
        rangeCondition,
        rangeMetric)
    val lessThan50Filter = LessThan(rangeIncrement, Literal(50))

    // Count even numbers >= 20 while selecting column a.
    val largeEvenCondition = And(
      GreaterThanOrEqual(UnresolvedAttribute("a"), Literal(20)),
      EqualTo(Pmod(UnresolvedAttribute("a"), Literal(2)), Literal(0))
    )
    val largeEvenIncrement =
      ConditionalIncrementMetric(UnresolvedAttribute("a"), largeEvenCondition, largeEvenMetric)

    val df = testDf
      .filter(Column(divisibleBy5Filter))    // Filter: a % 5 == 0, Metric: counts a % 3 == 0
      .filter(Column(greaterThan10Filter))   // Filter: a > 10, Metric: counts a % 7 == 1
      .filter(Column(lessThan50Filter))      // Filter: a < 80, Metric: counts 30 <= a <= 70
      .select(
        Column(largeEvenIncrement).as("result"), // Metric inside Project: counts even a >= 20
        col("a")
      )
      .filter(col("a") >= 30)  // Additional filter after select.
    val results = df.collect()
    val numRows = results.size
    validatePlan(df.queryExecution.executedPlan)
    // divisibleBy3Metric counts values divisible by 3 among ALL rows (0-999)
    // The ConditionalIncrementMetric outputs (a % 5 == 0) as boolean for filtering,
    // but internally counts when (a % 3 == 0).
    // Divisible by 3: 0,3,6,9,12,15,...,999
    // Count: 1000/3 = 333.33, so 334 values (includes 0)
    assert(divisibleBy3Metric.value === 334)
    // oneMod7Metric counts a % 7 == 1 among rows that pass divisible by 5 filter.
    // Divisible by 5: 0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,...,995 (200 values)
    // Among these, a % 7 == 1: 15,50,85,...,995
    // Pattern: starts at 15, then every 35 (LCM of 5 and 7)
    // Count: (995-15)/35 + 1 = 29 values
    assert(oneMod7Metric.value === 29)

    // After divisible by 5 and > 10 filters: 15,20,25,30,35,40,45,50,55,60,65,70,75,...
    // Among these, in range [30,70]: 30,35,40,45,50,55,60,65,70 = 9 values
    assert(rangeMetric.value === 9)

    // After divisible by 5, > 10, < 50: 15,20,25,30,35,40,45
    // Among these, even and >= 20: 20,30,40 = 3 values.
    assert(largeEvenMetric.value === 3)

    // Final result: divisible by 5, > 10, < 50, >= 30
    // Values: 30,35,40,45 = 4 rows.
    assert(numRows === 4)
  }

  test("ConditionalIncrementMetric with mixed conditions in projections") {
    val trueMetric = createMetric(sparkContext, "conditionTrue")
    val falseMetric = createMetric(sparkContext, "conditionFalse")
    val equalMetric = createMetric(sparkContext, "conditionEqual")

    val trueCondition = LessThan(UnresolvedAttribute("a"), Literal(500))
    val falseCondition = GreaterThan(UnresolvedAttribute("a"), Literal(ROWS_IN_DF))
    val equalCondition = EqualTo(UnresolvedAttribute("a"), Literal(42))

    val trueIncrement = ConditionalIncrementMetric(UnresolvedAttribute("a"), trueCondition,
      trueMetric)
    val falseIncrement = ConditionalIncrementMetric(UnresolvedAttribute("a"), falseCondition,
      falseMetric)
    val equalIncrement = ConditionalIncrementMetric(UnresolvedAttribute("a"), equalCondition,
      equalMetric)

    val df = testDf
      .select(
        Column(trueIncrement).as("true_result"),
        Column(falseIncrement).as("false_result"),
        Column(equalIncrement).as("equal_result")
      )
    val numRows = df.collect().size
    validatePlan(df.queryExecution.executedPlan)

    assert(trueMetric.value === 500) // a < 500: rows 0-499
    assert(falseMetric.value === 0)  // a > 1000: no rows
    assert(equalMetric.value === 1)  // a == 42: exactly 1 row
    assert(numRows === ROWS_IN_DF)
  }

  test("ConditionalIncrementMetric with nullable condition") {
    val metric = createMetric(sparkContext, "nullable_condition")

    // Create a DataFrame with nullable boolean values.
    val df = spark.range(10).selectExpr(
      "id",
      "CASE WHEN id % 3 = 0 THEN null WHEN id % 3 = 1 THEN true ELSE false END AS nullable_bool"
    )

    // Create condition that can be null.
    val conditionExpr = UnresolvedAttribute("nullable_bool")
    val incrementExpr = ConditionalIncrementMetric(
      UnresolvedAttribute("id"),
      conditionExpr,
      metric
    )

    val resultDf = df.select(Column(incrementExpr).as("result"))
    val numRows = resultDf.collect().size
    validatePlan(resultDf.queryExecution.executedPlan)

    // The metric should only count rows where condition is true (not null and not false).
    // id=1,4,7 have nullable_bool=true (3 rows)
    // id=0,3,6,9 have nullable_bool=null (4 rows) - should NOT be counted
    // id=2,5,8 have nullable_bool=false (3 rows) - should NOT be counted
    assert(metric.value === 3)
    assert(numRows === 10)
  }

  test("ConditionalIncrementMetric with invalid condition type") {
    val metric = createMetric(sparkContext, "invalidType")
    val nonBooleanCondition = UnresolvedAttribute("a")  // Integer type
    val increment =
      ConditionalIncrementMetric(UnresolvedAttribute("a"), nonBooleanCondition, metric)

    // This should fail during analysis due to non-boolean condition type.
    val exception = intercept[AnalysisException] {
      val df = testDf.select(Column(increment).as("result"))
      df.collect()
    }
    assert(exception.getErrorClass == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
  }

  for (enabled <- BOOLEAN_DOMAIN) {
    test(s"ConditionalIncrementMetric optimization - literal conditions - enabled=$enabled") {
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_CONDITIONAL_INCREMENT_METRIC_ENABLED.key -> enabled.toString) {
        val trueMetric = createMetric(sparkContext, s"literalTrue$enabled")
        val falseMetric = createMetric(sparkContext, s"literalFalse$enabled")
        val nullMetric = createMetric(sparkContext, s"literalNull$enabled")
        val nonConstMetric = createMetric(sparkContext, s"nonConst$enabled")

        val childExpr = UnresolvedAttribute("a")
        val trueCondition = Literal(true, BooleanType)
        val falseCondition = Literal(false, BooleanType)
        val nullCondition = Literal(null, BooleanType)
        val nonConstCondition = GreaterThan(UnresolvedAttribute("a"), Literal(100))

        val trueExpr = ConditionalIncrementMetric(childExpr, trueCondition, trueMetric)
        val falseExpr = ConditionalIncrementMetric(childExpr, falseCondition, falseMetric)
        val nullExpr = ConditionalIncrementMetric(childExpr, nullCondition, nullMetric)
        val nonConstExpr = ConditionalIncrementMetric(childExpr, nonConstCondition, nonConstMetric)

        val df = testDf.select(
          Column(trueExpr).as("true_result"),
          Column(falseExpr).as("false_result"),
          Column(nullExpr).as("null_result"),
          Column(nonConstExpr).as("nonconst_result")
        )

        // Check optimized plan transformations.
        val optimizedPlan = df.queryExecution.optimizedPlan
        var conditionalCount = 0
        var nonConditionalCount = 0
        optimizedPlan.foreach {
          _.transformExpressions {
            case e: ConditionalIncrementMetric =>
              conditionalCount += 1
              e
            case e: IncrementMetric =>
              nonConditionalCount += 1
              e
          }
        }

        if (enabled) {
          assert(conditionalCount === 1,
            s"ConditionalIncrementMetric with non-const cond should remain unchanged." +
              s"\n$optimizedPlan")
          assert(nonConditionalCount === 1,
            s"ConditionalIncrementMetric with cond true should become IncrementMetric" +
              s"\n$optimizedPlan")
          // ConditionalIncrementMetric with condition false or null have gotten removed.
        } else {
          assert(conditionalCount === 4,
            s"All ConditionalIncrementMetric expressions should remain unchanged when " +
              s"optimization is disabled.\n$optimizedPlan")
          assert(nonConditionalCount === 0,
            s"No ConditionalIncrementMetric should be converted to IncrementMetric when " +
              s"optimization is disabled.\n$optimizedPlan")
        }

        // Verify metrics work correctly regardless of optimization state.
        df.collect()

        validatePlan(df.queryExecution.executedPlan)

        assert(trueMetric.value === ROWS_IN_DF)  // All rows counted (true condition)
        assert(falseMetric.value === 0)          // No rows counted (false condition)
        assert(nullMetric.value === 0)           // No rows counted (null condition)
        assert(nonConstMetric.value === 899)   // Rows where a > 100 (101-999)
      }
    }
  }

  test("ConditionalIncrementMetric with all-null condition column") {
    val metric = createMetric(sparkContext, "allNullCondition")

    // Create a DataFrame where the condition column is always null.
    val df = spark.range(10).selectExpr("id", "try_divide(id, 0) < 0 as null_condition")

    // Use the null condition column (not a literal, but all values are null).
    val incrementExpr = ConditionalIncrementMetric(
      UnresolvedAttribute("id"),
      UnresolvedAttribute("null_condition"),
      metric)

    val resultDf = df.select(Column(incrementExpr).as("result"))

    // This should NOT be optimized away since it's not a literal condition.
    val optimizedPlan = resultDf.queryExecution.optimizedPlan
    var conditionalCount = 0
    optimizedPlan.foreach {
      _.transformExpressions {
        case e: ConditionalIncrementMetric =>
          conditionalCount += 1
          e
      }
    }

    assert(conditionalCount === 1,
      s"Non-literal all-null condition should preserve ConditionalIncrementMetric\n$optimizedPlan")

    val numRows = resultDf.collect().size
    validatePlan(resultDf.queryExecution.executedPlan)

    // The metric should be 0 since all condition values are null.
    assert(metric.value === 0, "All-null condition should count 0 rows")
    assert(numRows === 10)
  }

  protected def validatePlan(plan: SparkPlan): Unit = {}

}

class IncrementMetricSuite extends IncrementMetricSuiteBase {}
