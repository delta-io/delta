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


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, GreaterThan, If, Literal}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

abstract class IncrementMetricSuiteBase extends QueryTest with SharedSparkSession {
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
      .filter(new Column(increment))
      .groupBy(new Column(groupByKey).as("gby"))
      .agg(sum("a").as("s"))
      .filter(new Column(havingIncrement))
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
      .filter(new Column(incrementPreFilter))
      .filter('a < 25)
      .filter(new Column(increment))
      .filter(new Column(conditional))
    val numRows = df.collect().size
    validatePlan(df.queryExecution.executedPlan)

    assert(incrementPreFilterMetric.value === ROWS_IN_DF)
    assert(trueBranchCount.value === numRows)
    assert(falseBranchCount.value + numRows === incrementMetric.value)
  }

  protected def validatePlan(plan: SparkPlan): Unit = {}

}

class IncrementMetricSuite extends IncrementMetricSuiteBase {}
