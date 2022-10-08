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

package org.apache.spark.sql.delta.stats


import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.DeltaTableScan
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.ScanReportHelper
import org.scalatest.GivenWhenThen

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait AggregatePushDownSuiteBase extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with GivenWhenThen
    with ScanReportHelper {

  import testImplicits._

  protected override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key, "true")
      .set(DeltaSQLConf.V2_READER_ENABLED.key, "true")

  def checkPushedAggregations(
    data: DataFrame,
    aggs: Seq[Column],
    filter: String = "true",
    groupBy: Seq[String] = Seq.empty,
    result: Seq[Row] = Seq.empty,
    pushedAggString: String = ""
  ): Unit = {
    val plans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
      checkAnswer(
        data.filter(filter).groupBy(groupBy.map(col): _*).agg(aggs.head, aggs.tail: _*),
        result
      )
    }
    val scans = plans.flatMap(_.collect {
      case BatchScanExec(_, s: DeltaTableScan, _, _) => s
    })
    assert(scans.length == 1)
    assert(scans.head.getMetaData().get("PushedAggregation").get == s"[$pushedAggString]")
  }

  test("Aggregates are pushed down") {
    withTempDir { inputDir =>
      val testPath = inputDir.getCanonicalPath
      spark.range(10)
        .withColumn("part", $"id" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      val df = spark.read.format("delta").load(testPath)

      Given("No filter or group by")
      checkPushedAggregations(
        df,
        Seq(min($"id"), max($"id"), count($"id")),
        result = Seq(Row(0, 9, 10)),
        pushedAggString = "MIN(id), MAX(id), COUNT(id)")

      Given("Group by on partition")
      checkPushedAggregations(
        df,
        Seq(min($"id"), max($"id"), count($"id")),
        groupBy = Seq("part"),
        result = Seq(Row(0, 0, 8, 5), Row(1, 1, 9, 5)),
        pushedAggString = "MIN(id), MAX(id), COUNT(id)")

      Given("Filter on partition")
      checkPushedAggregations(
        df,
        Seq(min($"id"), max($"id"), count($"id")),
        filter = "part = 1",
        result = Seq(Row(1, 9, 5)),
        pushedAggString = "MIN(id), MAX(id), COUNT(id)")
    }
  }
}

class AggregatePushDownSuite extends AggregatePushDownSuiteBase
