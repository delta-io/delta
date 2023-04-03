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

package org.apache.spark.sql.delta.perf

import scala.collection.mutable

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class OptimizeMetadataOnlyDeltaQuerySuite
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterAll
    with DeltaSQLCommandTest {
  val testTableName = "table_basic"
  val testTablePath = Utils.createTempDir().getAbsolutePath
  val noStatsTableName = " table_nostats"
  val mixedStatsTableName = " table_mixstats"
  val totalRows = 9L
  val totalNonNullData = 8L
  val totalDistinctData = 5L

  override def beforeAll(): Unit = {
    super.beforeAll()
    val df = spark.createDataFrame(Seq((1L, "a", 1L), (2L, "b", 1L), (3L, "c", 1L)))
      .toDF("id", "data", "group")
    val df2 = spark.createDataFrame(Seq(
      (4L, "d", 1L),
      (5L, "e", 1L),
      (6L, "f", 1L),
      (7L, null, 1L),
      (8L, "b", 1L),
      (9L, "b", 1L),
      (10L, "b", 1L))).toDF("id", "data", "group")

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      df.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(noStatsTableName)
      df.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(mixedStatsTableName)

      spark.sql(s"DELETE FROM $noStatsTableName WHERE id = 1")
      spark.sql(s"DELETE FROM $mixedStatsTableName WHERE id = 1")

      df2.write.format("delta").mode("append").saveAsTable(noStatsTableName)
    }

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      import io.delta.tables._

      df.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(testTableName)
      df.write.format("delta").mode(SaveMode.Overwrite).save(testTablePath)

      spark.sql(s"DELETE FROM $testTableName WHERE id = 1")
      DeltaTable.forPath(spark, testTablePath).delete("id = 1")

      df2.write.format("delta").mode(SaveMode.Append).saveAsTable(testTableName)
      df2.write.format("delta").mode(SaveMode.Append).save(testTablePath)
      df2.write.format("delta").mode(SaveMode.Append).saveAsTable(mixedStatsTableName)
    }
  }

  test("Select Count: basic") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: column alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) as MyColumn FROM $testTableName",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: table alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName MyTable",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: time travel") {
    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 0",
      Seq(Row(3L)),
      "LocalRelation [none#0L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 1",
      Seq(Row(2L)),
      "LocalRelation [none#0L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 2",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: external") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM delta.`$testTablePath`",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: sub-query") {
    checkResultsAndOptimizedPlan(
      s"SELECT (SELECT COUNT(*) FROM $testTableName)",
      Seq(Row(totalRows)),
      "Project [scalar-subquery#0 [] AS #0L]\n:  +- LocalRelation [none#0L]\n+- OneRowRelation")
  }

  test("Select Count: as sub-query filter") {
    checkResultsAndOptimizedPlan(
      s"SELECT 'ABC' WHERE (SELECT COUNT(*) FROM $testTableName) = $totalRows",
      Seq(Row("ABC")),
      "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        totalRows + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation")
  }

  test("Select Count: limit") {
    // Limit doesn't affect COUNT results
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName LIMIT 3",
      Seq(Row(totalRows)),
      "LocalRelation [none#0L]")
  }

  test("Select Count: empty table") {
    sql(s"CREATE TABLE TestEmpty (c1 int) USING DELTA")

    val query = "SELECT COUNT(*) FROM TestEmpty"

    checkResultsAndOptimizedPlan(query, Seq(Row(0)), "LocalRelation [none#0L]")
  }

  test("Select Count: snapshot isolation") {
    sql(s"CREATE TABLE TestSnapshotIsolation (c1 int) USING DELTA")
    spark.sql("INSERT INTO TestSnapshotIsolation VALUES (1)")

    val scannedVersions = mutable.ArrayBuffer[Long]()
    val query = "SELECT (SELECT COUNT(*) FROM TestSnapshotIsolation), " +
      "(SELECT COUNT(*) FROM TestSnapshotIsolation)"

    checkResultsAndOptimizedPlan(
      query,
      Seq(Row(1, 1)),
      "Project [scalar-subquery#0 [] AS #0L, scalar-subquery#0 [] AS #1L]\n" +
        ":  :- LocalRelation [none#0L]\n" +
        ":  +- LocalRelation [none#0L]\n" +
        "+- OneRowRelation")

    PrepareDeltaScanBase.withCallbackOnGetDeltaScanGenerator(scanGenerator => {
      // Record the scanned version and make changes to the table. We will verify changes in the
      // middle of the query are not visible to the query.
      scannedVersions += scanGenerator.snapshotToScan.version
      // Insert a row after each call to get scanGenerator
      // to test if the count doesn't change in the same query
      spark.sql("INSERT INTO TestSnapshotIsolation VALUES (1)")
    }) {
      val result = spark.sql(query).collect()(0)
      val c1 = result.getLong(0)
      val c2 = result.getLong(1)
      assertResult(c1, "Snapshot isolation should guarantee the results are always the same")(c2)
      assert(
        scannedVersions.toSet.size == 1,
        s"Scanned multiple versions of the same table in one query: ${scannedVersions.toSet}")
    }
  }

  // Tests to validate the optimizer won't use missing or partial stats
  test("Select Count: missing stats") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) FROM $mixedStatsTableName",
      Seq(Row(totalRows)))

    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) FROM $noStatsTableName",
      Seq(Row(totalRows)))
  }

  // Tests to validate the optimizer won't incorrectly change queries it can't correctly handle
  test("Select Count: multiple aggregations") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) AS MyCount, MAX(id) FROM $testTableName",
      Seq(Row(totalRows, 10L)))
  }

  test("Select Count: group by") {
    checkSameQueryPlanAndResults(
      s"SELECT group, COUNT(*) FROM $testTableName GROUP BY group",
      Seq(Row(1L, totalRows)))
  }

  test("Select Count: count twice") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*), COUNT(*) FROM $testTableName",
      Seq(Row(totalRows, totalRows)))
  }

  test("Select Count: plus literal") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) + 1 FROM $testTableName",
      Seq(Row(totalRows + 1)))
  }

  test("Select Count: distinct") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(DISTINCT data) FROM $testTableName",
      Seq(Row(totalDistinctData)))
  }

  test("Select Count: filter") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) FROM $testTableName WHERE id > 0",
      Seq(Row(totalRows)))
  }

  test("Select Count: sub-query with filter") {
    checkSameQueryPlanAndResults(
      s"SELECT (SELECT COUNT(*) FROM $testTableName WHERE id > 0)",
      Seq(Row(totalRows)))
  }

  test("Select Count: non-null") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(ALL data) FROM $testTableName",
      Seq(Row(totalNonNullData)))
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(data) FROM $testTableName",
      Seq(Row(totalNonNullData)))
  }

  test("Select Count: join") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) FROM $testTableName A, $testTableName B",
      Seq(Row(totalRows * totalRows)))
  }

  test("Select Count: over") {
    checkSameQueryPlanAndResults(
      s"SELECT COUNT(*) OVER() FROM $testTableName LIMIT 1",
      Seq(Row(totalRows)))
  }

  private def checkResultsAndOptimizedPlan(
    query: String,
    expectedAnswer: scala.Seq[Row],
    expectedOptimizedPlan: String): Unit = {
    checkResultsAndOptimizedPlan(() => spark.sql(query), expectedAnswer, expectedOptimizedPlan)
  }

  private def checkResultsAndOptimizedPlan(
    generateQueryDf: () => DataFrame,
    expectedAnswer: scala.Seq[Row],
    expectedOptimizedPlan: String): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {
      val queryDf = generateQueryDf()
      val optimizedPlan = queryDf.queryExecution.optimizedPlan.canonicalized.toString()

      assertResult(expectedAnswer(0)(0)) {
        queryDf.collect()(0)(0)
      }

      assertResult(expectedOptimizedPlan.trim) {
        optimizedPlan.trim
      }
    }
  }

  /**
   * Verify the query plans and results are the same with/without metadata query optimization.
   * This method can be used to verify cases that we shouldn't trigger optimization
   * or cases that we can potentially improve.
   * @param query
   * @param expectedAnswer
   */
  private def checkSameQueryPlanAndResults(
    query: String,
    expectedAnswer: scala.Seq[Row]) {
    var optimizationEnabledQueryPlan: String = null
    var optimizationDisabledQueryPlan: String = null

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {

      val queryDf = spark.sql(query)
      optimizationEnabledQueryPlan = queryDf.queryExecution.optimizedPlan
        .canonicalized.toString()
      checkAnswer(queryDf, expectedAnswer)
    }

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {

      val countQuery = spark.sql(query)
      optimizationDisabledQueryPlan = countQuery.queryExecution.optimizedPlan
        .canonicalized.toString()
      checkAnswer(countQuery, expectedAnswer)
    }

    assertResult(optimizationEnabledQueryPlan) {
      optimizationDisabledQueryPlan
    }
  }

  // scalastyle:off println
  test(".collect() and .show() both use this optimization") {
    val collectPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT COUNT(*) FROM $testTableName").collect()
    }
    val collectResultData = collectPlans.collect { case x: LocalRelation => x.data }
    assert(collectResultData.size === 1)
    assert(collectResultData.head.head.getLong(0) === totalRows)

    val showPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT COUNT(*) FROM $testTableName").show()
    }
    val showResultData = showPlans.collect { case x: LocalRelation => x.data }
    assert(showResultData.size === 1)
    assert(showResultData.head.head.getString(0).toLong === totalRows)
  }
}
