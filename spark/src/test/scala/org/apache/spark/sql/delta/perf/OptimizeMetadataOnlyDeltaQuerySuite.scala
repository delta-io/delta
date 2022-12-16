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

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset

import scala.collection.mutable

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession

class OptimizeMetadataOnlyDeltaQuerySuite
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterAll
    with DeltaSQLCommandTest {
  val testTableName = "table_basic"
  val noStatsTableName = " table_nostats"
  val mixedStatsTableName = " table_mixstats"

  var dfPart1: DataFrame = null
  var dfPart2: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    dfPart1 = generateRowsDataFrame(spark.range(1L, 6L))
    dfPart2 = generateRowsDataFrame(spark.range(6L, 11L))

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(noStatsTableName)
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(mixedStatsTableName)

      spark.sql(s"DELETE FROM $noStatsTableName WHERE id = 1")
      spark.sql(s"DELETE FROM $mixedStatsTableName WHERE id = 1")

      dfPart2.write.format("delta").mode("append").saveAsTable(noStatsTableName)
    }

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(testTableName)

      spark.sql(s"DELETE FROM $testTableName WHERE id = 1")

      dfPart2.write.format("delta").mode(SaveMode.Append).saveAsTable(testTableName)
      dfPart2.write.format("delta").mode(SaveMode.Append).saveAsTable(mixedStatsTableName)

      // Run updates to generate more Delta Log and trigger a checkpoint
      // and make sure stats works after checkpoints
      for (a <- 1 to 10) {
        spark.sql(s"UPDATE $testTableName SET data='$a' WHERE id = 7")
      }
      spark.sql(s"UPDATE $testTableName SET data=NULL WHERE id = 7")

      // Creates an empty (numRecords == 0) AddFile record
      generateRowsDataFrame(spark.range(11L, 12L))
        .write.format("delta").mode("append").saveAsTable(testTableName)
      spark.sql(s"DELETE FROM $testTableName WHERE id = 11")
    }
  }

  test("Select Count: basic") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName",
      "LocalRelation [none#0L]")
  }

  test("Select Count: column alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) as MyColumn FROM $testTableName",
      "LocalRelation [none#0L]")
  }

  test("Select Count: table alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName MyTable",
      "LocalRelation [none#0L]")
  }

  test("Select Count: time travel") {
    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 0",
      "LocalRelation [none#0L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 1",
      "LocalRelation [none#0L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*) FROM $testTableName VERSION AS OF 2",
      "LocalRelation [none#0L]")
  }

  test("Select Count: external") {
    withTempDir { dir =>
      val testTablePath = dir.getAbsolutePath
      dfPart1.write.format("delta").mode("overwrite").save(testTablePath)
      DeltaTable.forPath(spark, testTablePath).delete("id = 1")
      dfPart2.write.format("delta").mode(SaveMode.Append).save(testTablePath)

      checkResultsAndOptimizedPlan(
        s"SELECT COUNT(*) FROM delta.`$testTablePath`",
        "LocalRelation [none#0L]")
    }
  }

  test("Select Count: sub-query") {
    checkResultsAndOptimizedPlan(
      s"SELECT (SELECT COUNT(*) FROM $testTableName)",
      "Project [scalar-subquery#0 [] AS #0L]\n:  +- LocalRelation [none#0L]\n+- OneRowRelation")
  }

  test("Select Count: as sub-query filter") {
    val result = spark.sql(s"SELECT COUNT(*) FROM $testTableName").head
    val totalRows = result.getLong(0)

    checkResultsAndOptimizedPlan(
      s"SELECT 'ABC' WHERE" +
        s" (SELECT COUNT(*) FROM $testTableName) = $totalRows",
      "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        totalRows + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation")
  }

  test("Select Count: limit") {
    // Limit doesn't affect COUNT results
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName LIMIT 3",
      "LocalRelation [none#0L]")
  }

  test("Select Count: empty table") {
    sql(s"CREATE TABLE TestEmpty (c1 int) USING DELTA")

    val query = "SELECT COUNT(*) FROM TestEmpty"

    checkResultsAndOptimizedPlan(query, "LocalRelation [none#0L]")
  }

  test("Select Count: snapshot isolation") {
    sql(s"CREATE TABLE TestSnapshotIsolation (c1 int) USING DELTA")
    spark.sql("INSERT INTO TestSnapshotIsolation VALUES (1)")

    val scannedVersions = mutable.ArrayBuffer[Long]()
    val query = "SELECT (SELECT COUNT(*) FROM TestSnapshotIsolation), " +
      "(SELECT COUNT(*) FROM TestSnapshotIsolation)"

    checkResultsAndOptimizedPlan(
      query,
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
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $mixedStatsTableName")

    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $noStatsTableName")
  }

  // Tests to validate the optimizer won't incorrectly change queries it can't correctly handle
  test("Select Count: multiple aggregations") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) AS MyCount, MAX(id) FROM $testTableName")
  }

  test("Select Count: group by") {
    checkOptimizationIsNotTriggered(
      s"SELECT group, COUNT(*) FROM $testTableName GROUP BY group")
  }

  test("Select Count: count twice") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*), COUNT(*) FROM $testTableName")
  }

  test("Select Count: plus literal") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) + 1 FROM $testTableName")
  }

  test("Select Count: distinct") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(DISTINCT data) FROM $testTableName")
  }

  test("Select Count: filter") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $testTableName WHERE id > 0")
  }

  test("Select Count: sub-query with filter") {
    checkOptimizationIsNotTriggered(
      s"SELECT (SELECT COUNT(*) FROM $testTableName WHERE id > 0)")
  }

  test("Select Count: non-null") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(ALL data) FROM $testTableName")
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(data) FROM $testTableName")
  }

  test("Select Count: join") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $testTableName A, $testTableName B")
  }

  test("Select Count: over") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) OVER() FROM $testTableName LIMIT 1")
  }

  private def generateRowsDataFrame(source: Dataset[java.lang.Long]): DataFrame = {
    import testImplicits._
    import org.apache.spark.sql.functions._

    source.select('id,
      'id.cast("tinyint") as 'TinyIntColumn,
      'id.cast("smallint") as 'SmallIntColumn,
      'id.cast("int") as 'IntColumn,
      'id.cast("bigint") as 'BigIntColumn,
      ('id / 3.3).cast("float") as 'FloatColumn,
      ('id / 3.3).cast("double") as 'DoubleColumn,
      date_add(lit("2022-08-31").cast("date"), col("id").cast("int")) as 'DateColumn,
      ('id % 2).cast("integer") as 'group,
      'id.cast("string") as 'data)
  }

  /** Validate the results of the query is the same with the flag
   * DELTA_OPTIMIZE_METADATA_QUERY_ENABLED enabled and disabled.
   * And the expected Optimized Query Plan with the flag enabled */
  private def checkResultsAndOptimizedPlan(
    query: String,
    expectedOptimizedPlan: String): Unit = {
    checkResultsAndOptimizedPlan(() => spark.sql(query), expectedOptimizedPlan)
  }

  /** Validate the results of the query is the same with the flag
   * DELTA_OPTIMIZE_METADATA_QUERY_ENABLED enabled and disabled.
   * And the expected Optimized Query Plan with the flag enabled. */
  private def checkResultsAndOptimizedPlan(
    generateQueryDf: () => DataFrame,
    expectedOptimizedPlan: String): Unit = {
    var expectedAnswer: scala.Seq[org.apache.spark.sql.Row] = null
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {
      expectedAnswer = generateQueryDf().collect()
    }

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {
      val queryDf = generateQueryDf()
      val optimizedPlan = queryDf.queryExecution.optimizedPlan.canonicalized.toString()

      assert(queryDf.collect().sameElements(expectedAnswer))

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
   */
  private def checkOptimizationIsNotTriggered(query: String) {
    var expectedOptimizedPlan: String = null
    var expectedAnswer: scala.Seq[org.apache.spark.sql.Row] = null

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {

      val generateQueryDf = spark.sql(query)
      expectedOptimizedPlan = generateQueryDf.queryExecution.optimizedPlan
        .canonicalized.toString()
      expectedAnswer = generateQueryDf.collect()
    }

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {

      val generateQueryDf = spark.sql(query)
      val optimizationEnabledQueryPlan = generateQueryDf.queryExecution.optimizedPlan
        .canonicalized.toString()

      assert(generateQueryDf.collect().sameElements(expectedAnswer))

      assertResult(expectedOptimizedPlan) {
        optimizationEnabledQueryPlan
      }
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
