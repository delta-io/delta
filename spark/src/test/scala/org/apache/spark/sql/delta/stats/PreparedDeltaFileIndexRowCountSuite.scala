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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to verify when preparedScan.scanned.rows is populated in PreparedDeltaFileIndex,
 * and the behavior of the DELTA_ALWAYS_COLLECT_STATS flag.
 */
class PreparedDeltaFileIndexRowCountSuite
    extends QueryTest
    with DeltaSQLCommandTest {

  import testImplicits._

  private def getDeltaScan(df: DataFrame): DeltaScan = {
    val scans = df.queryExecution.optimizedPlan.collect {
      case DeltaTable(prepared: PreparedDeltaFileIndex) => prepared.preparedScan
    }
    assert(scans.size == 1, s"Expected 1 DeltaScan, found ${scans.size}")
    scans.head
  }

  /**
   * Test utility that creates a partitioned Delta table and verifies scanned.rows and
   * scanned.logicalRows behavior.
   *
   * @param alwaysCollectStats value of the DELTA_ALWAYS_COLLECT_STATS flag
   * @param queryTransform function to transform the base DataFrame (apply filters)
   * @param expectedRowsDefined whether scanned.rows should be defined
   * @param expectedRowCount expected row count if defined (None to skip validation)
   * @param expectedLogicalRowsDefined whether scanned.logicalRows should be defined
   *                                   (defaults to same as expectedRowsDefined)
   * @param expectedLogicalRowCount expected logical row count if defined (None to skip validation)
   */
  private def testRowCountBehavior(
      alwaysCollectStats: Boolean,
      queryTransform: DataFrame => DataFrame,
      expectedRowsDefined: Boolean,
      expectedRowCount: Option[Long] = None,
      expectedLogicalRowsDefined: Option[Boolean] = None,
      expectedLogicalRowCount: Option[Long] = None): Unit = {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
        spark.range(100).toDF("id")
          .withColumn("part", $"id" % 4)
          .repartition(4)
          .write.format("delta").partitionBy("part").save(dir.getAbsolutePath)
      }

      DeltaLog.clearCache()

      withSQLConf(DeltaSQLConf.DELTA_ALWAYS_COLLECT_STATS.key -> alwaysCollectStats.toString) {
        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        val scan = getDeltaScan(queryTransform(df))

        if (expectedRowsDefined) {
          assert(scan.scanned.rows.isDefined, "scanned.rows should be defined")
          expectedRowCount.foreach { expected =>
            assert(scan.scanned.rows.get == expected,
              s"Expected $expected rows, got ${scan.scanned.rows.get}")
          }
        } else {
          assert(scan.scanned.rows.isEmpty, "scanned.rows should be None")
        }

        // logicalRows should follow the same defined/undefined pattern as rows by default
        val logicalDefined = expectedLogicalRowsDefined.getOrElse(expectedRowsDefined)
        if (logicalDefined) {
          assert(scan.scanned.logicalRows.isDefined, "scanned.logicalRows should be defined")
          expectedLogicalRowCount.foreach { expected =>
            assert(scan.scanned.logicalRows.get == expected,
              s"Expected $expected logical rows, got ${scan.scanned.logicalRows.get}")
          }
        } else {
          assert(scan.scanned.logicalRows.isEmpty, "scanned.logicalRows should be None")
        }
      }
    }
  }

  // Define query cases: (name, transform function, always collects rows)
  private val queryCases: Seq[(String, DataFrame => DataFrame, Boolean)] = Seq(
    ("no filter", identity[DataFrame], false),
    ("TrueLiteral filter", _.where(lit(true)), false),
    ("partition filter only", _.where($"part" === 1), false),
    ("data filter", _.where($"id" === 50), true),
    ("partition + data filter", _.where($"part" === 1).where($"id" === 49), true)
  )

  // Grid test: all query cases x flag values
  for {
    (caseName, queryTransform, alwaysCollectsRows) <- queryCases
    alwaysCollectStats <- Seq(false, true)
  } {
    val flagDesc = s"alwaysCollectStats=$alwaysCollectStats"
    // If the query type always collects rows, rows is always defined; otherwise depends on flag
    val expectedRowsDefined = alwaysCollectsRows || alwaysCollectStats

    test(s"$caseName - $flagDesc") {
      testRowCountBehavior(
        alwaysCollectStats = alwaysCollectStats,
        queryTransform = queryTransform,
        expectedRowsDefined = expectedRowsDefined
      )
    }
  }

  test("alwaysCollectStats with missing stats returns None") {
    withTempDir { dir =>
      // Create table without stats
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        spark.range(100).toDF("id")
          .write.format("delta").save(dir.getAbsolutePath)
      }

      DeltaLog.clearCache()

      withSQLConf(DeltaSQLConf.DELTA_ALWAYS_COLLECT_STATS.key -> "true") {
        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        val scan = getDeltaScan(df)
        assert(scan.scanned.rows.isEmpty, "scanned.rows should be None when stats are missing")
        assert(scan.scanned.logicalRows.isEmpty,
          "scanned.logicalRows should be None when stats are missing")
      }
    }
  }

}
