/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Coverage for reading CDF via the existing Delta APIs (table_changes(),
 * .option("readChangeData", "true")), but going through Spark DSv2 CDC read implementation.
 */
class DeltaV2CDFSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    // CDF defaults to off so the row-tracking table exercises the read-time CDF route.
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "false")
    // `ApplyV2CDF` delegates to `ChangelogSupport.loadChangelog`, which is feature-gated.
    .set(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED.key, "true")

  private def createRowTrackingTable(tbl: String): Unit = {
    sql(
      s"""CREATE TABLE $tbl (id BIGINT, name STRING) USING delta TBLPROPERTIES (
         |  'delta.enableChangeDataFeed' = 'false',
         |  'delta.enableRowTracking' = 'true',
         |  'delta.enableDeletionVectors' = 'false'
         |)""".stripMargin)
    sql(s"INSERT INTO $tbl VALUES (1, 'Alice')")   // v1
    sql(s"INSERT INTO $tbl VALUES (2, 'Bob')")     // v2
    sql(s"INSERT INTO $tbl VALUES (3, 'Charlie')") // v3
  }

  private def withStrictV2[T](body: => T): T = {
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT")(body)
  }

  test("table_changes routes to V2 read-time CDF when catalog returns SparkTable") {
    val tbl = "rt_cdf_tbl"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      withStrictV2 {
        val res = sql(s"SELECT id, name, _change_type FROM table_changes('$tbl', 1, 3)")
          .orderBy("_commit_version", "id")
        checkAnswer(
          res,
          Row(1L, "Alice", "insert") ::
          Row(2L, "Bob", "insert") ::
          Row(3L, "Charlie", "insert") :: Nil)
      }
    }
  }

  test("readChangeFeed via DataFrame.table routes to V2 read-time CDF") {
    val tbl = "rt_cdf_opt_tbl"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      withStrictV2 {
        val res = spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "1")
          .option("endingVersion", "3")
          .table(tbl)
          .select("id", "name", "_change_type")
          .orderBy("_commit_version", "id")
        checkAnswer(
          res,
          Row(1L, "Alice", "insert") ::
          Row(2L, "Bob", "insert") ::
          Row(3L, "Charlie", "insert") :: Nil)
      }
    }
  }

  test("V1 mode keeps legacy CDCReader behavior: no CDF -> CHANGE_DATA_NOT_RECORDED") {
    val tbl = "rt_cdf_v1_tbl"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      // Default V2_ENABLE_MODE is NONE, so catalog returns DeltaTableV2 and the V1 toReadQuery
      // path runs CDCReader, which throws because `delta.enableChangeDataFeed` is unset.
      val ex = intercept[Exception] {
        sql(s"SELECT * FROM table_changes('$tbl', 1, 3)").collect()
      }
      assert(
        ex.getMessage.contains("DELTA_MISSING_CHANGE_DATA") ||
          ex.getMessage.contains("change data was not recorded"),
        s"Expected V1 fall-through error, got: ${ex.getMessage}")
    }
  }

  test("V1 mode keeps legacy CDCReader for tables that DO have CDF enabled") {
    val tbl = "rt_cdf_v1_cdf_on"
    withTable(tbl) {
      sql(
        s"""CREATE TABLE $tbl (id BIGINT) USING delta TBLPROPERTIES (
           |  'delta.enableChangeDataFeed' = 'true'
           |)""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (1), (2)")
      val res = sql(s"SELECT id, _change_type FROM table_changes('$tbl', 1, 1)")
        .orderBy("id")
      checkAnswer(res, Row(1L, "insert") :: Row(2L, "insert") :: Nil)
    }
  }

  private val unsupportedOptionCases: Seq[(String, String)] = Seq(
    "computeUpdates" -> "true",
    "computeUpdates" -> "false",
    "deduplicationMode" -> "dropCarryovers",
    "deduplicationMode" -> "none",
    "deduplicationMode" -> "netChanges",
    "startingBoundInclusive" -> "true",
    "startingBoundInclusive" -> "false",
    "endingBoundInclusive" -> "true",
    "endingBoundInclusive" -> "false")

  for ((key, value) <- unsupportedOptionCases) {
    test(s"$key=$value is rejected by the V2 CDC reader") {
      val tbl = s"rt_cdf_${key}_$value".toLowerCase(java.util.Locale.ROOT)
      withTable(tbl) {
        createRowTrackingTable(tbl)
        withStrictV2 {
          val ex = intercept[Exception] {
            spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .option("endingVersion", "3")
              .option(key, value)
              .table(tbl)
              .collect()
          }
          assert(
            ex.getMessage.contains("DELTA_CHANGELOG_UNSUPPORTED_OPTION") &&
              ex.getMessage.contains(s"`$key`"),
            s"Expected `$key` rejection, got: ${ex.getMessage}")
        }
      }
    }
  }
}
