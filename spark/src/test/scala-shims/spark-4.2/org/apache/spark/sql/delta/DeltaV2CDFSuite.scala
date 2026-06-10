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
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
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

  test("table_changes routes to V2 read-time CDF when catalog returns DeltaV2Table") {
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

  test("readChangeFeed via DataFrame.table() routes to V2 read-time CDF") {
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
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"SELECT * FROM table_changes('$tbl', 1, 3)").collect()
        },
        "DELTA_MISSING_CHANGE_DATA",
        parameters = Map(
          "startVersion" -> "1",
          "endVersion" -> "3",
          "version" -> "1",
          "key" -> DeltaConfigs.CHANGE_DATA_FEED.key))
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
          checkError(
            intercept[DeltaIllegalArgumentException] {
              spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", "1")
                .option("endingVersion", "3")
                .option(key, value)
                .table(tbl)
                .collect()
            },
            "DELTA_CHANGELOG_UNSUPPORTED_OPTION",
            sqlState = "0AKDE",
            parameters = Map("option" -> key))
        }
      }
    }
  }

  // TODO: Re-enable when Spark 4.2 is out. 4.2.0-preview5 doesn't yet contain post-processing
  // for CDC in Spark that labels update pre/post images.
  ignore("UPDATE: preimage/postimage pair emitted") {
    val tbl = "rt_cdf_update"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      sql(s"UPDATE $tbl SET name = 'Robert' WHERE id = 2") // v4
      withStrictV2 {
        val res = sql(
          s"SELECT id, name, _change_type FROM table_changes('$tbl', 4, 4)")
          .orderBy("id", "_change_type")
        checkAnswer(
          res,
          Row(2L, "Bob", "update_preimage") ::
          Row(2L, "Robert", "update_postimage") :: Nil)
      }
    }
  }

  test("DELETE: surfaces as a `delete` row") {
    val tbl = "rt_cdf_delete"
    withTable(tbl) {
      createRowTrackingTable(tbl) // v3
      sql(s"DELETE FROM $tbl WHERE id = 2") // v4
      withStrictV2 {
        val res = sql(
          s"SELECT id, name, _change_type FROM table_changes('$tbl', 4, 4)")
          .orderBy("id")
        checkAnswer(res, Row(2L, "Bob", "delete") :: Nil)
      }
    }
  }

  test("predicate filters drop non-matching change rows") {
    val tbl = "rt_cdf_predicate"
    withTable(tbl) {
      createRowTrackingTable(tbl)           // v1-v3: insert id 1, 2, 3 (one file each)
      sql(s"DELETE FROM $tbl WHERE id = 2") // v4: delete file 2
      withStrictV2 {
        checkAnswer(
          sql(s"SELECT id, name, _change_type FROM table_changes('$tbl', 1, 4) WHERE id = 2"),
          Row(2L, "Bob", "insert") ::
          Row(2L, "Bob", "delete") :: Nil)
        checkAnswer(
          sql(s"SELECT id, _change_type FROM table_changes('$tbl', 1, 4) " +
            "WHERE _change_type = 'delete'"),
          Row(2L, "delete") :: Nil)
      }
    }
  }

  // TODO: Re-enable when Spark 4.2 is out. 4.2.0-preview5 doesn't yet contain post-processing
  // for CDC in Spark that labels update pre/post images.
  ignore("MERGE: insert/update/delete in one commit each surface with the right change_type") {
    val src = "rt_cdf_merge_src"
    val tgt = "rt_cdf_merge_tgt"
    withTable(src, tgt) {
      createRowTrackingTable(tgt)
      sql(s"CREATE TABLE $src (id BIGINT, name STRING) USING delta")
      sql(s"INSERT INTO $src VALUES (2, 'Robert'), (3, NULL), (4, 'Dave')")

      sql(
        s"""MERGE INTO $tgt t USING $src s ON t.id = s.id
           |WHEN MATCHED AND s.name IS NULL THEN DELETE
           |WHEN MATCHED THEN UPDATE SET t.name = s.name
           |WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)""".stripMargin) // v4

      withStrictV2 {
        val res = sql(
          s"SELECT id, name, _change_type FROM table_changes('$tgt', 4, 4)")
          .orderBy("id", "_change_type")
        checkAnswer(
          res,
          Row(2L, "Bob", "update_preimage") ::
          Row(2L, "Robert", "update_postimage") ::
          Row(3L, "Charlie", "delete") ::
          Row(4L, "Dave", "insert") :: Nil)
      }
    }
  }

  // TODO: Re-enable when Spark 4.2 is out. 4.2.0-preview5 doesn't yet contain post-processing
  // for CDC in Spark that drops carry-over rows / labels update pre/post images, both of which
  // a multi-row base file rewritten by repeated DMLs relies on.
  ignore("repeated DMLs touching the same base file diff to per-row changes") {
    val tbl = "rt_cdf_repeated_dml"
    withTable(tbl) {
      sql(
        s"""CREATE TABLE $tbl (id BIGINT, name STRING) USING delta TBLPROPERTIES (
           |  'delta.enableChangeDataFeed' = 'false',
           |  'delta.enableRowTracking' = 'true',
           |  'delta.enableDeletionVectors' = 'false'
           |)""".stripMargin)

      sql(s"INSERT INTO $tbl VALUES (1, 'A'), (2, 'B'), (3, 'C')") // v1 writes a single file
      sql(s"UPDATE $tbl SET name = 'B2' WHERE id = 2")             // v2 rewrites the file
      sql(s"UPDATE $tbl SET name = 'C2' WHERE id = 3")             // v3 rewrites the file from v2
      withStrictV2 {
        val res = sql(s"SELECT id, name, _change_type FROM table_changes('$tbl', 2, 3)")
        checkAnswer(
          res,
          Row(2L, "B", "update_preimage") ::
          Row(2L, "B2", "update_postimage") ::
          Row(3L, "C", "update_preimage") ::
          Row(3L, "C2", "update_postimage") :: Nil)
      }
    }
  }

  // TODO: Re-enable once the V2 read-time CDF path applies deletion vectors. Today
  // DeltaChangelogBatch reads whole data files and ignores DVs, and DeltaChangelog always
  // reports containsCarryoverRows=true, so a DV-based DELETE (which references the same file
  // with a DV rather than rewriting it) cancels out as a carry-over and the deleted row is
  // never surfaced. Labeling also depends on the post-processing missing from 4.2.0-preview5.
  ignore("deletion vectors: a DV-based DELETE surfaces the deleted row") {
    val tbl = "rt_cdf_dv_delete"
    withTable(tbl) {
      sql(
        s"""CREATE TABLE $tbl (id BIGINT, name STRING) USING delta TBLPROPERTIES (
           |  'delta.enableChangeDataFeed' = 'false',
           |  'delta.enableRowTracking' = 'true',
           |  'delta.enableDeletionVectors' = 'true'
           |)""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (1, 'A'), (2, 'B'), (3, 'C')") // v1: one file
      sql(s"DELETE FROM $tbl WHERE id = 2")                        // v2: writes a DV, keeps file
      withStrictV2 {
        val res = sql(s"SELECT id, name, _change_type FROM table_changes('$tbl', 2, 2)")
          .orderBy("id")
        checkAnswer(res, Row(2L, "B", "delete") :: Nil)
      }
    }
  }

  // TODO: Re-enable when Spark 4.2 is out. 4.2.0-preview5 doesn't yet contain post-processing
  // for CDC in Spark that labels update pre/post images.
  ignore("V2 mode with delta.enableChangeDataFeed=true also routes through the V2 reader") {
    val tbl = "rt_cdf_v2_cdf_on"
    withTable(tbl) {
      sql(
        s"""CREATE TABLE $tbl (id BIGINT, name STRING) USING delta TBLPROPERTIES (
           |  'delta.enableChangeDataFeed' = 'true',
           |  'delta.enableRowTracking' = 'true',
           |  'delta.enableDeletionVectors' = 'false'
           |)""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (1, 'A'), (2, 'B')") // v1
      sql(s"UPDATE $tbl SET name = 'b' WHERE id = 2")    // v2
      withStrictV2 {
        val res = sql(s"SELECT id, name, _change_type FROM table_changes('$tbl', 2, 2)")
          .orderBy("id", "_change_type")
        checkAnswer(
          res,
          Row(2L, "B", "update_preimage") ::
          Row(2L, "b", "update_postimage") :: Nil)
      }
    }
  }

  test("table_changes_by_path stays on the V1 path even in V2 mode") {
    // Path-based reads resolve through `UnresolvedPathBasedDeltaTableRelation` ->
    // `getPathBasedDeltaTable`, which always returns a V1 `DeltaTableV2`.
    // This test may need to be updated if/when we support DSv2 for path based access.
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(2).toDF("id").write.format("delta")
        .option("delta.enableRowTracking", "true")
        .save(path)
      withStrictV2 {
        checkError(
          intercept[DeltaAnalysisException] {
            sql(s"SELECT * FROM table_changes_by_path('$path', 0, 0)").collect()
          },
          "DELTA_MISSING_CHANGE_DATA",
          parameters = Map(
            "startVersion" -> "0",
            "endVersion" -> "0",
            "version" -> "0",
            "key" -> DeltaConfigs.CHANGE_DATA_FEED.key))
      }
    }
  }

  test("DELTA_CHANGELOG_V2_ENABLED=false leaves V2-resolved CDC plans unresolved") {
    val tbl = "rt_cdf_changelog_v2_off"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      withStrictV2 {
        withSQLConf(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED.key -> "false") {
          intercept[Exception] {
            sql(s"SELECT * FROM table_changes('$tbl', 1, 3)").collect()
          }
        }
      }
    }
  }

  test("end < start range fails with a clear error") {
    val tbl = "rt_cdf_empty_range"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      withStrictV2 {
        checkError(
          intercept[DeltaIllegalArgumentException] {
            sql(s"SELECT * FROM table_changes('$tbl', 3, 1)").collect()
          },
          "DELTA_INVALID_CDC_RANGE",
          parameters = Map("start" -> "3", "end" -> "1"))
      }
    }
  }

  test("start version after the latest commit fails with a clear error") {
    val tbl = "rt_cdf_start_after_latest"
    withTable(tbl) {
      createRowTrackingTable(tbl) // latest is v3
      withStrictV2 {
        checkError(
          intercept[DeltaIllegalArgumentException] {
            sql(s"SELECT * FROM table_changes('$tbl', 99, 99)").collect()
          },
          "DELTA_CDC_START_VERSION_AFTER_LATEST",
          parameters = Map("start" -> "99", "latest" -> "3"))
      }
    }
  }

  test("timestamp-range CDC read selects the right commits") {
    val tbl = "rt_cdf_ts_range"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      val tsV2 = sql(s"DESCRIBE HISTORY $tbl")
        .where("version = 2")
        .select("timestamp")
        .collect()
        .head
        .getTimestamp(0)
        .toString
      withStrictV2 {
        val res = spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingTimestamp", tsV2)
          .option("endingTimestamp", tsV2)
          .table(tbl)
          .select("id", "name", "_change_type")
        checkAnswer(res, Row(2L, "Bob", "insert") :: Nil)
      }
    }
  }

  test("V2 CDC read on a table without row tracking fails with a clear error") {
    val tbl = "rt_cdf_no_row_tracking"
    withTable(tbl) {
      sql(
        s"""CREATE TABLE $tbl (id BIGINT) USING delta TBLPROPERTIES (
           |  'delta.enableChangeDataFeed' = 'false',
           |  'delta.enableRowTracking' = 'false'
           |)""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (1), (2)")
      withStrictV2 {
        checkError(
          intercept[DeltaAnalysisException] {
            sql(s"SELECT * FROM table_changes('$tbl', 1, 1)").collect()
          },
          "DELTA_CHANGELOG_REQUIRES_ROW_TRACKING",
          parameters = Map("tableName" -> "spark_catalog.default.rt_cdf_no_row_tracking"))
      }
    }
  }

  test("time travel option combined with readChangeFeed is rejected") {
    val tbl = "rt_cdf_time_travel"
    withTable(tbl) {
      createRowTrackingTable(tbl)
      withStrictV2 {
        // In V2 mode, the catalog returns DeltaV2Table from `loadTable(ident)`. The
        // time-travel overload `loadTable(ident, version)` falls through to the parent
        // `TableCatalog` default after `AbstractDeltaCatalog.loadTableWithTimeTravel` sees the
        // non-DeltaTableV2 case, which throws Spark's `UNSUPPORTED_FEATURE.TIME_TRAVEL`. This
        // also blocks `readChangeFeed` + `versionAsOf` -- the combination never reaches our
        // V2 CDC rule.
        checkError(
          intercept[AnalysisException] {
            spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .option("endingVersion", "3")
              .option("versionAsOf", "2")
              .table(tbl)
              .collect()
          },
          "UNSUPPORTED_FEATURE.TIME_TRAVEL",
          parameters = Map("relationId" -> s"`spark_catalog`.`default`.`$tbl`"))
      }
    }
  }
}
