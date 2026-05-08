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

import java.util.Locale

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class DeltaTruncateTableSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private def deltaLogForTable(tableName: String): DeltaLog =
    DeltaLog.forTable(spark, TableIdentifier(tableName))

  private def activeFileCount(log: DeltaLog): Long = log.update().allFiles.count()

  private def latestHistory(tableName: String): Row =
    sql(s"DESCRIBE HISTORY $tableName LIMIT 1").head()

  test("truncate non-empty delta table removes all active files and records TRUNCATE") {
    val tableName = "truncate_non_empty"
    withTable(tableName) {
      spark.range(start = 0, end = 10, step = 1, numPartitions = 4)
        .write.format("delta").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      val numFilesBeforeTruncate = activeFileCount(log)
      assert(numFilesBeforeTruncate > 1)

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(activeFileCount(log) === 0)

      val history = latestHistory(tableName)
      assert(history.getAs[String]("operation") === "TRUNCATE")
    }
  }

  test("truncate empty delta table is a no-op") {
    val tableName = "truncate_empty"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG) USING delta")

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === 0)
    }
  }

  test("truncate append-only table fails atomically") {
    val tableName = "truncate_append_only"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (id LONG)
           |USING delta
           |TBLPROPERTIES ('delta.appendOnly' = 'true')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1), (2)")

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version
      val numFilesBeforeTruncate = activeFileCount(log)

      val e = intercept[DeltaUnsupportedOperationException] {
        sql(s"TRUNCATE TABLE $tableName")
      }
      checkError(
        e,
        "DELTA_CANNOT_MODIFY_APPEND_ONLY",
        parameters = Map("table_name" -> "null", "config" -> DeltaConfigs.IS_APPEND_ONLY.key))

      checkAnswer(sql(s"SELECT id FROM $tableName ORDER BY id"), Seq(Row(1), Row(2)))
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === numFilesBeforeTruncate)
    }
  }

  test("truncate path-based delta table works without catalog metadata") {
    withTempPath { path =>
      val tablePath = path.getCanonicalPath
      spark.range(start = 0, end = 5, step = 1, numPartitions = 2)
        .write.format("delta").save(tablePath)

      val log = DeltaLog.forTable(spark, tablePath)
      assert(activeFileCount(log) > 0)

      sql(s"TRUNCATE TABLE delta.`$tablePath`")

      checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Nil)
      assert(activeFileCount(log) === 0)

      val history = io.delta.tables.DeltaTable.forPath(spark, tablePath)
        .history(1)
        .select("operation")
        .head()
      assert(history.getString(0) === "TRUNCATE")
    }
  }

  test("truncate partitioned table removes every partition and preserves metadata") {
    val tableName = "truncate_partitioned"
    withTable(tableName) {
      spark.range(start = 0, end = 8, step = 1, numPartitions = 4)
        .selectExpr("id", "id % 2 AS part")
        .write.format("delta").partitionBy("part").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      assert(activeFileCount(log) > 0)

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(activeFileCount(log) === 0)
      assert(log.update().metadata.partitionColumns === Seq("part"))
    }
  }

  test("truncate with partition spec is rejected and leaves table unchanged") {
    val tableName = "truncate_partition_spec"
    withTable(tableName) {
      spark.range(start = 0, end = 4, step = 1, numPartitions = 2)
        .selectExpr("id", "id % 2 AS part")
        .write.format("delta").partitionBy("part").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version
      val numFilesBeforeTruncate = activeFileCount(log)

      val e = intercept[Exception] {
        sql(s"TRUNCATE TABLE $tableName PARTITION (part = 1)")
      }
      assert(
        Option(e.getMessage).exists { message =>
          val lowerCaseMessage = message.toLowerCase(Locale.ROOT)
          message.contains("DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED") ||
            lowerCaseMessage.contains("truncate") && lowerCaseMessage.contains("partition")
        },
        s"Expected partition truncate rejection, got: ${e.getMessage}")

      checkAnswer(
        sql(s"SELECT id, part FROM $tableName ORDER BY id"),
        Seq(Row(0, 0), Row(1, 1), Row(2, 0), Row(3, 1)))
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === numFilesBeforeTruncate)
    }
  }
}
