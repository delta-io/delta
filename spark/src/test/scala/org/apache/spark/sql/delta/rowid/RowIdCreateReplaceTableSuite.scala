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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.RowId.extractHighWatermark

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class RowIdCreateReplaceTableSuite extends QueryTest
  with SharedSparkSession with RowIdTestUtils {

  private val numSourceRows = 50

  test("Create or replace table with values list") {
    withRowTrackingEnabled(enabled = true) {
      withTable("target") {
        writeTargetTestData(withRowIds = true)
        val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))

        val highWaterMarkBefore = extractHighWatermark(snapshot).get
        createReplaceTargetTable(
          commandName = "CREATE OR REPLACE",
          query = "SELECT * FROM VALUES (0, 0), (1, 1)")

        assertHighWatermarkIsCorrectAfterUpdate(
          log, highWaterMarkBefore, expectedNumRecordsWritten = 2)
        assertRowIdsAreLargerThanValue(log, highWaterMarkBefore)
      }
    }
  }

  test("Create or replace table with other delta table") {
    withRowTrackingEnabled(enabled = true) {
      withTable("source", "target") {
        writeTargetTestData(withRowIds = true)

        writeSourceTestData(withRowIds = true)
        val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))

        val highWaterMarkBefore = extractHighWatermark(snapshot).get
        createReplaceTargetTable(commandName = "CREATE OR REPLACE", query = "SELECT * FROM source")

        assertHighWatermarkIsCorrectAfterUpdate(
          log, highWaterMarkBefore, expectedNumRecordsWritten = numSourceRows)
        assertRowIdsAreLargerThanValue(log, highWaterMarkBefore)
      }
    }
  }

  test("Replace table with values list") {
    withRowTrackingEnabled(enabled = true) {
      withTable("target") {
        writeTargetTestData(withRowIds = true)
        val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))

        val highWaterMarkBefore = extractHighWatermark(snapshot).get
        createReplaceTargetTable(commandName = "REPLACE", query = "SELECT * FROM VALUES (0), (1)")

        assertHighWatermarkIsCorrectAfterUpdate(
          log, highWaterMarkBefore, expectedNumRecordsWritten = 2)
        assertRowIdsAreLargerThanValue(log, highWaterMarkBefore)
      }
    }
  }

  test("Replace table with another delta table") {
    withRowTrackingEnabled(enabled = true) {
      withTable("source", "target") {
        writeTargetTestData(withRowIds = true)
        val log = DeltaLog.forTable(spark, TableIdentifier("target"))

        writeSourceTestData(withRowIds = true)

        val highWaterMarkBefore = extractHighWatermark(log.update()).get
        createReplaceTargetTable(commandName = "REPLACE", query = "SELECT * FROM source")

        assertHighWatermarkIsCorrectAfterUpdate(
          log, highWaterMarkBefore, expectedNumRecordsWritten = numSourceRows)
        assertRowIdsAreLargerThanValue(log, highWaterMarkBefore)
      }
    }
  }

  test("Replace table with row IDs with table without row IDs assigns new row IDs") {
    withTable("source", "target") {
      writeTargetTestData(withRowIds = true)
      val log = DeltaLog.forTable(spark, TableIdentifier("target"))

      writeSourceTestData(withRowIds = false)

      val highWaterMarkBefore = extractHighWatermark(log.update()).get
      withRowTrackingEnabled(enabled = false) {
        createReplaceTargetTable(commandName = "REPLACE", query = "SELECT * FROM source")
      }

      assertHighWatermarkIsCorrectAfterUpdate(
        log, highWaterMarkBefore, expectedNumRecordsWritten = numSourceRows)
    }
  }

  def createReplaceTargetTable(
      commandName: String, query: String, tblProperties: Seq[String] = Seq.empty): Unit = {
    val tblPropertiesStr = if (tblProperties.nonEmpty) {
      s"TBLPROPERTIES ${tblProperties.mkString("(", ",", ")")}"
    } else {
      ""
    }
    sql(
      s"""
         |$commandName TABLE target
         |USING delta
         |$tblPropertiesStr
         |AS $query
         |""".stripMargin)
  }

  def writeTargetTestData(withRowIds: Boolean): Unit = {
    withRowTrackingEnabled(enabled = withRowIds) {
      spark.range(start = 0, end = 100, step = 1, numPartitions = 10)
        .write.format("delta").saveAsTable("target")
    }
  }

  def writeSourceTestData(withRowIds: Boolean): Unit = {
    withRowTrackingEnabled(enabled = withRowIds) {
      spark.range(start = 0, end = numSourceRows, step = 1, numPartitions = 10)
        .write.format("delta").saveAsTable("source")
    }
  }
}
