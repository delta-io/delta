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

import java.time.{Duration, Period}

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.streaming.Trigger

class DeltaIntervalTypeSuite extends DeltaSourceSuiteBase with DeltaSQLCommandTest {

  private case class IntervalCase(
      name: String,
      dataType: String,
      value: String,
      expected: Any)

  private val intervalCases = Seq(
    IntervalCase(
      "year to month",
      "INTERVAL YEAR TO MONTH",
      "INTERVAL '10-5' YEAR TO MONTH",
      Period.of(10, 5, 0)),
    IntervalCase(
      "year",
      "INTERVAL YEAR",
      "INTERVAL '11' YEAR",
      Period.of(11, 0, 0)),
    IntervalCase(
      "month",
      "INTERVAL MONTH",
      "INTERVAL '16' MONTH",
      Period.of(1, 4, 0)),
    IntervalCase(
      "day to second",
      "INTERVAL DAY TO SECOND",
      "INTERVAL '2 03:04:05.123456' DAY TO SECOND",
      Duration.ofSeconds(183845, 123456000)),
    IntervalCase(
      "day to hour",
      "INTERVAL DAY TO HOUR",
      "INTERVAL '1 02' DAY TO HOUR",
      Duration.ofHours(26)))

  private def assertPartitionCounts(
      tableName: String,
      totalPartitionKeys: Int,
      yearMonthKeys: Int,
      dayTimeKeys: Int): Unit = {
    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
    val partitionValues = snapshot.allFiles.collect().map(_.partitionValues)
    assert(partitionValues.distinct.length === totalPartitionKeys)
    assert(partitionValues.map(_("ym")).distinct.length === yearMonthKeys)
    assert(partitionValues.map(_("dt")).distinct.length === dayTimeKeys)
  }

  private def assertResultAndNumFilesRead(
      expectedRows: Seq[Row],
      numFilesRead: Int,
      queryText: String): Unit = {
    val query = sql(queryText)
    checkAnswer(query, expectedRows)
    assert(numberOfFilesRead(query) === numFilesRead)
  }

  private def numberOfFilesRead(result: DataFrame): Long = {
    result.queryExecution.executedPlan.collectFirst {
      case scan: FileSourceScanExec => scan.selectedPartitions.totalNumberOfFiles
    }.getOrElse(fail("Delta query did not contain a file scan"))
  }

  intervalCases.foreach { testCase =>
    test(s"SQL create, insert, and read round trip - ${testCase.name}") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        sql(s"CREATE TABLE delta.`$path` (id INT, value ${testCase.dataType}) USING DELTA")
        sql(s"INSERT INTO delta.`$path` VALUES (1, ${testCase.value}), (2, NULL)")

        checkAnswer(
          sql(s"SELECT * FROM delta.`$path`"),
          Seq(Row(1, testCase.expected), Row(2, null)))
      }
    }
  }

  test("DataFrameWriter V2 writes interval columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""CREATE TABLE delta.`$path` (
           |  ym INTERVAL YEAR TO MONTH,
           |  dt INTERVAL DAY TO SECOND)
           |USING DELTA""".stripMargin)

      sql(
        """SELECT
          |  INTERVAL '10-5' YEAR TO MONTH AS ym,
          |  INTERVAL '2 03:04:05.123456' DAY TO SECOND AS dt""".stripMargin)
        .writeTo(s"delta.`$path`")
        .append()

      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        Row(Period.of(10, 5, 0), Duration.ofSeconds(183845, 123456000)))
    }
  }

  test("DataFrameWriter V1 reports its interval column limitation") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"CREATE TABLE delta.`$path` (ym INTERVAL YEAR TO MONTH) USING DELTA")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT INTERVAL '10-5' YEAR TO MONTH AS ym")
            .write
            .mode("append")
            .format("delta")
            .save(path)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`ym`",
          "columnType" -> "\"INTERVAL .*\"",
          "format" -> ".*DeltaDataSource.*"),
        matchPVals = true)
    }
  }

  test("streaming write round trip with interval columns") {
    withTempDirs { (sourceDir, checkpointDir, destinationDir) =>
      val sourcePath = sourceDir.getCanonicalPath
      val destinationPath = destinationDir.getCanonicalPath
      val schema = "ym INTERVAL YEAR TO MONTH, dt INTERVAL DAY TO SECOND"

      sql(s"CREATE TABLE delta.`$sourcePath` ($schema) USING DELTA")
      sql(s"CREATE TABLE delta.`$destinationPath` ($schema) USING DELTA")
      sql(
        s"""INSERT INTO delta.`$sourcePath` VALUES (
           |  INTERVAL '10-5' YEAR TO MONTH,
           |  INTERVAL '2 03:04:05.123456' DAY TO SECOND)""".stripMargin)

      val query = spark.readStream
        .format("delta")
        .load(sourcePath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.AvailableNow())
        .start(destinationPath)
      try {
        assert(query.awaitTermination(10000), "Streaming query did not terminate in 10 seconds")
      } finally {
        if (query.isActive) query.stop()
      }

      checkAnswer(
        sql(s"SELECT * FROM delta.`$destinationPath`"),
        Row(Period.of(10, 5, 0), Duration.ofSeconds(183845, 123456000)))
    }
  }

  test("interval columns use the usual minimum protocol") {
    withTable("interval_table") {
      sql(
        """CREATE TABLE interval_table (
          |  id INT,
          |  ym INTERVAL YEAR TO MONTH,
          |  dt INTERVAL DAY TO SECOND)
          |USING DELTA""".stripMargin)

      assert(getProtocolForTable("interval_table") === Protocol(1, 2))
    }
  }

  test("interval columns support schema evolution and defaults") {
    withTable("interval_table") {
      sql("CREATE TABLE interval_table (id INT) USING DELTA")
      sql("ALTER TABLE interval_table ADD COLUMNS (ym INTERVAL YEAR TO MONTH)")
      sql("ALTER TABLE interval_table ADD COLUMNS (dt INTERVAL DAY TO SECOND)")
      sql(
        """ALTER TABLE interval_table SET TBLPROPERTIES (
          |  'delta.feature.allowColumnDefaults' = 'supported')""".stripMargin)
      sql(
        """ALTER TABLE interval_table ALTER COLUMN dt
          |SET DEFAULT INTERVAL '0 00:00:42' DAY TO SECOND""".stripMargin)
      sql("INSERT INTO interval_table (id, ym) VALUES (1, INTERVAL '10-5' YEAR TO MONTH)")

      checkAnswer(
        sql("SELECT * FROM interval_table"),
        Row(1, Period.of(10, 5, 0), Duration.ofSeconds(42)))
    }
  }

  test("generated interval columns can be partition columns") {
    withTable("interval_table") {
      sql(
        """CREATE TABLE interval_table (
          |  ym_text STRING,
          |  dt_text STRING,
          |  ym INTERVAL YEAR TO MONTH
          |    GENERATED ALWAYS AS (CAST(ym_text AS INTERVAL YEAR TO MONTH)),
          |  dt INTERVAL DAY TO SECOND
          |    GENERATED ALWAYS AS (CAST(dt_text AS INTERVAL DAY TO SECOND)))
          |USING DELTA
          |PARTITIONED BY (ym, dt)""".stripMargin)
      sql(
        """INSERT INTO interval_table (ym_text, dt_text) VALUES
          |  ('2-7', '1 02:03:04'),
          |  ('-5-3', '1 02:03:04'),
          |  ('2-7', '-0 00:00:30'),
          |  ('-5-3', '-0 00:00:30'),
          |  ('1-0', '1 00:01:00'),
          |  ('1-0', '1 00:01:00.0')""".stripMargin)

      assertPartitionCounts(
        "interval_table",
        totalPartitionKeys = 5,
        yearMonthKeys = 3,
        dayTimeKeys = 3)
      checkAnswer(
        sql("SELECT ym, dt FROM interval_table WHERE ym_text = '2-7'"),
        Seq(
          Row(Period.of(2, 7, 0), Duration.ofSeconds(93784)),
          Row(Period.of(2, 7, 0), Duration.ofSeconds(-30))))
    }
  }

  test("interval partition filters prune files") {
    withTable("interval_table") {
      sql(
        """CREATE TABLE interval_table (
          |  id INT,
          |  ym INTERVAL YEAR TO MONTH,
          |  dt INTERVAL DAY TO SECOND)
          |USING DELTA
          |PARTITIONED BY (ym, dt)""".stripMargin)
      sql(
        """INSERT INTO interval_table VALUES
          |  (1, INTERVAL '10-5' YEAR TO MONTH, INTERVAL '2 03:04:05' DAY TO SECOND),
          |  (2, INTERVAL '10-0' YEAR TO MONTH, INTERVAL '2 03:04:05' DAY TO SECOND),
          |  (3, INTERVAL '10-5' YEAR TO MONTH, INTERVAL '5.55' SECOND),
          |  (4, INTERVAL '10-0' YEAR TO MONTH, INTERVAL '5.55' SECOND),
          |  (5, INTERVAL '10-0' YEAR TO MONTH, INTERVAL '5.55' SECOND)""".stripMargin)

      assertPartitionCounts(
        "interval_table",
        totalPartitionKeys = 4,
        yearMonthKeys = 2,
        dayTimeKeys = 2)
      assertResultAndNumFilesRead(
        Seq(
          Row(4, Period.of(10, 0, 0), Duration.ofSeconds(5, 550000000)),
          Row(5, Period.of(10, 0, 0), Duration.ofSeconds(5, 550000000))),
        numFilesRead = 1,
        """SELECT * FROM interval_table
          |WHERE ym = INTERVAL '10-0' YEAR TO MONTH
          |  AND dt = INTERVAL '5.55' SECOND""".stripMargin)
    }
  }
}
