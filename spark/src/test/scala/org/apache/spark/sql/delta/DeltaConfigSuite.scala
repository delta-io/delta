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

package org.apache.spark.sql.delta

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.DeltaConfigs.{getMilliSeconds, isValidIntervalConfigValue, parseCalendarInterval}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.ManualClock

class DeltaConfigSuite extends SparkFunSuite
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("parseCalendarInterval") {
    for (input <- Seq("5 MINUTES", "5 minutes", "5 Minutes", "inTERval 5 minutes")) {
      assert(parseCalendarInterval(input) ===
        new CalendarInterval(0, 0, TimeUnit.MINUTES.toMicros(5)))
    }

    for (input <- Seq(null, "", " ")) {
      val e = intercept[IllegalArgumentException] {
        parseCalendarInterval(input)
      }
      assert(e.getMessage.contains("cannot be null or blank"))
    }

    for (input <- Seq("interval", "interval1 day", "foo", "foo 1 day")) {
      val e = intercept[IllegalArgumentException] {
        parseCalendarInterval(input)
      }
      assert(e.getMessage.contains("not a valid INTERVAL"))
    }
  }

  test("isValidIntervalConfigValue") {
    for (input <- Seq(
        // Allow 0 microsecond because we always convert microseconds to milliseconds so 0
        // microsecond is the same as 100 microseconds.
        "0 microsecond",
        "1 microsecond",
        "1 millisecond",
        "1 day",
        "-1 day 86400001 milliseconds", // This is 1 millisecond
        "1 day -1 microseconds")) {
      assert(isValidIntervalConfigValue(parseCalendarInterval(input)))
    }
    for (input <- Seq(
        "-1 microseconds",
        "-1 millisecond",
        "-1 day",
        "1 day -86400001 milliseconds", // This is -1 millisecond
        "1 month",
        "1 year")) {
      assert(!isValidIntervalConfigValue(parseCalendarInterval(input)), s"$input")
    }
  }

  test("Optional Calendar Interval config") {
    val clock = new ManualClock(System.currentTimeMillis())

    // case 1: duration not specified
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      val retentionTimestampOpt = DeltaLog.forTable(spark, dir, clock)
        .snapshot.minSetTransactionRetentionTimestamp

      assert(retentionTimestampOpt.isEmpty)
    }

    // case 2: valid duration specified
    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.setTransactionRetentionDuration' = 'interval 1 days')
           |""".stripMargin)

      DeltaLog.clearCache() // we want to ensure we can use the ManualClock we pass in

      val log = DeltaLog.forTable(spark, dir, clock)
      val retentionTimestampOpt = log.snapshot.minSetTransactionRetentionTimestamp
      assert(log.clock.getTimeMillis() == clock.getTimeMillis())
      val expectedRetentionTimestamp =
        clock.getTimeMillis() - getMilliSeconds(parseCalendarInterval("interval 1 days"))

      assert(retentionTimestampOpt.contains(expectedRetentionTimestamp))
    }

    // case 3: invalid duration specified
    withTempDir { dir =>
      val e = intercept[IllegalArgumentException] {
        sql(
          s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.setTransactionRetentionDuration' = 'interval 1 foo')
             |""".stripMargin)
      }
      assert(e.getMessage.contains("not a valid INTERVAL"))
    }
  }

  test("DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES = true") {
    withSQLConf(DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES.key -> "true") {
      // (1) we can set arbitrary table properties
      withTempDir { tempDir =>
        sql(
          s"""CREATE TABLE delta.`${tempDir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.autoOptimize.autoCompact' = true)
             |""".stripMargin)
      }

      // (2) we still validate matching properties
      withTempDir { tempDir =>
        val e = intercept[IllegalArgumentException] {
          sql(
            s"""CREATE TABLE delta.`${tempDir.getCanonicalPath}` (id bigint) USING delta
               |TBLPROPERTIES ('delta.setTransactionRetentionDuration' = 'interval 1 foo')
               |""".stripMargin)
        }
        assert(e.getMessage.contains("not a valid INTERVAL"))
      }
    }
  }

  test("we don't allow arbitrary delta-prefixed table properties") {

    // standard behavior
    withSQLConf(DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES.key -> "false") {
      val e = intercept[AnalysisException] {
        withTempDir { tempDir =>
          sql(
            s"""CREATE TABLE delta.`${tempDir.getCanonicalPath}` (id bigint) USING delta
               |TBLPROPERTIES ('delta.foo' = true)
               |""".stripMargin)
        }
      }
      var msg = "[DELTA_UNKNOWN_CONFIGURATION] " +
        "Unknown configuration was specified: delta.foo\nTo disable this check, set " +
        "spark.databricks.delta.allowArbitraryProperties.enabled=true in the Spark session " +
        "configuration."
      assert(e.getMessage == msg)
    }
  }

  test("allow setting valid and supported isolation level") {
    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
           |""".stripMargin)

      val isolationLevel =
        DeltaLog.forTable(spark, dir.getCanonicalPath).startTransaction().defaultIsolationLevel()

      assert(isolationLevel == Serializable)
    }

    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')
           |""".stripMargin)

      val isolationLevel =
        DeltaLog.forTable(spark, dir.getCanonicalPath).startTransaction().defaultIsolationLevel()

      assert(isolationLevel == WriteSerializable)
    }
  }

  test("do not allow setting valid but unsupported isolation level") {
    withTempDir { dir =>
      val e = intercept[IllegalArgumentException] {
        sql(
          s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.isolationLevel' = 'SnapshotIsolation')
             |""".stripMargin)
      }
      val msg = "requirement failed: delta.isolationLevel must be Serializable or WriteSerializable"
      assert(e.getMessage == msg)
    }
  }

  test("do not allow setting invalid isolation level") {
    withTempDir { dir =>
      val e = intercept[IllegalArgumentException] {
        sql(
          s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.isolationLevel' = 'InvalidSerializable')
             |""".stripMargin)
      }
      val msg = "[DELTA_INVALID_ISOLATION_LEVEL] invalid isolation level 'InvalidSerializable'"
      assert(e.getMessage == msg)
    }
  }

  test("getAllConfigs API") {
    assert(DeltaConfigs.getAllConfigs.contains("minreaderversion"))
    assert(!DeltaConfigs.getAllConfigs.contains("confignotexist"))
  }
}

