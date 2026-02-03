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

  test("auto-normalize table properties without delta prefix") {
    // Test 1: logRetentionDuration auto-normalization
    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('logRetentionDuration' = 'interval 7 days')
           |""".stripMargin)

      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      // Should be normalized to delta.logRetentionDuration
      assert(config.contains("delta.logRetentionDuration"),
        s"Expected delta.logRetentionDuration but got: $config")
      assert(!config.contains("logRetentionDuration"),
        "Original key should not be present")
    }

    // Test 2: Non-delta property should pass through unchanged
    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('myCustomProp' = 'myValue')
           |""".stripMargin)

      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      assert(config.contains("myCustomProp"),
        "Custom property should remain unchanged")
    }
  }

  test("auto-normalize table properties with ALTER TABLE") {
    // Test 1: ALTER TABLE SET with non-prefixed logRetentionDuration
    withTempDir { dir =>
      // Create table without any special properties
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      // Use ALTER TABLE to add property without delta. prefix
      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}`
           |SET TBLPROPERTIES ('logRetentionDuration' = 'interval 10 days')
           |""".stripMargin)

      DeltaLog.clearCache()
      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      // Should be normalized to delta.logRetentionDuration
      assert(config.contains("delta.logRetentionDuration"),
        s"Expected delta.logRetentionDuration but got: $config")
      assert(config("delta.logRetentionDuration") == "interval 10 days",
        "Value should match what was set")
      assert(!config.contains("logRetentionDuration"),
        "Original non-prefixed key should not be present")
    }

    // Test 2: ALTER TABLE SET with non-prefixed deletedFileRetentionDuration
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}`
           |SET TBLPROPERTIES ('deletedFileRetentionDuration' = 'interval 14 days')
           |""".stripMargin)

      DeltaLog.clearCache()
      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      assert(config.contains("delta.deletedFileRetentionDuration"),
        s"Expected delta.deletedFileRetentionDuration but got: $config")
    }

    // Test 3: ALTER TABLE with custom (non-delta) property should pass through
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}`
           |SET TBLPROPERTIES ('myCustomProperty' = 'customValue')
           |""".stripMargin)

      DeltaLog.clearCache()
      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      assert(config.contains("myCustomProperty"),
        "Custom property should remain unchanged")
      assert(config("myCustomProperty") == "customValue")
    }

    // Test 4: ALTER TABLE to set property on table created without any properties
    withTempDir { dir =>
      // Create table without any table properties
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      // Verify no logRetentionDuration is set initially
      val initialLog = DeltaLog.forTable(spark, dir)
      assert(!initialLog.snapshot.metadata.configuration.contains("delta.logRetentionDuration"),
        "Property should not exist initially")

      // Set property using non-prefixed version via ALTER TABLE
      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}`
           |SET TBLPROPERTIES ('logRetentionDuration' = 'interval 20 days')
           |""".stripMargin)

      DeltaLog.clearCache()
      val log = DeltaLog.forTable(spark, dir)
      val config = log.snapshot.metadata.configuration

      assert(config.contains("delta.logRetentionDuration"),
        "Property should be normalized to delta.logRetentionDuration")
      assert(config("delta.logRetentionDuration") == "interval 20 days",
        "Value should be set to 20 days")
      assert(!config.contains("logRetentionDuration"),
        "Non-prefixed key should not be present")
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
      checkError(e, "DELTA_UNKNOWN_CONFIGURATION", "F0000", Map(
        "config" -> "delta.foo",
        "disableCheckConfig" -> DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES.key))
    }
  }

  test("allow setting valid and supported isolation level") {
    // currently only Serializable isolation level is supported
    withTempDir { dir =>
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
           |""".stripMargin)

      val isolationLevel =
        DeltaLog.forTable(spark, dir.getCanonicalPath).startTransaction().getDefaultIsolationLevel()

      assert(isolationLevel == Serializable)
    }
  }

  test("do not allow setting valid but unsupported isolation level") {
    withTempDir { dir =>
      val e = intercept[IllegalArgumentException] {
        sql(
          s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')
             |""".stripMargin)
      }
      val msg = "requirement failed: delta.isolationLevel must be Serializable"
      assert(e.getMessage == msg)
    }
  }

  test("do not allow setting invalid isolation level") {
    withTempDir { dir =>
      val e = intercept[DeltaIllegalArgumentException] {
        sql(
          s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES ('delta.isolationLevel' = 'InvalidSerializable')
             |""".stripMargin)
      }
      checkError(e, "DELTA_INVALID_ISOLATION_LEVEL", "25000",
        Map("isolationLevel" -> "InvalidSerializable"))
    }
  }

  test("getAllConfigs API") {
    assert(DeltaConfigs.getAllConfigs.contains("minreaderversion"))
    assert(!DeltaConfigs.getAllConfigs.contains("confignotexist"))
  }
}

