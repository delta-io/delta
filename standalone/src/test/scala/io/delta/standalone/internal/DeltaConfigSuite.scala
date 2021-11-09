/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.internal.DeltaConfigs.{isValidIntervalConfigValue, parseCalendarInterval}
import io.delta.standalone.internal.actions.Metadata
import io.delta.standalone.internal.util.{CalendarInterval, DateTimeConstants}

class DeltaConfigSuite extends FunSuite {

  test("mergeGlobalConfigs") {

    val hadoopConf = new Configuration()
    hadoopConf.set(
      DeltaConfigs.hadoopConfPrefix + DeltaConfigs.IS_APPEND_ONLY.key.stripPrefix("delta."),
      "true")
    hadoopConf.set(
      DeltaConfigs.hadoopConfPrefix +
        DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key.stripPrefix("delta."),
      "true")
    val metadataConf = Map(DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key -> "false",
      DeltaConfigs.CHECKPOINT_INTERVAL.key -> "1 day")
    val mergedConf = DeltaConfigs.mergeGlobalConfigs(hadoopConf, metadataConf)
    assert(mergedConf.get(DeltaConfigs.IS_APPEND_ONLY.key) == Some("true"))
    assert(mergedConf.get(DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key) == Some("false"))
    assert(mergedConf.get(DeltaConfigs.CHECKPOINT_INTERVAL.key) == Some("1 day"))
    assert(!mergedConf.contains("delta.deletedFileRetentionDuration")) // we didn't add other keys
  }

  test("check DeltaConfig defaults") {
    val emptyMetadata = new Metadata()
    assert(
      DeltaConfigs.getMilliSeconds(DeltaConfigs.TOMBSTONE_RETENTION.fromMetadata(emptyMetadata)) ==
      DateTimeConstants.MILLIS_PER_DAY*DateTimeConstants.DAYS_PER_WEEK) // default is 1 week

    assert(DeltaConfigs.getMilliSeconds(DeltaConfigs.LOG_RETENTION.fromMetadata(emptyMetadata)) ==
        DateTimeConstants.MILLIS_PER_DAY*30) // default is 30 days

    assert(DeltaConfigs.CHECKPOINT_INTERVAL.fromMetadata(emptyMetadata) == 10)

    assert(DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.fromMetadata(emptyMetadata))

    assert(!DeltaConfigs.IS_APPEND_ONLY.fromMetadata(emptyMetadata))
  }

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
      assert(e.getMessage.contains("Invalid interval"))
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
}
