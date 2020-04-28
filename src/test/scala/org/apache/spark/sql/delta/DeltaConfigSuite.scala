/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.DeltaConfigs.{isValidIntervalConfigValue, parseCalendarInterval}

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.CalendarInterval

class DeltaConfigSuite extends SparkFunSuite {

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
