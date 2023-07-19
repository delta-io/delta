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
package io.delta.kernel

import java.sql.Timestamp
import java.util.TimeZone

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  def withTimeZone(zoneId: String)(f: => Unit): Unit = {
    val currentDefault = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId))
      f
    } finally {
      TimeZone.setDefault(currentDefault)
    }
  }

  test("test micros since epoch calculation") {
    withTimeZone("America/Los_Angeles") {
      // This is used for turning partition timestamps (timezone-less) to micros since the epoch
      // w.r.t. the system time
      // TODO: this seems a little confusing but is the best way to expose to the connector w/out
      //  storing partition timestamps using a different underlying type. confirm this?
      val time1 = Timestamp.valueOf("2023-07-18 15:38:01")
      val time2 = Timestamp.valueOf("2023-01-18 15:38:01")
      val time1Millis = 1689719881000L
      val time2Millis = 1674085081000L

      assert(DefaultKernelUtils.microsSinceEpoch(time1) == time1Millis*1000)
      assert(DefaultKernelUtils.microsSinceEpoch(time2) == time2Millis*1000)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Tests for the golden tables
  //////////////////////////////////////////////////////////////////////////////////

  // Below table is written in either UTC or PST
  /*
  Id: int  | Part (TZ agnostic): timestamp     | time : timestamp
  ------------------------------------------------------------------------
  0        | 2020-01-01 08:09:10.001           | 2020-02-01 08:09:10
  1        | 2021-10-01 08:09:20               | 1999-01-01 09:00:00
  2        | 2021-10-01 08:09:20               | 2000-01-01 09:00:00
  */

  def row0: (Int, Long, Long) = (
    0,
    // micros since epoch of 2020-01-01 08:09:10.001 in the current TZ (varies based on system tz)
    DefaultKernelUtils.microsSinceEpoch(Timestamp.valueOf("2020-01-01 08:09:10.001")),
    // 2020-02-01 08:09:10 UTC to micros since the epoch
    1580544550000000L)

  def row1: (Int, Long, Long) = (
    1,
    // micros since epoch of 2021-10-01 08:09:20  in the current TZ (varies based on system tz)
    DefaultKernelUtils.microsSinceEpoch(Timestamp.valueOf("2021-10-01 08:09:20 ")),
    // 1999-01-01 09:00:00 UTC to micros since the epoch
    915181200000000L
  )

  def row2: (Int, Long, Long) = (
    2,
    // micros since epoch of 2021-10-01 08:09:20  in the current TZ (varies based on system tz)
    DefaultKernelUtils.microsSinceEpoch(Timestamp.valueOf("2021-10-01 08:09:20 ")),
    // 2000-01-01 09:00:00 UTC to micros since the epoch
    946717200000000L
  )

  def utcTableExpectedResult: Set[(Int, Long, Long)] = Set(row0, row1, row2)

  for (timestampType <- Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
    for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
      test(
        s"end-to-end usage: timestamp table parquet timestamp format $timestampType tz $timeZone") {
        withTimeZone(timeZone) {
          // kernel expects a fully qualified path
          val path = "file:" + goldenTablePath("kernel-timestamp-" + timestampType)
          val result = readTable(path, new Configuration()) { row =>
            (row.getInt(0), row.getLong(1), row.getLong(2))
          }
          assert(result.toSet == utcTableExpectedResult)
        }
      }
    }
  }

  // PST table - all the "time" col timestamps are + 8 hours
  def pstTableExpectedResult: Set[(Int, Long, Long)] = utcTableExpectedResult.map {
    case (id, part, col) =>
      (id, part, col + DefaultKernelUtils.DateTimeConstants.MICROS_PER_HOUR * 8)
  }

  for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
    test(s"end-to-end usage: timestamp in written in PST read in $timeZone") {
      withTimeZone(timeZone) {
        // kernel expects a fully qualified path
        val path = "file:" + goldenTablePath("kernel-timestamp-PST")
        val result = readTable(path, new Configuration()) { row =>
          (row.getInt(0), row.getLong(1), row.getLong(2))
        }
        assert(result.toSet == pstTableExpectedResult)
      }
    }
  }
}
