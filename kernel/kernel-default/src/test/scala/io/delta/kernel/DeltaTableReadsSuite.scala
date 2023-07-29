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

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.scalatest.funsuite.AnyFunSuite

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////

  // TODO: for now we do not support timestamp partition columns, make sure it's blocked
  test("cannot read partition column of timestamp type") {
    // kernel expects a fully qualified path
    val path = "file:" + goldenTablePath("kernel-timestamp-TIMESTAMP_MICROS")
    val snapshot = latestSnapshot(path)

    val e = intercept[UnsupportedOperationException] {
      readSnapshot(snapshot) // request entire schema
    }
    assert(e.getMessage.contains("Reading partition columns of TimestampType is unsupported"))
  }

  // Below table is written in either UTC or PDT for the golden tables
  /*
  id: int  | Part (TZ agnostic): timestamp     | time : timestamp
  ------------------------------------------------------------------------
  0        | 2020-01-01 08:09:10.001           | 2020-02-01 08:09:10
  1        | 2021-10-01 08:09:20               | 1999-01-01 09:00:00
  2        | 2021-10-01 08:09:20               | 2000-01-01 09:00:00
  */

  def row0: (Int, Long) = (
    0,
    // 2020-02-01 08:09:10 UTC to micros since the epoch
    1580544550000000L)

  def row1: (Int, Long) = (
    1,
    // 1999-01-01 09:00:00 UTC to micros since the epoch
    915181200000000L
  )

  def row2: (Int, Long) = (
    2,
    // 2000-01-01 09:00:00 UTC to micros since the epoch
    946717200000000L
  )

  def utcTableExpectedResult: Set[(Int, Long)] = Set(row0, row1, row2)

  for (timestampType <- Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
    for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
      test(
        s"end-to-end usage: timestamp table parquet timestamp format $timestampType tz $timeZone") {
        withTimeZone(timeZone) {
          // kernel expects a fully qualified path
          val path = "file:" + goldenTablePath("kernel-timestamp-" + timestampType)
          val snapshot = latestSnapshot(path)

          // for now omit "part" column since we don't support reading timestamp partition values
          val readSchema = snapshot.getSchema(defaultTableClient)
            .withoutField("part")

          val result = readSnapshot(snapshot, readSchema).map { row =>
            (row.getInt(0), row.getLong(1))
          }

          assert(result.toSet == utcTableExpectedResult)
        }
      }
    }
  }

  // PST table - all the "time" col timestamps are + 8 hours
  def pstTableExpectedResult: Set[(Int, Long)] = utcTableExpectedResult.map {
    case (id, col) =>
      (id, col + DefaultKernelUtils.DateTimeConstants.MICROS_PER_HOUR * 8)
  }

  for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
    test(s"end-to-end usage: timestamp in written in PST read in $timeZone") {
      withTimeZone(timeZone) {
        // kernel expects a fully qualified path
        val path = "file:" + goldenTablePath("kernel-timestamp-PST")
        val snapshot = latestSnapshot(path)

        // for now omit "part" column since we don't support reading timestamp partition values
        val readSchema = snapshot.getSchema(defaultTableClient)
          .withoutField("part")

        val result = readSnapshot(snapshot, readSchema).map { row =>
          (row.getInt(0), row.getLong(1))
        }

        assert(result.toSet == pstTableExpectedResult)
      }
    }
  }
}
