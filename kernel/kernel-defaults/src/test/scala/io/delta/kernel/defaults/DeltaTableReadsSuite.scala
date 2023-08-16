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
package io.delta.kernel.defaults

import java.math.BigDecimal

import org.scalatest.funsuite.AnyFunSuite

import io.delta.golden.GoldenTableUtils.goldenTablePath

import io.delta.kernel.Table

import io.delta.kernel.defaults.internal.DefaultKernelUtils

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////

  // TODO: for now we do not support timestamp partition columns, make sure it's blocked
  test("cannot read partition column of timestamp type") {
    // kernel expects a fully qualified path
    val path = "file:" + goldenTablePath("kernel-timestamp-TIMESTAMP_MICROS")
    val snapshot = Table.forPath(path).getLatestSnapshot(defaultTableClient)

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
  3        | 1969-01-01 00:00:00               | 1969-01-01 00:00:00
  4        | null                              | null
  */

  def row0: (Int, Option[Long]) = (
    0,
    Some(1580544550000000L) // 2020-02-01 08:09:10 UTC to micros since the epoch
  )

  def row1: (Int, Option[Long]) = (
    1,
    Some(915181200000000L) // 1999-01-01 09:00:00 UTC to micros since the epoch
  )

  def row2: (Int, Option[Long]) = (
    2,
    Some(946717200000000L) // 2000-01-01 09:00:00 UTC to micros since the epoch
  )

  def row3: (Int, Option[Long]) = (
    3,
    Some(-31536000000000L) // 1969-01-01 00:00:00 UTC to micros since the epoch
  )

  def row4: (Int, Option[Long]) = (
    4,
    None
  )

  // TODO: refactor this once testing utilities have support for Rows/ColumnarBatches
  def utcTableExpectedResult: Set[(Int, Option[Long])] = Set(row0, row1, row2, row3, row4)

  def testTimestampTable(
    goldenTableName: String,
    timeZone: String,
    expectedResult: Set[(Int, Option[Long])]): Unit = {
    withTimeZone(timeZone) {
      // kernel expects a fully qualified path
      val path = "file:" + goldenTablePath(goldenTableName)
      val snapshot = Table.forPath(path).getLatestSnapshot(defaultTableClient)

      // for now omit "part" column since we don't support reading timestamp partition values
      val readSchema = snapshot.getSchema(defaultTableClient)
        .withoutField("part")

      val result = readSnapshot(snapshot, readSchema).map { row =>
        (row.getInt(0), if (row.isNullAt(1)) Option.empty[Long] else Some(row.getLong(1)))
      }

      assert(result.toSet == expectedResult)
    }
  }

  for (timestampType <- Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
    for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
      test(
        s"end-to-end usage: timestamp table parquet timestamp format $timestampType tz $timeZone") {
        testTimestampTable("kernel-timestamp-" + timestampType, timeZone, utcTableExpectedResult)
      }
    }
  }

  // PST table - all the "time" col timestamps are + 8 hours
  def pstTableExpectedResult: Set[(Int, Option[Long])] = utcTableExpectedResult.map {
    case (id, col) =>
      (id, col.map(_ + DefaultKernelUtils.DateTimeConstants.MICROS_PER_HOUR * 8))
  }

  for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
    test(s"end-to-end usage: timestamp in written in PST read in $timeZone") {
      testTimestampTable("kernel-timestamp-PST", timeZone, pstTableExpectedResult)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Decimal type tests
  //////////////////////////////////////////////////////////////////////////////////

  for (tablePath <- Seq("basic-decimal-table", "basic-decimal-table-legacy")) {
    test(s"end to end: reading $tablePath") {
      val expectedResult = Seq(
        ("234.00000", "1.00", "2.00000", "3.0000000000"),
        ("2342222.23454", "111.11", "22222.22222", "3333333333.3333333333"),
        ("0.00004", "0.00", "0.00000", "0E-10"),
        ("-2342342.23423", "-999.99", "-99999.99999", "-9999999999.9999999999")
      ).map { tup =>
        (new BigDecimal(tup._1), new BigDecimal(tup._2), new BigDecimal(tup._3),
          new BigDecimal(tup._4))
      }.toSet

      // kernel expects a fully qualified path
      val path = "file:" + goldenTablePath(tablePath)
      val snapshot = Table.forPath(path).getLatestSnapshot(defaultTableClient)

      val result = readSnapshot(snapshot).map { row =>
        (row.getDecimal(0), row.getDecimal(1), row.getDecimal(2), row.getDecimal(3))
      }

      assert(expectedResult == result.toSet)
    }
  }
}
