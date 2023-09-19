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

import java.io.File
import java.math.BigDecimal
import java.sql.Date

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.{Table, TableNotFoundException}
import io.delta.kernel.defaults.internal.DefaultKernelUtils
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import io.delta.kernel.internal.util.InternalUtils.daysSinceEpoch

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////

  // TODO: for now we do not support timestamp partition columns, make sure it's blocked
  test("cannot read partition column of timestamp type") {
    val path = goldenTablePath("kernel-timestamp-TIMESTAMP_MICROS")
    val snapshot = latestSnapshot(path);

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

  def row0: TestRow = TestRow(
    0,
    1580544550000000L // 2020-02-01 08:09:10 UTC to micros since the epoch
  )

  def row1: TestRow = TestRow(
    1,
    915181200000000L // 1999-01-01 09:00:00 UTC to micros since the epoch
  )

  def row2: TestRow = TestRow(
    2,
    946717200000000L // 2000-01-01 09:00:00 UTC to micros since the epoch
  )

  def row3: TestRow = TestRow(
    3,
    -31536000000000L // 1969-01-01 00:00:00 UTC to micros since the epoch
  )

  def row4: TestRow = TestRow(
    4,
    null
  )

  def utcTableExpectedResult: Seq[TestRow] = Seq(row0, row1, row2, row3, row4)

  def testTimestampTable(
    goldenTableName: String,
    timeZone: String,
    expectedResult: Seq[TestRow]): Unit = {
    withTimeZone(timeZone) {
      checkTable(
        path = goldenTablePath(goldenTableName),
        expectedAnswer = expectedResult,
        // for now omit "part" column since we don't support reading timestamp partition values
        readCols = Seq("id", "time")
      )
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
  def pstTableExpectedResult: Seq[TestRow] = utcTableExpectedResult.map { testRow =>
    val values = testRow.toSeq
    TestRow(
      values(0),
      if (values(1) == null) {
        null
      } else {
        values(1).asInstanceOf[Long] + DefaultKernelUtils.DateTimeConstants.MICROS_PER_HOUR * 8
      }
    )
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
      }

      checkTable(
        path = goldenTablePath(tablePath),
        expectedAnswer = expectedResult.map(TestRow.fromTuple(_))
      )
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Misc tests
  //////////////////////////////////////////////////////////////////////////////////

  test("end to end: multi-part checkpoint") {
    checkTable(
      path = goldenTablePath("multi-part-checkpoint"),
      expectedAnswer = (Seq(0L) ++ (0L until 30L)).map(TestRow(_))
    )
  }

  test("invalid path") {
    val invalidPath = "/path/to/non-existent-directory"
    val ex = intercept[TableNotFoundException] {
      Table.forPath(defaultTableClient, invalidPath)
    }
    assert(ex.getMessage().contains(s"Table at path `$invalidPath` is not found"))
  }

  test("table deleted after the `Table` creation") {
    withTempDir { temp =>
      val source = new File(goldenTablePath("data-reader-primitives"))
      val target = new File(temp.getCanonicalPath)
      FileUtils.copyDirectory(source, target)

      val table = Table.forPath(defaultTableClient, target.getCanonicalPath)
      // delete the table and try to get the snapshot. Expect a failure.
      FileUtils.deleteDirectory(target)
      val ex = intercept[TableNotFoundException] {
        table.getLatestSnapshot(defaultTableClient)
      }
      assert(ex.getMessage.contains(
        s"Table at path `file:${target.getCanonicalPath}` is not found"))
    }
  }

  test("read partitioned table") {
    val path = "file:" + goldenTablePath("data-reader-partition-values")

    // for now we don't support timestamp type partition columns so remove from read columns
    val readCols = Table.forPath(defaultTableClient, path).getLatestSnapshot(defaultTableClient)
      .getSchema(defaultTableClient)
      .withoutField("as_timestamp")
      .fields()
      .asScala
      .map(_.getName)

    val expectedAnswer = Seq(0, 1).map { i =>
      TestRow(
        i,
        i.toLong,
        i.toByte,
        i.toShort,
        i % 2 == 0,
        i.toFloat,
        i.toDouble,
        i.toString,
        "null",
        daysSinceEpoch(Date.valueOf("2021-09-08")),
        new BigDecimal(i),
        Seq(TestRow(i), TestRow(i), TestRow(i)),
        TestRow(i.toString, i.toString, TestRow(i, i.toLong)),
        i.toString
      )
    } ++ (TestRow(
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      Seq(TestRow(2), TestRow(2), TestRow(2)),
      TestRow("2", "2", TestRow(2, 2L)),
      "2"
    ) :: Nil)

    checkTable(
      path = path,
      expectedAnswer = expectedAnswer,
      readCols = readCols
    )
  }
}
