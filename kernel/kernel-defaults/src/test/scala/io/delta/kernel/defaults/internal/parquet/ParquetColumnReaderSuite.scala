/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet

import java.util.TimeZone

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Suite covering reading Parquet columns with different types.
 */
class ParquetColumnReaderSuite  extends AnyFunSuite with ParquetSuiteBase {

  /**
   * Defines a test case for this suite.
   * @param columnName Column to read from the file
   * @param toType Read type to use. May be different from the actualy Parquet type.
   * @param expectedExpr Expression returning the expected value for each row in the file.
   */
  case class TestCase(columnName: String, toType: DataType, expectedExpr: Int => Any)

  private val wideningTestCases: Seq[TestCase] = Seq(
    TestCase("ByteType", ShortType.SHORT, i => if (i % 72 != 0) i.toByte.toShort else null),
    TestCase("ByteType", IntegerType.INTEGER, i => if (i % 72 != 0) i.toByte.toInt else null),
    TestCase("ByteType", LongType.LONG, i => if (i % 72 != 0) i.toByte.toLong else null),
    TestCase("ShortType", IntegerType.INTEGER, i => if (i % 56 != 0) i else null),
    TestCase("ShortType", LongType.LONG, i => if (i % 56 != 0) i.toLong else null),
    TestCase("IntegerType", LongType.LONG, i => if (i % 23 != 0) i.toLong else null),
    TestCase("IntegerType", DoubleType.DOUBLE, i => if (i % 23 != 0) i.toDouble else null),
    TestCase("FloatType", DoubleType.DOUBLE,
      i => if (i % 28 != 0) (i * 0.234).toFloat.toDouble else null),
    TestCase("decimal", new DecimalType(12, 2),
      i => if (i % 67 != 0) java.math.BigDecimal.valueOf(i * 12352, 2) else null),
    TestCase("decimal", new DecimalType(12, 4),
      i => if (i % 67 != 0) java.math.BigDecimal.valueOf(i * 1235200, 4) else null),
    TestCase("decimal", new DecimalType(26, 10),
      i => if (i % 67 != 0) java.math.BigDecimal.valueOf(i * 12352, 2).setScale(10)
      else null),
    TestCase("IntegerType", new DecimalType(10, 0),
      i => if (i % 23 != 0) new java.math.BigDecimal(i) else null),
    TestCase("IntegerType", new DecimalType(16, 4),
      i => if (i % 23 != 0) new java.math.BigDecimal(i).setScale(4) else null),
    TestCase("LongType", new DecimalType(20, 0),
      i => if (i % 25 != 0) new java.math.BigDecimal(i + 1) else null),
    TestCase("LongType", new DecimalType(28, 6),
      i => if (i % 25 != 0) new java.math.BigDecimal(i + 1).setScale(6) else null)
  )

  for (testCase <- wideningTestCases)
  test(s"parquet widening conversion - ${testCase.columnName} -> ${testCase.toType.toString}") {
    val inputLocation = goldenTablePath("parquet-all-types")
    val readSchema = new StructType().add(testCase.columnName, testCase.toType)
    val result = readParquetFilesUsingKernel(inputLocation, readSchema)
    val expected = (0 until 200)
      .map { i => TestRow(testCase.expectedExpr(i))}
    checkAnswer(result, expected)
  }

  test (s"parquet widening conversion - date -> timestamp_ntz") {
    val timezones =
      Seq("UTC", "Iceland", "PST", "America/Los_Angeles", "Etc/GMT+9", "Asia/Beirut", "JST")
    for (fromTimezone <- timezones; toTimezone <- timezones) {
      val inputLocation = goldenTablePath(s"data-reader-date-types-$fromTimezone")
      TimeZone.setDefault(TimeZone.getTimeZone(toTimezone))

      val readSchema = new StructType().add("date", TimestampNTZType.TIMESTAMP_NTZ)
      val result = readParquetFilesUsingKernel(inputLocation, readSchema)
      // 1577836800000000L -> 2020-01-01 00:00:00 UTC
      checkAnswer(result, Seq(TestRow(1577836800000000L)))
    }
  }
}
