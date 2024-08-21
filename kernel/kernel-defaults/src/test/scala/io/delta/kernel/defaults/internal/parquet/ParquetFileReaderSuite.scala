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
package io.delta.kernel.defaults.internal.parquet

import java.math.BigDecimal
import java.util.TimeZone

import io.delta.golden.GoldenTableUtils.{goldenTableFile, goldenTablePath}
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.funsuite.AnyFunSuite

class ParquetFileReaderSuite extends AnyFunSuite
  with ParquetSuiteBase with VectorTestUtils with ExpressionTestUtils {

  test("decimals encoded using dictionary encoding ") {
    // Below golden tables contains three decimal columns
    // each stored in a different physical format: int32, int64 and fixed binary
    val decimalDictFileV1 = goldenTableFile("parquet-decimal-dictionaries-v1").getAbsolutePath
    val decimalDictFileV2 = goldenTableFile("parquet-decimal-dictionaries-v2").getAbsolutePath

    val expResult = (0 until 1000000).map { i =>
      TestRow(i, BigDecimal.valueOf(i%5), BigDecimal.valueOf(i%6), BigDecimal.valueOf(i%2))
    }

    val readSchema = tableSchema(decimalDictFileV1)

    for (file <- Seq(decimalDictFileV1, decimalDictFileV2)) {
      val actResult = readParquetFilesUsingKernel(file, readSchema)

      checkAnswer(actResult, expResult)
    }
  }

  test("large scale decimal type file") {
    val largeScaleDecimalTypesFile = goldenTableFile("parquet-decimal-type").getAbsolutePath

    def expand(n: BigDecimal): BigDecimal = {
      n.scaleByPowerOfTen(5).add(n)
    }

    val expResult = (0 until 99998).map { i =>
      if (i % 85 == 0) {
        val n = BigDecimal.valueOf(i)
        TestRow(i, n.movePointLeft(1).setScale(1), n.setScale(5), n.setScale(5))
      } else {
        val negation = if (i % 33 == 0) -1 else 1
        val n = BigDecimal.valueOf(i*negation)
        TestRow(
          i,
          n.movePointLeft(1),
          expand(n).movePointLeft(5),
          expand(expand(expand(n))).movePointLeft(5)
        )
      }
    }

    val readSchema = tableSchema(largeScaleDecimalTypesFile)

    val actResult = readParquetFilesUsingKernel(largeScaleDecimalTypesFile, readSchema)

    checkAnswer(actResult, expResult)
  }

  Seq(
      "parquet-all-types",
      "parquet-all-types-legacy-format"
  ).foreach { allTypesTableName =>
    test(s"read all types of data - $allTypesTableName") {
      val allTypesFile = goldenTableFile(allTypesTableName).getAbsolutePath
      val readSchema = tableSchema(allTypesFile)

      checkAnswer(
        readParquetFilesUsingKernel(allTypesFile, readSchema), /* actual */
        readParquetFilesUsingSpark(allTypesFile, readSchema) /* expected */)
    }
  }

  /**
   * Test case for reading a column using a given type.
   * @param columnName Column to read from the file
   * @param toType Read type to use. May be different from the actually Parquet type.
   * @param expectedExpr Expression returning the expected value for each row in the file.
   */
  case class TestCase(columnName: String, toType: DataType, expectedExpr: Int => Any)

  private val supportedConversions: Seq[TestCase] = Seq(
    // 'ByteType' column was generated with overflowing values, we need to call i.toByte to also
    // wrap around here and generate the correct expected values.
    TestCase("ByteType", ShortType.SHORT, i => if (i % 72 != 0) i.toByte.toShort else null),
    TestCase("ByteType", IntegerType.INTEGER, i => if (i % 72 != 0) i.toByte.toInt else null),
    TestCase("ByteType", LongType.LONG, i => if (i % 72 != 0) i.toByte.toLong else null),
    TestCase("ByteType", DoubleType.DOUBLE, i => if (i % 72 != 0) i.toByte.toDouble else null),
    TestCase("ShortType", IntegerType.INTEGER, i => if (i % 56 != 0) i else null),
    TestCase("ShortType", LongType.LONG, i => if (i % 56 != 0) i.toLong else null),
    TestCase("ShortType", DoubleType.DOUBLE, i => if (i % 56 != 0) i.toDouble else null),
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
      i => if (i % 25 != 0) new java.math.BigDecimal(i + 1).setScale(6) else null),

    TestCase("BinaryType", StringType.STRING, i => if (i % 59 != 0) i.toString else null)
  )

  // The following conversions are supported by Kernel but not by Spark with parquet-mr.
  // TODO: We should properly reject these conversions, a lot of them produce wrong results.
  // Collecting them here to document the current behavior.
  private val kernelOnlyConversions: Seq[TestCase] = Seq(
    // This conversions will silently overflow.
    TestCase("ShortType", ByteType.BYTE, i => if (i % 56 != 0) i.toByte else null),
    TestCase("IntegerType", ByteType.BYTE, i => if (i % 23 != 0) i.toByte else null),
    TestCase("IntegerType", ShortType.SHORT, i => if (i % 23 != 0) i.toShort else null),

    // This is reading the unscaled decimal value as long which is wrong.
    TestCase("decimal", LongType.LONG, i => if (i % 67 != 0) i.toLong * 12352 else null),

    // The following conversions seem legit, although Spark rejects them.
    TestCase("ByteType", DateType.DATE, i => if (i % 72 != 0) i.toByte.toInt else null),
    TestCase("ShortType", DateType.DATE, i => if (i % 56 != 0) i else null),
    TestCase("IntegerType", DateType.DATE, i => if (i % 23 != 0) i else null),
    TestCase("StringType", BinaryType.BINARY, i => if (i % 57 != 0) i.toString.getBytes else null)
  )

  for (testCase <- supportedConversions ++ kernelOnlyConversions)
  test(s"parquet supported conversion - ${testCase.columnName} -> ${testCase.toType.toString}") {
    val inputLocation = goldenTablePath("parquet-all-types")
    val readSchema = new StructType().add(testCase.columnName, testCase.toType)
    val result = readParquetFilesUsingKernel(inputLocation, readSchema)
    val expected = (0 until 200)
      .map { i => TestRow(testCase.expectedExpr(i))}
    checkAnswer(result, expected)

    if (!kernelOnlyConversions.contains(testCase)) {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val sparkResult = readParquetFilesUsingSpark(inputLocation, readSchema)
        checkAnswer(result, sparkResult)
      }
    }
  }

  test (s"parquet supported conversion - date -> timestamp_ntz") {
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

  for(column <- Seq("BooleanType", "ByteType", "ShortType", "IntegerType", "LongType",
    "FloatType", "DoubleType", "StringType", "BinaryType")) {
    test(s"parquet unsupported conversion from $column") {
      val inputLocation = goldenTablePath("parquet-all-types")
      val supportedTypes = (supportedConversions ++ kernelOnlyConversions)
        .filter(_.columnName == column)
        .map(_.toType)
      val unsupportedTypes = ALL_TYPES
        .filterNot(supportedTypes.contains)
        .filterNot(_.getClass.getSimpleName == column)

      for (toType <- unsupportedTypes) {
        val readSchema = new StructType().add(column, toType)
        withClue(s"Converting $column to $toType") {
          assertThrows[Throwable](readParquetFilesUsingKernel(inputLocation, readSchema))
        }
      }
    }
  }

  test(s"parquet unsupported conversion from decimal") {
    val inputLocation = goldenTablePath("parquet-all-types")
    // 'decimal' column is Decimal(10, 2) which fits into a long.
    for (toType <- ALL_TYPES.filterNot(_ == LongType.LONG)) {
      val readSchema = new StructType().add("decimal", toType)
      withClue(s"Converting decimal to $toType") {
        // We don't properly reject conversions and the error we get are currently varying a lot, so
        // we catch a generic Throwable.
        // TODO: Uniformize rejecting unsupported conversions.
        assertThrows[Throwable](readParquetFilesUsingKernel(inputLocation, readSchema))
      }
    }
  }

  test("read subset of columns") {
    val tablePath = goldenTableFile("parquet-all-types").getAbsolutePath
    val readSchema = new StructType()
      .add("byteType", ByteType.BYTE)
      .add("booleanType", BooleanType.BOOLEAN)
      .add("stringType", StringType.STRING)
      .add("dateType", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
      .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))

    checkAnswer(
      readParquetFilesUsingKernel(tablePath, readSchema), /* actual */
      readParquetFilesUsingSpark(tablePath, readSchema) /* expected */)
  }

  test("read subset of columns with missing columns in file") {
    val tablePath = goldenTableFile("parquet-all-types").getAbsolutePath
    val readSchema = new StructType()
      .add("booleanType", BooleanType.BOOLEAN)
      .add("integerType", IntegerType.INTEGER)
      .add("missing_column_struct", new StructType().add("ab", IntegerType.INTEGER))
      .add("longType", LongType.LONG)
      .add("missing_column_primitive", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))

    checkAnswer(
      readParquetFilesUsingKernel(tablePath, readSchema), /* actual */
      readParquetFilesUsingSpark(tablePath, readSchema) /* expected */)
  }

  test("read columns with int96 timestamp_ntz") {
    // Spark doesn't support writing timestamp_NTZ as INT96 (although reads are)
    // So we're reusing a pre-written file directly.
    val filePath = getTestResourceFilePath("parquet/parquet-timestamp_ntz_int96.parquet")
    val readSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("time", TimestampNTZType.TIMESTAMP_NTZ)
    checkAnswer(
      readParquetFilesUsingKernel(filePath, readSchema), /* actual */
      Seq(TestRow(1, 915181200000000L) /* expected */)
    )
  }

  test("request row indices") {
    val readSchema = new StructType()
      .add("id", LongType.LONG)
      .add(StructField.METADATA_ROW_INDEX_COLUMN)

    val path = getTestResourceFilePath("parquet-basic-row-indexes")
    val actResult1 = readParquetFilesUsingKernel(path, readSchema)
    val expResult1 = (0L until 30L)
      .map(i => TestRow(i, if (i < 10) i else if (i < 20) i - 10L else i - 20L))

    checkAnswer(actResult1, expResult1)

    // File with multiple row-groups [0, 20000) where rowIndex = id
    val filePath = getTestResourceFilePath("parquet/row_index_multiple_row_groups.parquet")
    val actResult2 = readParquetFilesUsingKernel(filePath, readSchema)
    val expResult2 = (0L until 20000L).map(i => TestRow(i, i))

    checkAnswer(actResult2, expResult2)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Test compatibility with Parquet legacy format files                                         //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // Test and the test file are copied from Spark's `ParquetThriftCompatibilitySuite`
  test("read parquet file generated by parquet-thrift") {
    val parquetFilePath = getTestResourceFilePath("parquet/parquet-thrift-compat.snappy.parquet")

    val readSchema = new StructType()
      .add("boolColumn", BooleanType.BOOLEAN)
      .add("byteColumn", ByteType.BYTE)
      .add("shortColumn", ShortType.SHORT)
      .add("intColumn", IntegerType.INTEGER)
      .add("longColumn", LongType.LONG)
      .add("doubleColumn", DoubleType.DOUBLE)
      // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
      // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
      // Thrift `STRING`s are encoded using UTF-8.
      .add("binaryColumn", StringType.STRING)
      .add("stringColumn", StringType.STRING)
      .add("enumColumn", StringType.STRING)
      // maybe indicates nullable columns, above ones are non-nullable
      .add("maybeBoolColumn", BooleanType.BOOLEAN)
      .add("maybeByteColumn", ByteType.BYTE)
      .add("maybeShortColumn", ShortType.SHORT)
      .add("maybeIntColumn", IntegerType.INTEGER)
      .add("maybeLongColumn", LongType.LONG)
      .add("maybeDoubleColumn", DoubleType.DOUBLE)
      // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
      // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
      // Thrift `STRING`s are encoded using UTF-8.
      .add("maybeBinaryColumn", StringType.STRING)
      .add("maybeStringColumn", StringType.STRING)
      .add("maybeEnumColumn", StringType.STRING)
      // TODO: not working - separate PR to handle 2-level legacy lists
      // .add("stringsColumn", new ArrayType(StringType.STRING, true /* containsNull */))
      // .add("intSetColumn", new ArrayType(IntegerType.INTEGER, true /* containsNull */))
      .add("intToStringColumn",
        new MapType(IntegerType.INTEGER, StringType.STRING, true /* valueContainsNull */))
      // TODO: not working - separate PR to handle 2-level legacy lists
      // .add("complexColumn", new MapType(
      //  IntegerType.INTEGER,
      //  new ArrayType(
      //    new StructType()
      //      .add("nestedIntsColumn", new ArrayType(IntegerType.INTEGER, true /* containsNull */))
      //      .add("nestedStringColumn", StringType.STRING)
      //      .add("stringColumn", StringType.STRING),
      //    true /* containsNull */),
      //  true /* valueContainsNull */))

    assert(parquetFileRowCount(parquetFilePath) === 10)
    checkAnswer(
      readParquetFilesUsingKernel(parquetFilePath, readSchema), /* actual */
      readParquetFilesUsingSpark(parquetFilePath, readSchema) /* expected */)
  }
}
