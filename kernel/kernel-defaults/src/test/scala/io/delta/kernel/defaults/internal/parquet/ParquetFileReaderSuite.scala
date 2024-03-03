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

import java.io.File
import java.math.{BigDecimal, RoundingMode}

import scala.collection.mutable.ArrayBuffer

import io.delta.golden.GoldenTableUtils.{goldenTableFile, goldenTablePath}
import io.delta.kernel.defaults.internal.DefaultKernelUtils.DateTimeConstants
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow, VectorTestUtils}
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

class ParquetFileReaderSuite extends AnyFunSuite
  with ParquetSuiteBase with VectorTestUtils with ExpressionTestUtils {

  def getSingleParquetFile(directory: File): String = {
    val parquetFiles = directory.listFiles().filter(_.getName.endsWith(".parquet"))
    assert(parquetFiles.size == 1)
    parquetFiles.head.getAbsolutePath
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Decimal type tests
  //////////////////////////////////////////////////////////////////////////////////

  private val DECIMAL_TYPES_DICT_FILE_V1 = getSingleParquetFile(
    goldenTableFile("parquet-decimal-dictionaries-v1"))

  private val DECIMAL_TYPES_DICT_FILE_V2 = getSingleParquetFile(
    goldenTableFile("parquet-decimal-dictionaries-v2"))

  test("decimals encoded using dictionary encoding ") {
    val expResult = (0 until 1000000).map { i =>
      TestRow(i, BigDecimal.valueOf(i%5), BigDecimal.valueOf(i%6), BigDecimal.valueOf(i%2))
    }

    val readSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("col1", new DecimalType(9, 0)) // INT32: 1 <= precision <= 9
      .add("col2", new DecimalType(12, 0)) // INT64: 10 <= precision <= 18
      .add("col3", new DecimalType(25, 0)) // FIXED_LEN_BYTE_ARRAY

    for (file <- Seq(DECIMAL_TYPES_DICT_FILE_V1, DECIMAL_TYPES_DICT_FILE_V2)) {
      val actResult = readParquetFilesUsingSpark(file, readSchema)

      checkAnswer(actResult, expResult)
    }
  }

  private val LARGE_SCALE_DECIMAL_TYPES_FILE = getSingleParquetFile(
    goldenTableFile("parquet-decimal-type"))

  test("large scale decimal type file") {

    def expand(n: BigDecimal): BigDecimal = {
      n.scaleByPowerOfTen(5).add(n)
    }

    val expResult = (0 until 99998).map { i =>
      if (i % 85 == 0) {
        val n = BigDecimal.valueOf(i)
        TestRow(i, n.movePointLeft(1).setScale(1), n.setScale(5), n.setScale(5))
      } else {
        val negation = if (i % 33 == 0) {
          -1
        } else {
          1
        }
        val n = BigDecimal.valueOf(i*negation)
        TestRow(
          i,
          n.movePointLeft(1),
          expand(n).movePointLeft(5),
          expand(expand(expand(n))).movePointLeft(5)
        )
      }
    }

    val readSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("col1", new DecimalType(5, 1)) // INT32: 1 <= precision <= 9
      .add("col2", new DecimalType(10, 5)) // INT64: 10 <= precision <= 18
      .add("col3", new DecimalType(20, 5)) // FIXED_LEN_BYTE_ARRAY

    val actResult = readParquetFilesUsingSpark(LARGE_SCALE_DECIMAL_TYPES_FILE, readSchema)

    checkAnswer(actResult, expResult)
  }

  private val ALL_TYPES_FILE = getSingleParquetFile(goldenTableFile("parquet-all-types"))

  test("read all types of data") {
    val readSchema = tableSchema(goldenTablePath("parquet-all-types"))

    val actResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    val expResult = generateRowsFromAllTypesFile(readSchema)

    checkAnswer(actResult, expResult)
  }

  test("read subset of columns") {
    val readSchema = new StructType()
      .add("byteType", ByteType.BYTE)
      .add("booleanType", BooleanType.BOOLEAN)
      .add("stringType", StringType.STRING)
      .add("dateType", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
      .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))

    val actResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    val expResult = generateRowsFromAllTypesFile(readSchema)

    checkAnswer(actResult, expResult)
  }

  test("read subset of columns with missing columns in file") {
    val readSchema = new StructType()
      .add("booleanType", BooleanType.BOOLEAN)
      .add("integerType", IntegerType.INTEGER)
      .add("missing_column_struct", new StructType().add("ab", IntegerType.INTEGER))
      .add("longType", LongType.LONG)
      .add("missing_column_primitive", DateType.DATE)
      .add("nested_struct", new StructType()
        .add("aa", StringType.STRING)
        .add("ac", new StructType().add("aca", IntegerType.INTEGER)))

    val actResult = readParquetFilesUsingSpark(ALL_TYPES_FILE, readSchema)

    val expResult = generateRowsFromAllTypesFile(readSchema)

    checkAnswer(actResult, expResult)
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
    val filePath = getTestResourceFilePath("parquet/")
    val actResult2 = readParquetFilesUsingKernel(filePath, readSchema)
    val expResult2 = (0L until 20000L).map(i => TestRow(i, i))

    checkAnswer(actResult2, expResult2)
  }

  private def generateRowsFromAllTypesFile(readSchema: StructType): Seq[TestRow] = {
    (0 until 200).map { rowId =>
      val expRow = ArrayBuffer.empty[Any]
      readSchema.fields.forEach { field =>
        val name = field.getName.toLowerCase()
        val expValue = name match {
          case "booleantype" => if (rowId % 87 != 0) rowId % 2 == 0 else null
          case "bytetype" => if (rowId % 72 != 0) rowId.toByte else null
          case "shorttype" => if (rowId % 56 != 0) rowId.toShort else null
          case "datetype" => if (rowId % 61 != 0) {
              Math.floorDiv(rowId * 20000000L, DateTimeConstants.MILLIS_PER_DAY).toInt
            } else null
          case "integertype" => if (rowId % 23 != 0) rowId else null
          case "longtype" => if (rowId % 25 != 0) rowId + 1L else null
          case "floattype" => if (rowId % 28 != 0) {
              new BigDecimal(rowId * 0.234f).setScale(3, RoundingMode.HALF_UP)
                .stripTrailingZeros.floatValue
            } else null
          case "doubletype" => if (rowId % 54 != 0) rowId * 234234.23d else null
          case "stringtype" => if (rowId % 57 != 0) rowId.toString else null
          case "binarytype" => if (rowId % 59 != 0) rowId.toString.getBytes else null
          case "timestamptype" =>
            // Tests only for spark.sql.parquet.outputTimestampTyp = INT96, other formats
            // are tested in end-to-end tests in DeltaTableReadsSuite
            if (rowId % 62 != 0) 23423523L * rowId * 1000 else null
          case "decimal" =>
            // Value is rounded to scale=2 when written
            if (rowId % 67 != 0) new BigDecimal(rowId * 123.52).setScale(2, RoundingMode.HALF_UP)
            else null
          case "nested_struct" => generateNestedStructColumn(rowId)
          case "array_of_prims" => if (rowId % 25 == 0) null
            else if (rowId % 29 == 0) Vector.empty[Int]
            else Vector(rowId, null, rowId + 1)
          case "array_of_arrays" => generateArrayOfArraysColumn(rowId)
          case "array_of_structs" => Vector(TestRow(rowId.toLong), null)
          case "map_of_prims" => generateMapOfPrimsColumn(rowId)
          case "map_of_rows" =>
            Map(rowId + 1 -> (if (rowId % 10 == 0) TestRow(rowId * 20L) else null))
          case "map_of_arrays" => generateMapOfArraysColumn(rowId)
          case "missing_column_primitive" => null
          case "missing_column_struct" => null
          case _ => throw new IllegalArgumentException("unknown column: " + name)
        }
        expRow += expValue
      }
      TestRow(expRow: _*)
    }
  }

  private def generateNestedStructColumn(rowId: Int): TestRow = {
    if (rowId % 63 == 0) return null
    if (rowId % 19 != 0 && rowId % 23 != 0) return TestRow(rowId.toString, TestRow(rowId))
    if (rowId % 23 == 0) return TestRow(rowId.toString, null)
    TestRow(null, null)
  }

  private def generateMapOfPrimsColumn(rowId: Int): Map[Int, Any] = {
    if (rowId % 28 == 0) return null
    if (rowId % 30 == 0) return Map.empty[Int, Any]
    Map(
      rowId -> (if (rowId % 29 == 0) null else rowId + 2L),
      (if (rowId % 27 != 0) rowId + 2 else rowId + 3) -> (rowId + 9L)
    )
  }

  private def generateArrayOfArraysColumn(rowId: Int): Seq[Seq[Any]] = {
    if (rowId % 8 == 0) return null

    val emptyArray = ArrayBuffer.empty[Int]
    val singleElemArray = ArrayBuffer(rowId)
    val doubleElemArray = ArrayBuffer(rowId + 10, rowId + 20)
    val arrayWithNulls = ArrayBuffer(null, rowId + 200)
    val singleElemNullArray = ArrayBuffer(null)

    rowId % 7 match {
      case 0 => Vector(singleElemArray, singleElemArray, arrayWithNulls)
      case 1 => Vector(singleElemArray, doubleElemArray, emptyArray)
      case 2 => Vector(arrayWithNulls)
      case 3 => Vector(singleElemNullArray)
      case 4 => Vector(null)
      case 5 => Vector(emptyArray)
      case 6 => Vector.empty[ArrayBuffer[Int]]
    }
  }

  private def generateMapOfArraysColumn(rowId: Int): Map[Long, Seq[Any]] = {
    if (rowId % 30 == 0) return null

    val val1 = if (rowId % 4 == 0) ArrayBuffer[Any](rowId, null, rowId + 1)
      else ArrayBuffer.empty[Any]
    val val2 = if (rowId % 7 == 0) ArrayBuffer.empty[Any]
      else ArrayBuffer[Any](null)
    val expMap = if (rowId % 24 == 0) Map.empty[Long, ArrayBuffer[Any]]
      else Map[Long, ArrayBuffer[Any]](rowId.toLong -> val1, rowId + 1L -> val2)

    expMap
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////
  // TODO move over from DeltaTableReadsSuite once there is better testing infra
}
