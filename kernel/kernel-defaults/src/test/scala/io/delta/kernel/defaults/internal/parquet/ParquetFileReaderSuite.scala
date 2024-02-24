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
import java.lang.{Double => DoubleJ, Float => FloatJ}
import java.math.BigDecimal

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow, VectorTestUtils}
import io.delta.kernel.types.{DecimalType, IntegerType, StructType}
import org.apache.hadoop.conf.Configuration
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
    val expectedResult = (0 until 1000000).map { i =>
      TestRow(i, BigDecimal.valueOf(i%5), BigDecimal.valueOf(i%6), BigDecimal.valueOf(i%2))
    }

    val readSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("col1", new DecimalType(9, 0)) // INT32: 1 <= precision <= 9
      .add("col2", new DecimalType(12, 0)) // INT64: 10 <= precision <= 18
      .add("col3", new DecimalType(25, 0)) // FIXED_LEN_BYTE_ARRAY

    val batchReader = new ParquetBatchReader(new Configuration())
    for (file <- Seq(DECIMAL_TYPES_DICT_FILE_V1, DECIMAL_TYPES_DICT_FILE_V2)) {
      val batches = batchReader.read(file, readSchema)
      val result = batches.toSeq.flatMap(_.getRows.toSeq)
      checkAnswer(result, expectedResult)
    }
  }

  private val LARGE_SCALE_DECIMAL_TYPES_FILE = getSingleParquetFile(
    goldenTableFile("parquet-decimal-type"))

  test("large scale decimal type file") {

    def expand(n: BigDecimal): BigDecimal = {
      n.scaleByPowerOfTen(5).add(n)
    }

    val expectedResult = (0 until 99998).map { i =>
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

    val batchReader = new ParquetBatchReader(new Configuration())
    val batches = batchReader.read(LARGE_SCALE_DECIMAL_TYPES_FILE, readSchema)

    val result = batches.toSeq.flatMap(_.getRows.toSeq)
    checkAnswer(result, expectedResult)
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////
  // TODO move over from DeltaTableReadsSuite once there is better testing infra

  test("read all types of data") {
    Seq(
      ( // no stats collection as NaN is present
        Seq(Float.NegativeInfinity, Float.MinValue, -1.0f,
          -0.0f, 0.0f, 1.0f, null, Float.MaxValue, Float.PositiveInfinity, Float.NaN),
        Seq(Double.NegativeInfinity, Double.MinValue, -1.0d,
          -0.0d, 0.0d, 1.0d, null, Double.MaxValue, Double.PositiveInfinity, Double.NaN),
        10,
        (null, null, null),
        (null, null, null)
      ),
      ( // Min and max are infinities
        Seq(Float.NegativeInfinity, Float.MinValue, -1.0f,
          -0.0f, 0.0f, 1.0f, null, Float.MaxValue, Float.PositiveInfinity),
        Seq(Double.NegativeInfinity, Double.MinValue, -1.0d,
          -0.0d, 0.0d, 1.0d, null, Double.MaxValue, Double.PositiveInfinity),
        9,
        (Float.NegativeInfinity, Float.PositiveInfinity, 1L),
        (Double.NegativeInfinity, Double.PositiveInfinity, 1L)
      )
    ).foreach {
      case (floats: Seq[FloatJ], doubles: Seq[DoubleJ], expRowCount, expFltStats, expDblStats) =>
        withTempDir { tempPath =>
          val targetDir = tempPath.getAbsolutePath
          val testBatch = columnarBatch(floatVector(floats), doubleVector(doubles))
          val dataToWrite = Seq(testBatch.toFiltered)

          val writeOutput =
            writeToParquetUsingKernel(
              dataToWrite,
              targetDir,
              statsColumns = Seq(col("col_0"), col("col_1")))

          assert(parquetFileRowCount(targetDir) == expRowCount)
          verifyContent(targetDir, dataToWrite)

          val stats = writeOutput.head.getStatistics.get()

          def getStats(column: String): (Object, Object, Object) =
            (
              Option(stats.getMinValues.get(col(column))).map(_.getValue).orNull,
              Option(stats.getMaxValues.get(col(column))).map(_.getValue).orNull,
              Option(stats.getNullCounts.get(col(column))).orNull
            )

          assert(getStats("col_0") === expFltStats)
          assert(getStats("col_1") === expDblStats)
        }
    }
  }
}
