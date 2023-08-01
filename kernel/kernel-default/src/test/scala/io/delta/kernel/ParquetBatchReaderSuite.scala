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

import java.io.File
import java.math.BigDecimal

import org.scalatest.funsuite.AnyFunSuite

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.parquet.ParquetBatchReader
import io.delta.kernel.types.{DecimalType, IntegerType, StructType}
import org.apache.hadoop.conf.Configuration

class ParquetBatchReaderSuite extends AnyFunSuite with TestUtils {

  def getSingleParquetFile(directory: File): String = {
    val parquetFiles = directory.listFiles().filter(_.getName.endsWith(".parquet"))
    assert(parquetFiles.size == 1)
    parquetFiles.head.getAbsolutePath
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Decimal type tests
  //////////////////////////////////////////////////////////////////////////////////

  private val DECIMAL_TYPES_DICT_FILE = getSingleParquetFile(
    goldenTableFile("parquet-decimal-dictionaries"))

  test("decimals encoded using dictionary encoding ") {
    val expectedResult = (0 until 1000000).map { i =>
      (i, BigDecimal.valueOf(i%5), BigDecimal.valueOf(i%6), BigDecimal.valueOf(i%2))
    }.toSet

    val readSchema = new StructType()
      .add("id", IntegerType.INSTANCE)
      .add("col1", new DecimalType(9, 0))
      .add("col2", new DecimalType(12, 0))
      .add("col3", new DecimalType(25, 0))

    val batchReader = new ParquetBatchReader(new Configuration())
    val batches = batchReader.read(DECIMAL_TYPES_DICT_FILE, readSchema)

    val result = batches.toSeq.flatMap(_.getRows.toSeq).map { row =>
      (row.getInt(0), row.getDecimal(1), row.getDecimal(2), row.getDecimal(3))
    }

    assert(expectedResult == result.toSet)
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
        (i, n.movePointLeft(1).setScale(1), n.setScale(5), n.setScale(5))
      } else {
        val negation = if (i % 33 == 0) {
          -1
        } else {
          1
        }
        val n = BigDecimal.valueOf(i*negation)
        (
          i,
          n.movePointLeft(1),
          expand(n).movePointLeft(5),
          expand(expand(expand(n))).movePointLeft(5)
        )
      }
    }.toSet

    val readSchema = new StructType()
      .add("id", IntegerType.INSTANCE)
      .add("col1", new DecimalType(5, 1))
      .add("col2", new DecimalType(10, 5))
      .add("col3", new DecimalType(20, 5))

    val batchReader = new ParquetBatchReader(new Configuration())
    val batches = batchReader.read(LARGE_SCALE_DECIMAL_TYPES_FILE, readSchema)

    val result = batches.toSeq.flatMap(_.getRows.toSeq).map { row =>
      (row.getInt(0), row.getDecimal(1), row.getDecimal(2), row.getDecimal(3))
    }

    assert(expectedResult == result.toSet)
  }
}
