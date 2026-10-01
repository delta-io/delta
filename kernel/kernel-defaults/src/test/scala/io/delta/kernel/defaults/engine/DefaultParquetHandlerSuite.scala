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
package io.delta.kernel.defaults.engine

import java.io.IOException
import java.nio.file.FileAlreadyExistsException

import scala.collection.JavaConverters._

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.internal.util.Utils.toCloseableIterator

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DefaultParquetHandlerSuite extends AnyFunSuite with ParquetSuiteBase {

  val parquetHandler = new DefaultParquetHandler(
    new HadoopFileIO(
      new Configuration {
        set("delta.kernel.default.parquet.reader.batch-size", "10")
      }))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for `writeParquetFileAtomically`. Test for `writeParquetFiles` are covered in
  // `ParquetFileWriterSuite` as this API implementation by itself doesn't have any special logic.
  /////////////////////////////////////////////////////////////////////////////////////////////////
  test("atomic write of a single Parquet file") {
    withTempDir { tempDir =>
      val inputLocation = goldenTableFile("parquet-all-types").toString

      val dataToWrite =
        readParquetUsingKernelAsColumnarBatches(inputLocation, tableSchema(inputLocation))
          .map(_.toFiltered)
      assert(dataToWrite.size === 1)
      assert(dataToWrite.head.getData.getSize === 200)

      val filePath = tempDir + "/1.parquet"

      def writeAndVerify(): Unit = {
        parquetHandler.writeParquetFileAtomically(
          filePath,
          toCloseableIterator(dataToWrite.asJava.iterator()))

        // Uses both Spark and Kernel to read and verify the content is same as the one written.
        verifyContent(tempDir.getAbsolutePath, dataToWrite)
      }

      writeAndVerify()

      // Try to write as same file and expect an error
      val e = intercept[IOException] {
        parquetHandler.writeParquetFileAtomically(
          filePath,
          toCloseableIterator(dataToWrite.asJava.iterator()))
      }
      assert(e.getCause.isInstanceOf[FileAlreadyExistsException])
    }
  }
}
