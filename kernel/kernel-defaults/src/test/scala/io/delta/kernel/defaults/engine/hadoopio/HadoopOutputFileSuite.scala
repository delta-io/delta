/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine.hadoopio

import java.io.File

import io.delta.kernel.defaults.utils.TestUtils
import io.delta.storage.LogStore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class HadoopOutputFileSuite extends AnyFunSuite with TestUtils {

  /**
   * Registers [[NonPartialWriteVisibleLogStore]] as the LogStore for the `file` scheme, so that
   * `HadoopOutputFile.create(putIfAbsent = true)` resolves `useRename = false`, i.e. the returned
   * stream writes directly to the target path instead of a temp-then-rename path.
   */
  private def confWithNonRenamingLogStore(): Configuration = {
    val conf = new Configuration()
    conf.set(
      "io.delta.kernel.logStore.file.impl",
      classOf[NonPartialWriteVisibleLogStore].getName)
    conf
  }

  test("abort() with useRename=false discards the write and publishes no file") {
    withTempDir { tempDir =>
      val targetPath = new File(tempDir, "target.txt").getAbsolutePath
      val outputFile = new HadoopOutputFile(confWithNonRenamingLogStore(), targetPath)

      val stream = outputFile.create( /* putIfAbsent = */ true)
      stream.write("partial-content".getBytes("UTF-8"))
      stream.abort()
      stream.close()

      assert(
        !new File(targetPath).exists(),
        "an aborted write with useRename=false must not publish a file at the target path")
    }
  }

  test("normal write with useRename=false publishes the file") {
    withTempDir { tempDir =>
      val targetPath = new File(tempDir, "target.txt").getAbsolutePath
      val outputFile = new HadoopOutputFile(confWithNonRenamingLogStore(), targetPath)

      val content = "full-content".getBytes("UTF-8")
      val stream = outputFile.create( /* putIfAbsent = */ true)
      stream.write(content)
      stream.close()

      val publishedFile = new File(targetPath)
      assert(publishedFile.exists(), "a non-aborted write must publish a file at the target path")
      assertResult(content.toSeq)(java.nio.file.Files.readAllBytes(publishedFile.toPath).toSeq)
    }
  }
}

/** A [[LogStore]] whose `isPartialWriteVisible` is `false`, e.g. mimicking GCS/S3-like stores. */
class NonPartialWriteVisibleLogStore(initHadoopConf: Configuration)
    extends LogStore(initHadoopConf) {

  override def read(
      path: Path,
      hadoopConf: Configuration): io.delta.kernel.utils.CloseableIterator[String] =
    throw new UnsupportedOperationException("not used in this test")

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit =
    throw new UnsupportedOperationException("not used in this test")

  override def listFrom(
      path: Path,
      hadoopConf: Configuration): java.util.Iterator[org.apache.hadoop.fs.FileStatus] =
    throw new UnsupportedOperationException("not used in this test")

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path =
    throw new UnsupportedOperationException("not used in this test")

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean =
    false
}
