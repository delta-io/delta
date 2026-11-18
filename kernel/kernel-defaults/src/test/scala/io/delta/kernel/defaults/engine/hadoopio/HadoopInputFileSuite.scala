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

import java.io.{File, FileOutputStream}
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import io.delta.kernel.defaults.utils.TestUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.Options.OpenFileOptions
import org.apache.hadoop.fs.impl.OpenFileParameters
import org.scalatest.funsuite.AnyFunSuite

class HadoopInputFileSuite extends AnyFunSuite with TestUtils {

  private val helloBytes = "hello-kernel".getBytes("UTF-8")

  private def withRecordingFs(testFn: (RecordingFs, Path) => Unit): Unit = {
    withTempDir { tempDir =>
      val file = new File(tempDir, "input.bin")
      val out = new FileOutputStream(file)
      try out.write(helloBytes)
      finally out.close()

      val fs = new RecordingFs()
      fs.initialize(file.toURI, new Configuration())
      try testFn(fs, new Path(file.toURI))
      finally fs.close()
    }
  }

  test("newStream uses openFile() with length option when fileSize is known") {
    withRecordingFs { (fs, path) =>
      val input = new HadoopInputFile(fs, path, helloBytes.length.toLong)
      val stream = input.newStream()
      try {
        val buf = new Array[Byte](helloBytes.length)
        stream.readFully(buf, 0, buf.length)
        assertResult(helloBytes.toSeq)(buf.toSeq)
      } finally {
        stream.close()
      }

      assertResult(1)(fs.openFileCalls.get())
      assertResult(Some(helloBytes.length.toLong))(fs.lastOpenFileLength)
    }
  }

  test("newStream omits the length option for an empty file (fileSize 0)") {
    withRecordingFs { (fs, path) =>
      // Truncate the temp file to empty.
      val emptyFile = new java.io.File(path.toUri)
      val out = new FileOutputStream(emptyFile)
      out.close()

      val input = new HadoopInputFile(fs, path, 0L)
      val stream = input.newStream()
      try {
        assertResult(-1)(stream.read())
      } finally {
        stream.close()
      }

      assertResult(1)(fs.openFileCalls.get())
      assertResult(None)(fs.lastOpenFileLength)
    }
  }

  // 0 is an unknown-size placeholder (e.g. FileStatus.of(path)), not a real length, so the hint
  // must be omitted; otherwise S3A trusts it and reads the non-empty file back as empty.
  test("newStream omits the length option when fileSize is 0 for a non-empty file") {
    withRecordingFs { (fs, path) =>
      val input = new HadoopInputFile(fs, path, 0L)
      val stream = input.newStream()
      try {
        val buf = new Array[Byte](helloBytes.length)
        stream.readFully(buf, 0, buf.length)
        assertResult(helloBytes.toSeq)(buf.toSeq)
      } finally {
        stream.close()
      }

      assertResult(1)(fs.openFileCalls.get())
      assertResult(None)(fs.lastOpenFileLength)
    }
  }

  test("newStream omits the length option when fileSize is negative") {
    withRecordingFs { (fs, path) =>
      val input = new HadoopInputFile(fs, path, -1L)
      val stream = input.newStream()
      try {
        val buf = new Array[Byte](helloBytes.length)
        stream.readFully(buf, 0, buf.length)
        assertResult(helloBytes.toSeq)(buf.toSeq)
      } finally {
        stream.close()
      }

      assertResult(1)(fs.openFileCalls.get())
      assertResult(None)(fs.lastOpenFileLength)
    }
  }
}

private class RecordingFs extends RawLocalFileSystem {
  val openFileCalls = new AtomicInteger(0)
  @volatile var lastOpenFileLength: Option[Long] = None

  override protected def openFileWithOptions(
      path: Path,
      parameters: OpenFileParameters): CompletableFuture[FSDataInputStream] = {
    openFileCalls.incrementAndGet()
    val raw = parameters.getOptions.get(OpenFileOptions.FS_OPTION_OPENFILE_LENGTH)
    if (raw != null) {
      lastOpenFileLength = Some(raw.toLong)
    }
    super.openFileWithOptions(path, parameters)
  }
}
