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
package io.delta.kernel.defaults.engine

import java.io.FileNotFoundException
import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.checkpoints.{Checkpointer, CheckpointMetaData}
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.util.JsonUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class DefaultFileSystemClientSuite extends AnyFunSuite with TestUtils {

  val fsClient = defaultEngine.getFileSystemClient
  val fs = FileSystem.get(configuration)

  private def writeFile(path: String, content: String): Unit = {
    val out = fs.create(new Path(path))
    try {
      out.write(content.getBytes("UTF-8"))
    } finally {
      out.close()
    }
  }

  private def readFile(path: String): String = {
    val fileStatus = fs.getFileStatus(new Path(path))
    val buffer = new Array[Byte](fileStatus.getLen.toInt)
    val in = fs.open(new Path(path))
    try {
      in.readFully(buffer)
      new String(buffer, "UTF-8")
    } finally {
      in.close()
    }
  }

  private def withTempSrcAndDestFiles(f: (String, String) => Unit): Unit = {
    withTempDir { tempDir =>
      val src = tempDir + "/source.txt"
      val dest = tempDir + "/dest.txt"
      f(src, dest)
    }
  }

  test("list from file") {
    val basePath = fsClient.resolvePath(getTestResourceFilePath("json-files"))
    val listFrom = fsClient.resolvePath(getTestResourceFilePath("json-files/2.json"))

    val actListOutput = new ArrayBuffer[String]()
    val files = fsClient.listFrom(listFrom)
    try {
      fsClient.listFrom(listFrom).forEach(f => actListOutput += f.getPath)
    } finally if (files != null) {
        files.close()
      }

    val expListOutput = Seq(basePath + "/2.json", basePath + "/3.json")

    assert(expListOutput === actListOutput)
  }

  test("list from non-existent file") {
    intercept[FileNotFoundException] {
      fsClient.listFrom("file:/non-existentfileTable/01.json")
    }
  }

  test("resolve path") {
    val inputPath = getTestResourceFilePath("json-files")
    val resolvedPath = fsClient.resolvePath(inputPath)

    assert("file:" + inputPath === resolvedPath)
  }

  test("resolve path on non-existent file") {
    val inputPath = "/non-existentfileTable/01.json"
    val resolvedPath = fsClient.resolvePath(inputPath)
    assert("file:" + inputPath === resolvedPath)
  }

  test("mkdirs") {
    withTempDir { tempdir =>
      val dir1 = tempdir + "/test"
      assert(fsClient.mkdirs(dir1))
      assert(fs.exists(new Path(dir1)))

      val dir2 = tempdir + "/test1/test2" // nested
      assert(fsClient.mkdirs(dir2))
      assert(fs.exists(new Path(dir2)))

      val dir3 = "/non-existentfileTable/sfdsd"
      assert(!fsClient.mkdirs(dir3))
      assert(!fs.exists(new Path(dir3)))
    }
  }

  test("getFileStatus") {
    val filePath = getTestResourceFilePath("json-files/1.json")
    val fileStatus = fsClient.getFileStatus(filePath)

    assert(fileStatus.getPath == fsClient.resolvePath(filePath))
    assert(fileStatus.getSize > 0)
    assert(fileStatus.getModificationTime > 0)
  }

  test("getFileStatus on non-existent file") {
    intercept[FileNotFoundException] {
      fsClient.getFileStatus("/non-existent-file.json")
    }
  }

  test("copyFileAtomically - overwrite=false, dest does not exist") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "test content")
      fsClient.copyFileAtomically(src, dest, false /* overwrite */ )

      assert(fs.exists(new Path(dest)))
      assert(readFile(dest).trim == "test content")
    }
  }

  test("copyFileAtomically - overwrite=false, dest exists") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "source content")
      writeFile(dest, "existing content")

      intercept[java.nio.file.FileAlreadyExistsException] {
        fsClient.copyFileAtomically(src, dest, false /* overwrite */ )
      }
    }
  }

  test("copyFileAtomically - overwrite=true") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "new content")
      writeFile(dest, "old content")

      fsClient.copyFileAtomically(src, dest, true /* overwrite */ )
      assert(readFile(dest).trim == "new content")
    }
  }

  test("copyFileAtomically with non-existent source") {
    withTempSrcAndDestFiles { (src, dest) =>
      intercept[FileNotFoundException] {
        fsClient.copyFileAtomically(src, dest, false /* overwrite */ )
      }
    }
  }

  test("readWholeFileAsUtf8 returns the full contents") {
    withTempDir { tempDir =>
      val path = tempDir + "/whole.txt"
      val content = """{"version":2,"size":9,"checkpointSchema":{"type":"struct"}}"""
      writeFile(path, content)
      assert(fsClient.readWholeFileAsUtf8(path) == content)
    }
  }

  test("readWholeFileAsUtf8 handles an empty file") {
    withTempDir { tempDir =>
      val path = tempDir + "/empty.txt"
      writeFile(path, "")
      assert(fsClient.readWholeFileAsUtf8(path) == "")
    }
  }

  test("readWholeFileAsUtf8 handles multi-line / multi-byte content") {
    withTempDir { tempDir =>
      val path = tempDir + "/multi.txt"
      // Multiple lines plus a multi-byte UTF-8 character to exercise byte-accurate decoding.
      val content = "line1\nline2\néèê"
      writeFile(path, content)
      assert(fsClient.readWholeFileAsUtf8(path) == content)
    }
  }

  test("readWholeFileAsUtf8 on non-existent file") {
    intercept[FileNotFoundException] {
      fsClient.readWholeFileAsUtf8("/non-existent-file.json")
    }
  }

  test("readWholeFileAsUtf8 reads a full V2 _last_checkpoint byte-for-byte") {
    // A real V2 pointer: deeply nested v2Checkpoint block with nonFileActions and sidecarFiles,
    // and an embedded schemaString containing escaped quotes. Exercises that the whole-file read
    // is byte-exact (no truncation on the large/nested content, escaped quotes preserved).
    withTempDir { tempDir =>
      val path = tempDir + "/v2_last_checkpoint.json"
      // Built from short pieces so each source line stays within the 100-char limit; the
      // concatenation is the exact _last_checkpoint content written below.
      val checkpointPath0 =
        "00000000000000000002.checkpoint.6374b053-df23-479b-b2cf-c9c550132b49.json"
      val sidecar1 =
        "00000000000000000002.checkpoint.0000000001.0000000002." +
          "bd1885fd-6ec0-4370-b0f5-43b5162fd4de.parquet"
      val sidecar2 =
        "00000000000000000002.checkpoint.0000000002.0000000002." +
          "0a8d73ee-aa83-49d0-9583-c99db75b89b2.parquet"
      val schemaString =
        """{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",""" +
          """\"nullable\":true,\"metadata\":{}}]}"""
      val content =
        """{"version":2,"size":9,"sizeInBytes":19554,"numOfAddFiles":4,"v2Checkpoint":{""" +
          s""""path":"$checkpointPath0",""" +
          """"sizeInBytes":891,"modificationTime":1714496115810,"nonFileActions":[{"protocol":{""" +
          """"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],""" +
          """"writerFeatures":["v2Checkpoint","appendOnly","invariants"]}},{"metaData":{""" +
          """"id":"8a390218-e4ee-4341-b6de-4920e27d3f78","format":{"provider":"parquet",""" +
          s""""options":{}},"schemaString":"$schemaString","partitionColumns":[],""" +
          """"configuration":{"delta.checkpointInterval":"2","delta.checkpointPolicy":"v2"},""" +
          """"createdTime":1714496114564}},{"checkpointMetadata":{"version":2}}],""" +
          s""""sidecarFiles":[{"path":"$sidecar1","sizeInBytes":9367,""" +
          """"modificationTime":1714496115780},""" +
          s"""{"path":"$sidecar2","sizeInBytes":9296,"modificationTime":1714496115788}]},""" +
          """"checksum":"d09f95a326aab562c60d415a32ddd216"}"""
      writeFile(path, content)

      val actual = fsClient.readWholeFileAsUtf8(path)
      assert(actual == content)
      // The escaped schemaString survives verbatim (no unescaping / byte loss).
      assert(actual.contains("""\"type\":\"struct\""""))
      // And it parses as valid JSON with the expected shape.
      val node = JsonUtils.mapper().readTree(actual)
      assert(node.get("version").asInt() == 2)
      assert(node.get("v2Checkpoint").get("nonFileActions").size() == 3)
      assert(node.get("v2Checkpoint").get("sidecarFiles").size() == 2)
    }
  }

  test("readWholeFileAsUtf8 reads back a _last_checkpoint written by Checkpointer") {
    withTempDir { tempDir =>
      val logPath = new KernelPath(tempDir.getAbsolutePath, "_delta_log")
      fsClient.mkdirs(logPath.toString)
      val checkpointer = new Checkpointer(logPath)

      // Write a real _last_checkpoint via the production write path.
      val cpm = new CheckpointMetaData(2L, 6L, Optional.of(java.lang.Long.valueOf(4L)))
      checkpointer.writeLastCheckpointFile(defaultEngine, cpm)

      // The JSON the writer serializes (writeJsonFileAtomically serializes each row via rowToJson,
      // then writes it as a line, i.e. with a trailing newline).
      val expectedJson = JsonUtils.rowToJson(cpm.toRow())

      // readWholeFileAsUtf8 returns the file's bytes verbatim (no trimming), so the whole file is
      // the serialized JSON plus the writer's trailing newline.
      val checkpointPath =
        new KernelPath(logPath, Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString
      val actual = fsClient.readWholeFileAsUtf8(checkpointPath)
      assert(actual.trim == expectedJson)
      assert(actual == expectedJson + "\n")

      // And it round-trips back through the parser to the same object.
      val roundTripped = checkpointer.readLastCheckpointFile(defaultEngine)
      assert(roundTripped.isPresent)
      assert(roundTripped.get().version == 2L)
      assert(roundTripped.get().size == 6L)
      assert(roundTripped.get().parts == Optional.of(java.lang.Long.valueOf(4L)))
    }
  }
}
