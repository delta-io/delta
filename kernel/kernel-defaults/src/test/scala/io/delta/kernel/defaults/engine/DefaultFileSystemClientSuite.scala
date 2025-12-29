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

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.utils.TestUtils

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
}
