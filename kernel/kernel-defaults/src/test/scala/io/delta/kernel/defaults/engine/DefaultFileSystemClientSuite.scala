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

  test("list from file") {
    val basePath = fsClient.resolvePath(getTestResourceFilePath("json-files"))
    val listFrom = fsClient.resolvePath(getTestResourceFilePath("json-files/2.json"))

    val actListOutput = new ArrayBuffer[String]()
    val files = fsClient.listFrom(listFrom)
    try {
      fsClient.listFrom(listFrom).forEach(f => actListOutput += f.getPath)
    }
    finally if (files != null) {
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
}
