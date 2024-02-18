package io.delta.kernel.defaults.client

import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getTestResourceFilePath
import io.delta.kernel.defaults.utils.TestUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileNotFoundException
import scala.collection.mutable.ArrayBuffer

class DefaultFileSystemClientSuite extends AnyFunSuite with TestUtils {

  val fsClient = new DefaultFileSystemClient(new Configuration())

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
    intercept[FileNotFoundException] {
      fsClient.resolvePath("/non-existentfileTable/01.json")
    }
  }
}
