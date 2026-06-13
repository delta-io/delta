package io.delta.storage.internal

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FilterFileSystem, LocalFileSystem, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite

class S3LogStoreUtilTest extends AnyFunSuite {

  test("unwrapFilterFileSystem returns the same instance when not wrapped") {
    val conf = new Configuration()
    val rawFs = new RawLocalFileSystem()
    rawFs.initialize(new URI("file:///"), conf)
    assert(S3LogStoreUtil.unwrapFilterFileSystem(rawFs) eq rawFs)
  }

  test("unwrapFilterFileSystem unwraps a single layer of FilterFileSystem") {
    val conf = new Configuration()
    val rawFs = new RawLocalFileSystem()
    rawFs.initialize(new URI("file:///"), conf)
    val wrapped = new FilterFileSystem(rawFs) {}
    assert(S3LogStoreUtil.unwrapFilterFileSystem(wrapped) eq rawFs)
  }

  test("unwrapFilterFileSystem unwraps multiple layers of FilterFileSystem") {
    val conf = new Configuration()
    val rawFs = new RawLocalFileSystem()
    rawFs.initialize(new URI("file:///"), conf)
    val inner = new FilterFileSystem(rawFs) {}
    val outer = new FilterFileSystem(inner) {}
    assert(S3LogStoreUtil.unwrapFilterFileSystem(outer) eq rawFs)
  }

  test("keyBefore") {
    assert("a" == S3LogStoreUtil.keyBefore("b"))
    assert("aa/aa" == S3LogStoreUtil.keyBefore("aa/ab"))
    assert(Seq(1.toByte, 1.toByte)
       == S3LogStoreUtil.keyBefore(new String(Seq(1.toByte, 2.toByte).toArray)).getBytes.toList)
  }

  test("keyBefore with emojis") {
    assert("♥a" == S3LogStoreUtil.keyBefore("♥b"))
  }

  test("keyBefore with zero bytes") {
    assert("abc" == S3LogStoreUtil.keyBefore("abc\u0000"))
  }

  test("keyBefore with empty key") {
    assert(null == S3LogStoreUtil.keyBefore(""))
  }
}
