package io.delta.storage.internal

import org.scalatest.funsuite.AnyFunSuite

class S3LogStoreUtilTest extends AnyFunSuite {
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
