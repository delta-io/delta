package io.delta.storage.internal

import org.apache.hadoop.fs.{FilterFileSystem, Path, RawLocalFileSystem}
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

  test("unwrap peels FilterFileSystem chains to the raw delegate") {
    val raw = new RawLocalFileSystem
    assert(S3LogStoreUtil.unwrap(raw) eq raw)
    assert(S3LogStoreUtil.unwrap(new FilterFileSystem(raw)) eq raw)
    assert(S3LogStoreUtil.unwrap(new FilterFileSystem(new FilterFileSystem(raw))) eq raw)
  }

  test("s3ListFromArray throws UnsupportedOperationException for non-S3A") {
    val raw = new RawLocalFileSystem
    val p = new Path("s3a://bucket/_delta_log/x.json")
    assertThrows[UnsupportedOperationException] {
      S3LogStoreUtil.s3ListFromArray(new FilterFileSystem(raw), p, p.getParent)
    }
    assertThrows[UnsupportedOperationException] {
      S3LogStoreUtil.s3ListFromArray(
        new FilterFileSystem(new FilterFileSystem(raw)), p, p.getParent)
    }
  }
}
