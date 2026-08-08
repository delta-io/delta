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

  // The fast listFrom request must be delimited (single-directory), so it never recurses
  // into `_staged_commits/`. The prefix must end in "/" or the delimiter groups nothing.
  test("buildListObjectsV2Request is a delimited single-directory listing") {
    val req = S3LogStoreUtil.buildListObjectsV2Request(
      "bucket",
      "tbl/_delta_log", // parentKey as produced by pathToKey: no trailing slash
      "tbl/_delta_log/00000000000000000003.json",
      1000)

    assert("/" == req.delimiter())
    assert("tbl/_delta_log/" == req.prefix())
    assert(req.prefix().endsWith("/"))
    assert("bucket" == req.bucket())
    assert(1000 == req.maxKeys())
    assert(S3LogStoreUtil.keyBefore("tbl/_delta_log/00000000000000000003.json") ==
      req.startAfter())
  }
}
