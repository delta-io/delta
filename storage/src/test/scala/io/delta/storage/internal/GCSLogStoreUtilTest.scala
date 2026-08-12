package io.delta.storage.internal

import java.time.{Instant, OffsetDateTime, ZoneOffset}

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class GCSLogStoreUtilTest extends AnyFunSuite {
  private val parent = new Path("gs://bucket/tbl/_delta_log")

  test("pathToKey strips the leading slash") {
    assert("tbl/_delta_log/00.json"
      == GCSLogStoreUtil.pathToKey(new Path("gs://bucket/tbl/_delta_log/00.json")))
    assert("tbl/_delta_log" == GCSLogStoreUtil.pathToKey(parent))
  }

  test("toFileStatus for a file blob") {
    val updated = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1723500000000L), ZoneOffset.UTC)
    val status = GCSLogStoreUtil.toFileStatus(
      "tbl/_delta_log/00000000000000000010.json", false, java.lang.Long.valueOf(42L),
      updated, parent)
    assert(status.getPath == new Path("gs://bucket/tbl/_delta_log/00000000000000000010.json"))
    assert(!status.isDirectory)
    assert(status.getLen == 42L)
    assert(status.getModificationTime == 1723500000000L)
  }

  test("toFileStatus for a directory pseudo-blob strips the trailing slash") {
    val status = GCSLogStoreUtil.toFileStatus(
      "tbl/_delta_log/_staged_commits/", true, null, null, parent)
    assert(status.getPath == new Path("gs://bucket/tbl/_delta_log/_staged_commits"))
    assert(status.isDirectory)
    assert(status.getLen == 0L)
    assert(status.getModificationTime == 0L)
  }

  test("toFileStatus tolerates null size and mtime on file blobs") {
    val status = GCSLogStoreUtil.toFileStatus(
      "tbl/_delta_log/00000000000000000010.json", false, null, null, parent)
    assert(status.getLen == 0L)
    assert(status.getModificationTime == 0L)
  }

  test("gcsListFromArray throws UnsupportedOperationException for non-gs paths") {
    val p = new Path("s3a://bucket/_delta_log/x.json")
    assertThrows[UnsupportedOperationException] {
      GCSLogStoreUtil.gcsListFromArray(p, p.getParent)
    }
    val relative = new Path("_delta_log/x.json")
    assertThrows[UnsupportedOperationException] {
      GCSLogStoreUtil.gcsListFromArray(relative, relative.getParent)
    }
  }
}
