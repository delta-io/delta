package io.delta.storage.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI

class S3LogStoreUtilTest extends AnyFunSuite {
  ignore("s3ListFrom") {
    val bucket = "some-bucket"
    val deltaLogKey = "some-delta-table/_delta_log/00000000000000001234.json"
    val resolvedPath = new Path(s"s3a://$bucket/$deltaLogKey")
    val fs = new S3AFileSystem()
    val configuration = new Configuration()
    configuration.set( // for local testing only
      "fs.s3a.aws.credentials.provider",
      "com.amazonaws.auth.profile.ProfileCredentialsProvider"
    )
    fs.initialize(new URI(s"s3a://$bucket"), configuration)
    val response = S3LogStoreUtil.s3ListFrom(
      fs,
      resolvedPath,
      resolvedPath.getParent
    )
    assert(response.head.getPath == resolvedPath)
    assert(response.last.getPath.getName == "_last_checkpoint")
    val entries = response.dropRight(1).map(_.getPath.getName.takeWhile(_ != '.').toInt)
    entries.foldLeft(entries.head)((x, y) => {
      assert((x == y) || (x + 1 == y))
      y
    })
  }

  test("keyBefore") {
    assert("a" == S3LogStoreUtil.keyBefore("b"))
    assert("aa/aa" == S3LogStoreUtil.keyBefore("aa/ab"))
    assert(Seq(1.toByte, 1.toByte)
       == S3LogStoreUtil.keyBefore(new String(Seq(1.toByte, 2.toByte).toArray)).getBytes.toList)
  }
}
