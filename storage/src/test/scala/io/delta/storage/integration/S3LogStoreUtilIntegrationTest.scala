package io.delta.storage.integration

import io.delta.storage.internal.{FileNameUtils, S3LogStoreUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import scala.math.max
import scala.math.ceil
import scala.math.round

class S3LogStoreUtilIntegrationTest extends AnyFunSuite {
  private val runIntegrationTests: Boolean =
    Option(System.getenv("S3_LOG_STORE_UTIL_TEST_ENABLED")).exists(_.toBoolean)
  private val bucket = System.getenv("S3_LOG_STORE_UTIL_TEST_BUCKET")
  private val testRunUID =
    System.getenv("S3_LOG_STORE_UTIL_TEST_RUN_UID") // Prefix for all S3 keys in the current run
  private lazy val fs: S3AFileSystem = {
    val fs = new S3AFileSystem()
    fs.initialize(new URI(s"s3a://$bucket"), configuration)
    fs
  }
  private val maxKeys = 2
  private val configuration = new Configuration()
  configuration.set( // for local testing only
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
  )
  configuration.set("fs.s3a.paging.maximum", maxKeys.toString)

  private def touch(key: String) {
    fs.create(new Path(s"s3a://$bucket/$key")).close()
  }

  private def key(table: String, version: Int): String =
    s"$testRunUID/$table/_delta_log/%020d.json".format(version)

  private def path(table: String, version: Int): Path =
    new Path(s"s3a://$bucket/${key(table, version)}")

  private def version(path: Path): Long = FileNameUtils.deltaVersion(path)

  private val integrationTestTag = Tag("IntegrationTest")

  def integrationTest(name: String)(testFun: => Any): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)

  integrationTest("setup delta logs") {
    touch(s"$testRunUID/empty/some.json")
    touch(s"$testRunUID/small/_delta_log/%020d.json".format(1))
    (1 to maxKeys).foreach(v => touch(s"$testRunUID/medium/_delta_log/%020d.json".format(v)))
    (1 to maxKeys * 10).foreach(v => touch(s"$testRunUID/large/_delta_log/%020d.json".format(v)))
  }

  integrationTest("empty") {
    val resolvedPath = path("empty", 0)
    val response = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
    assert(response.isEmpty)
  }

  integrationTest("small") {
    Seq(0, 1, 2, 3).foreach(v => {
      val resolvedPath = path("small", v)
      val response = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
      // Check that we get consecutive versions from v to the max version. The smallest version is 1
      assert((max(1, v) to 1) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("medium") {
    (1 to maxKeys).foreach(v => {
      val resolvedPath = path("medium", v)
      val response = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
      assert((max(1, v) to maxKeys) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("large, also verify number of list requests") {
    (1 to maxKeys * 10).foreach(v => {
      val startCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      val resolvedPath = path("large", v)
      val response = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
      val endCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      // Check that we don't do more S3 list requests than necessary
      assert(endCount - startCount ==
        max(round(ceil((maxKeys * 10 - (v - 1)) / maxKeys.toDouble)).toInt, 1))
      // Check that we get consecutive versions from v to the max version. The smallest version is 1
      assert((max(1, v) to maxKeys * 10) == response.map(r => version(r.getPath)).toSeq)
    })
  }
}
