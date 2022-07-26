package io.delta.storage.integration

import io.delta.storage.internal.{FileNameUtils, S3LogStoreUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.{S3AFileSystem, UploadInfo}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalactic.source

import java.net.URI
import java.nio.file.Paths
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
  private val configuration = new Configuration()
  configuration.set( // for local testing only
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider"
  )

  private val file = Paths.get(getClass.getClassLoader.getResource("sample.json").getPath).toFile

  private def touch(key: String): UploadInfo =
    fs.putObject(fs.newPutObjectRequest(key, fs.newObjectMetadata(), file))

  private def key(table: String, version: Int): String =
    s"$testRunUID/$table/_delta_log/%020d.json".format(version)

  private def path(table: String, version: Int): Path =
    new Path(s"s3a://$bucket/${key(table, version)}")

  private def version(path: Path): Long = FileNameUtils.deltaVersion(path)

  private val integrationTestTag = Tag("IntegrationTest")

  def integrationTest(name: String)(testFun: => Any): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)

  integrationTest("setup delta logs") {
    val uploads = Seq(
      touch(s"$testRunUID/empty/some.json"),
      touch(s"$testRunUID/small/_delta_log/%020d.json".format(1))) ++
      (1 to 10).map(v => touch(s"$testRunUID/medium/_delta_log/%020d.json".format(v))) ++
      (1 to 1000).map(v => touch(s"$testRunUID/large/_delta_log/%020d.json".format(v))) ++
      (1 to 10000).map(v => touch(s"$testRunUID/xlarge/_delta_log/%020d.json".format(v)))
    uploads.foreach(_.getUpload.waitForUploadResult())
  }

  integrationTest("empty") {
    val resolvedPath = path("empty", 0)
    val response = S3LogStoreUtil.s3ListFrom(fs, resolvedPath, resolvedPath.getParent)
    assert(response.isEmpty)
  }

  integrationTest("small") {
    Seq(0, 1, 2, 3).foreach(v => {
      val resolvedPath = path("small", v)
      val response = S3LogStoreUtil.s3ListFrom(fs, resolvedPath, resolvedPath.getParent)
      assert((max(1, v) to 1) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("medium") {
    Seq(1, 2, 3, 5, 10, 11, 12).foreach(v => {
      val resolvedPath = path("medium", v)
      val response = S3LogStoreUtil.s3ListFrom(fs, resolvedPath, resolvedPath.getParent)
      assert((max(1, v) to 10) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("large") {
    Seq(0, 1, 3, 5, 500, 998, 999, 1000, 1001).foreach(v => {
      val resolvedPath = path("large", v)
      val response = S3LogStoreUtil.s3ListFrom(fs, resolvedPath, resolvedPath.getParent)
      assert((max(1, v) to 1000) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("xlarge, also verify number of list requests") {
    Seq(0, 1, 999, 2999, 5001, 9998, 9999, 10000, 10001).foreach(v => {
      val startCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      val resolvedPath = path("xlarge", v)
      val response = S3LogStoreUtil.s3ListFrom(fs, resolvedPath, resolvedPath.getParent)
      val endCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      assert(endCount - startCount ==
        max(round(ceil((10000 - v) / 1000.0)).toInt, 1))
      assert((max(1, v) to 10000) == response.map(r => version(r.getPath)).toSeq)
    })
  }
}
