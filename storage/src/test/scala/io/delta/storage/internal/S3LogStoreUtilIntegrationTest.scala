package io.delta.storage.internal

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
    System.getenv("S3_LOG_STORE_UTIL_TEST_ENABLED").toBoolean
  private val bucket = System.getenv("S3_LOG_STORE_UTIL_TEST_BUCKET")
  private val keyPrefix = System.getenv("S3_LOG_STORE_UTIL_TEST_KEY_PREFIX")
  private val fs = new S3AFileSystem()
  private val configuration = new Configuration()
  configuration.set( // for local testing only
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider"
  )
  fs.initialize(new URI(s"s3a://$bucket"), configuration)

  private val file = Paths.get("/tmp/tmp.json").toFile

  private def touch(key: String): UploadInfo =
    fs.putObject(fs.newPutObjectRequest(key, fs.newObjectMetadata(), file))

  private def key(table: String, version: Int): String =
    s"$keyPrefix/$table/_delta_log/%020d.json".format(version)

  private def path(table: String, version: Int): Path =
    new Path(s"s3a://$bucket/${key(table, version)}")

  private def version(path: Path): Int
  = path.getName.takeWhile(_ != '.').toInt

  private val integrationTestTag = Tag("IntegrationTest")

  def integrationTest(name: String)(testFun: => Any)(implicit pos: source.Position): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)(pos)

  integrationTest("setup delta logs") {
    val uploads = Seq(
      touch(s"$keyPrefix/empty/some.json"),
      touch(s"$keyPrefix/small/_delta_log/%020d.json".format(1)),
      touch(s"$keyPrefix/small/_before/some.json"),
      touch(s"$keyPrefix/small/_right_after/some.json")) ++
      (1 to 10).map(v => touch(s"$keyPrefix/medium/_delta_log/%020d.json".format(v))) ++
      (1 to 1000).map(v => touch(s"$keyPrefix/large/_delta_log/%020d.json".format(v))) ++
      (1 to 10000).map(v => touch(s"$keyPrefix/xlarge/_delta_log/%020d.json".format(v)))
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
