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

/**
 * These integration tests are executed by setting the
 * environment variables
 * S3_LOG_STORE_UTIL_TEST_BUCKET=some-s3-bucket-name
 * S3_LOG_STORE_UTIL_TEST_RUN_UID=some-uuid-for-test-run
 * and running
 * python run-integration-tests.py --s3-log-store-util-only
 *
 * Alternatively you can set the environment variables
 * S3_LOG_STORE_UTIL_TEST_ENABLED=true
 * S3_LOG_STORE_UTIL_TEST_BUCKET=some-s3-bucket-name
 * S3_LOG_STORE_UTIL_TEST_RUN_UID=some-uuid-for-test-run
 * and run the tests in this suite using your preferred
 * test execution mechanism (e.g., the IDE or sbt)
 *
 * S3_LOG_STORE_UTIL_TEST_BUCKET is the name of the S3 bucket used for the test.
 * S3_LOG_STORE_UTIL_TEST_RUN_UID is a prefix for all keys used in the test.
 * This is useful for isolating multiple test runs.
 */
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

  def testCase(testName: String, numKeys: Int): Unit = integrationTest(testName) {
    // Setup delta log
    (1 to numKeys).foreach(v => touch(s"$testRunUID/$testName/_delta_log/%020d.json".format(v)))

    // Check number of S3 requests and correct listing
    (1 to numKeys + 2).foreach(v => {
      val startCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      val resolvedPath = path(testName, v)
      val response = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
      val endCount = fs.getIOStatistics.counters().get("object_list_request") +
        fs.getIOStatistics.counters().get("object_continue_list_request")
      // Check that we don't do more S3 list requests than necessary
      val numberOfKeysToList = numKeys - (v - 1)
      val optimalNumberOfListRequests =
        max(ceil(numberOfKeysToList / maxKeys.toDouble).toInt, 1)
      val actualNumberOfListRequests = endCount - startCount
      assert(optimalNumberOfListRequests == actualNumberOfListRequests)
      // Check that we get consecutive versions from v to the max version. The smallest version is 1
      assert((max(1, v) to numKeys) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  def testNonRecursive(table: String): Unit = integrationTest(table) {
    // Setup delta log
    touch(s"$testRunUID/$table/_delta_log/%020d.json".format(1))
    // Setup data file
    touch(s"$testRunUID/$table/1.snappy.parquet")
    val resolvedPath = new Path(s"s3a://$bucket/$testRunUID/$table", "\u0000")
    val ListByStartAfter =
      S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, resolvedPath.getParent)
    val list = fs.listStatus(resolvedPath.getParent)
    assert(ListByStartAfter.size == list.size)
    ListByStartAfter.foreach(f => assert(list.contains(f)))
  }

  integrationTest("setup empty delta log") {
    touch(s"$testRunUID/empty/some.json")
  }

  testCase("empty", 0)

  testCase("small", 1)

  testCase("medium", maxKeys)

  testCase("large", 10 * maxKeys)

  testNonRecursive("non-recursive")
}
