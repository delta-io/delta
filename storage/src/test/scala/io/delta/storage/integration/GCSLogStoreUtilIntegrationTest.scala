/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage.integration

import io.delta.storage.internal.{FileNameUtils, GCSLogStoreUtil}

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.math.ceil
import scala.math.max

import com.google.api.client.http.HttpExecuteInterceptor
import com.google.api.client.http.HttpRequest
import com.google.api.client.http.HttpRequestInitializer
import com.google.cloud.ServiceOptions
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import org.apache.hadoop.fs.Path
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

/**
 * These integration tests are executed by setting the
 * environment variables
 * GCS_LOG_STORE_UTIL_TEST_BUCKET=some-gcs-bucket-name
 * GCS_LOG_STORE_UTIL_TEST_RUN_UID=some-uuid-for-test-run
 * and running
 * python run-integration-tests.py --gcs-log-store-util-only
 *
 * Alternatively you can set the environment variables
 * GCS_LOG_STORE_UTIL_TEST_ENABLED=true
 * GCS_LOG_STORE_UTIL_TEST_BUCKET=some-gcs-bucket-name
 * GCS_LOG_STORE_UTIL_TEST_RUN_UID=some-uuid-for-test-run
 * and run the tests in this suite using your preferred
 * test execution mechanism (e.g., the IDE or sbt)
 *
 * GCS_LOG_STORE_UTIL_TEST_BUCKET is the name of the GCS bucket used for the test.
 * GCS_LOG_STORE_UTIL_TEST_RUN_UID is a prefix for all keys used in the test.
 * This is useful for isolating multiple test runs.
 *
 * The tests authenticate via Application Default Credentials, the same way
 * GCSLogStoreUtil does in production.
 */
class GCSLogStoreUtilIntegrationTest extends AnyFunSuite {
  private val runIntegrationTests: Boolean =
    Option(System.getenv("GCS_LOG_STORE_UTIL_TEST_ENABLED")).exists(_.toBoolean)
  private val bucket = System.getenv("GCS_LOG_STORE_UTIL_TEST_BUCKET")
  private val testRunUID =
    System.getenv("GCS_LOG_STORE_UTIL_TEST_RUN_UID") // Prefix for all GCS keys in the current run

  private val maxKeys = 2

  private case class ListRequest(startOffset: String, prefix: String, delimiter: String)

  private val listRequestCount = new AtomicInteger(0)
  private val listRequests = ArrayBuffer.empty[ListRequest]

  /**
   * The GCS SDK exposes neither a request counter (the S3A IOStatistics equivalent) nor a
   * paging-size configuration (the fs.s3a.paging.maximum equivalent), so this transport
   * wrapper provides both: it counts objects.list requests, records the listing parameters
   * actually sent on the wire (to assert startOffset is really pushed down server-side),
   * and forces maxResults=maxKeys so that pagination kicks in with a handful of keys.
   */
  private class RecordingTransportOptions
    extends HttpTransportOptions(HttpTransportOptions.newBuilder()) {

    override def getHttpRequestInitializer(
        serviceOptions: ServiceOptions[_, _]): HttpRequestInitializer = {
      val base = super.getHttpRequestInitializer(serviceOptions)
      new HttpRequestInitializer {
        override def initialize(request: HttpRequest): Unit = {
          base.initialize(request)
          val previous = request.getInterceptor
          request.setInterceptor(new HttpExecuteInterceptor {
            override def intercept(req: HttpRequest): Unit = {
              if (previous != null) {
                previous.intercept(req)
              }
              // objects.list requests are GET /storage/v1/b/{bucket}/o
              if (req.getRequestMethod == "GET" && req.getUrl.getRawPath.endsWith("/o")) {
                req.getUrl.set("maxResults", Integer.valueOf(maxKeys))
                listRequestCount.incrementAndGet()
                listRequests += ListRequest(
                  startOffset = String.valueOf(req.getUrl.getFirst("startOffset")),
                  prefix = String.valueOf(req.getUrl.getFirst("prefix")),
                  delimiter = String.valueOf(req.getUrl.getFirst("delimiter")))
              }
            }
          })
        }
      }
    }
  }

  private lazy val storage: Storage = StorageOptions.newBuilder()
    .setTransportOptions(new RecordingTransportOptions)
    .build()
    .getService

  private def touch(key: String): Unit = {
    storage.create(BlobInfo.newBuilder(bucket, key).build())
  }

  private def key(table: String, version: Int): String =
    s"$testRunUID/$table/_delta_log/%020d.json".format(version)

  private def path(table: String, version: Int): Path =
    new Path(s"gs://$bucket/${key(table, version)}")

  private def version(path: Path): Long = FileNameUtils.deltaVersion(path)

  private val integrationTestTag = Tag("IntegrationTest")

  def integrationTest(name: String)(testFun: => Any): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)

  def testCase(testName: String, numKeys: Int): Unit = integrationTest(testName) {
    // Setup delta log
    (1 to numKeys).foreach(v => touch(key(testName, v)))

    // Check number of GCS requests and correct listing
    (1 to numKeys + 2).foreach(v => {
      val startCount = listRequestCount.get
      val resolvedPath = path(testName, v)
      val response =
        GCSLogStoreUtil.gcsListFrom(storage, bucket, resolvedPath, resolvedPath.getParent)
      // Check that we don't do more GCS list requests than necessary
      val numberOfKeysToList = numKeys - (v - 1)
      val optimalNumberOfListRequests =
        max(ceil(numberOfKeysToList / maxKeys.toDouble).toInt, 1)
      val actualNumberOfListRequests = listRequestCount.get - startCount
      assert(optimalNumberOfListRequests == actualNumberOfListRequests)
      // Check that the narrowing really happened server-side, on the first request
      // of this listing (follow-up pages repeat the same parameters).
      val firstRequest = listRequests(listRequests.size - actualNumberOfListRequests)
      assert(key(testName, v) == firstRequest.startOffset)
      assert(s"$testRunUID/$testName/_delta_log/" == firstRequest.prefix)
      assert("/" == firstRequest.delimiter)
      // Check that we get consecutive versions from v to the max version. The smallest version is 1
      assert((max(1, v) to numKeys) == response.map(r => version(r.getPath)).toSeq)
    })
  }

  integrationTest("setup empty delta log") {
    touch(s"$testRunUID/empty/some.json")
  }

  testCase("empty", 0)

  testCase("small", 1)

  testCase("medium", maxKeys)

  testCase("large", 10 * maxKeys)
}
