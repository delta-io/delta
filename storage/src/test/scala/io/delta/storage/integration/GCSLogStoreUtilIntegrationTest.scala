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

import java.io.FileNotFoundException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.storage.GCSLogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, Tag}
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
 * Everything — the setup writes, the listings under test, and the listFrom calls — goes
 * through gcs-connector's GoogleHadoopFileSystem with its regular authentication (e.g.
 * Application Default Credentials), i.e. exactly the identity GCSLogStore uses in production;
 * no separate client or credentials are involved. The suite constructs GCSLogStore with
 * delta.enableFastGCSListFrom=true and exercises listFrom end to end, with
 * fs.gs.list.max.items.per.call=2 so that pagination and the successor-based resume are
 * exercised with a handful of objects, and asserts the startOffset values actually passed to
 * the connector to prove the narrowing is server-side.
 */
class GCSLogStoreUtilIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {
  private val runIntegrationTests: Boolean =
    Option(System.getenv("GCS_LOG_STORE_UTIL_TEST_ENABLED")).exists(_.toBoolean)
  private val bucket = System.getenv("GCS_LOG_STORE_UTIL_TEST_BUCKET")
  private val testRunUID =
    System.getenv("GCS_LOG_STORE_UTIL_TEST_RUN_UID") // Prefix for all GCS keys in the current run

  private val maxKeysPerPage = 2

  /** The NUL character used by the successor-based page resume. */
  private val Nul: String = 0.toChar.toString

  private def newConf(fastListFrom: Boolean): Configuration = {
    val conf = new Configuration()
    conf.set("fs.gs.impl", classOf[RecordingGoogleHadoopFileSystem].getName)
    conf.setBoolean("fs.gs.impl.disable.cache", true)
    conf.setInt("fs.gs.list.max.items.per.call", maxKeysPerPage)
    conf.setBoolean("delta.enableFastGCSListFrom", fastListFrom)
    // The connector's default (COMPUTE_ENGINE) only works on GCE; the ADC chain works both
    // there and on developer machines with `gcloud auth application-default login`.
    conf.set("fs.gs.auth.type", "APPLICATION_DEFAULT")
    conf
  }

  private lazy val conf = newConf(fastListFrom = true)
  private lazy val store = new GCSLogStore(conf)
  private lazy val fs: FileSystem = new Path(s"gs://$bucket/").getFileSystem(conf)

  private def touch(key: String): Unit = fs.create(new Path(s"gs://$bucket/$key"), true).close()

  private def key(table: String, version: Int): String =
    s"$testRunUID/$table/_delta_log/%020d.json".format(version)

  private def path(table: String, version: Int): Path =
    new Path(s"gs://$bucket/${key(table, version)}")

  /**
   * Sorts immediately after every "$table/_delta_log/..." key ('0' > '/'), so listings
   * deterministically terminate on it instead of drifting into unrelated bucket keys.
   */
  private def sentinel(table: String): String = s"$testRunUID/$table/_delta_log0.end"

  private val integrationTestTag = Tag("IntegrationTest")

  def integrationTest(name: String)(testFun: => Any): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)

  private def listedKeys(from: Path): Seq[String] = {
    RecordingGoogleHadoopFileSystem.startingFromKeys.clear()
    store.listFrom(from, conf).asScala
      .map(s => s.getPath.toUri.getPath.stripPrefix("/")).toSeq
  }

  private def setupTable(table: String, numKeys: Int): Unit = {
    (1 to numKeys).foreach(v => touch(key(table, v)))
    touch(sentinel(table))
  }

  integrationTest("fast listFrom lists the tail, inclusive, ordered, across page boundaries") {
    val table = "paging"
    setupTable(table, numKeys = 7)

    // Full tail: 7 files at 2 keys/page.
    assert(listedKeys(path(table, 1)) == (1 to 7).map(key(table, _)))
    // Pages: [v1 v2], [v3 v4], [v5 v6], [v7 sentinel] -> exactly 4 requests, each resuming
    // from the NUL successor of the last listed key: server-side narrowing, server-side resume.
    assert(RecordingGoogleHadoopFileSystem.startingFromKeys.toSeq == Seq(
      key(table, 1),
      key(table, 2) + Nul,
      key(table, 4) + Nul,
      key(table, 6) + Nul))

    // Mid-tail: only the tail is listed (2 files -> a single request).
    assert(listedKeys(path(table, 6)) == (6 to 7).map(key(table, _)))
    assert(RecordingGoogleHadoopFileSystem.startingFromKeys.toSeq == Seq(
      key(table, 6),
      key(table, 7) + Nul))
  }

  integrationTest("listing past the latest version returns an empty iterator") {
    val table = "past-latest"
    setupTable(table, numKeys = 2)
    assert(listedKeys(path(table, 5)).isEmpty)
  }

  integrationTest("missing _delta_log throws FileNotFoundException") {
    val table = "never-created"
    assertThrows[FileNotFoundException] {
      store.listFrom(path(table, 1), conf)
    }
  }

  integrationTest("files under subdirectories of _delta_log are not listed") {
    val table = "staged"
    setupTable(table, numKeys = 3)
    touch(s"$testRunUID/$table/_delta_log/_staged_commits/%020d.uuid.json".format(9))
    assert(listedKeys(path(table, 1)) == (1 to 3).map(key(table, _)))
  }

  integrationTest("fast and default listFrom return the same files") {
    val table = "parity"
    setupTable(table, numKeys = 5)

    val fast = listedKeys(path(table, 2))

    val slowConf = newConf(fastListFrom = false)
    val slowStore = new GCSLogStore(slowConf)
    val slow = slowStore.listFrom(path(table, 2), slowConf).asScala
      .filter(!_.isDirectory) // the default path may also return subdirectory entries
      .map(s => s.getPath.toUri.getPath.stripPrefix("/")).toSeq

    assert(fast == slow)
  }

  integrationTest("results carry real object metadata") {
    val table = "metadata"
    setupTable(table, numKeys = 1)
    val statuses: Seq[FileStatus] = {
      RecordingGoogleHadoopFileSystem.startingFromKeys.clear()
      store.listFrom(path(table, 1), conf).asScala.toSeq
    }
    assert(statuses.map(_.getPath.toUri.getPath.stripPrefix("/")) == Seq(key(table, 1)))
    assert(statuses.forall(!_.isDirectory))
    assert(statuses.forall(_.getModificationTime > 0))
  }

  override def afterAll(): Unit = {
    try {
      if (runIntegrationTests && bucket != null && testRunUID != null && testRunUID.nonEmpty) {
        fs.delete(new Path(s"gs://$bucket/$testRunUID"), true)
      }
    } finally {
      super.afterAll()
    }
  }
}

/**
 * The production GoogleHadoopFileSystem with one addition: it records the start offsets passed
 * to listStatusStartingFrom, so the tests can assert that the narrowing really happens
 * server-side with the expected parameters.
 */
class RecordingGoogleHadoopFileSystem
  extends com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem {

  override def listStatusStartingFrom(hadoopPath: Path): Array[FileStatus] = {
    val k = {
      val p = hadoopPath.toUri.getPath
      if (p.startsWith("/")) p.substring(1) else p
    }
    RecordingGoogleHadoopFileSystem.startingFromKeys += k
    super.listStatusStartingFrom(hadoopPath)
  }
}

object RecordingGoogleHadoopFileSystem {
  val startingFromKeys: ArrayBuffer[String] = ArrayBuffer.empty
}
