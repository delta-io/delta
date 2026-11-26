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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.io.Source

import io.delta.storage.{GCSLogStore, ListFilter}
import io.delta.storage.internal.{GCSListing, RecursiveListing}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Read-only check of GCSLogStore's server-side listing against a real _delta_log through the real
 * gcs-connector. Lists and probes only; writes nothing.
 *
 * Runs only when DELTA_GCS_IT_LOG_PATH is set, e.g.
 *   DELTA_GCS_IT_LOG_PATH=gs://bucket/path/to/table/_delta_log \
 *     build/sbt "storage/testOnly *GCSLogStoreRealGcsIntegrationTest"
 * Authenticates with Application Default Credentials.
 *
 * The reference is one fs.listStatus of the directory, which is what the previous listFrom did on
 * every call. The table may be written while the test runs, so comparisons ignore versions newer
 * than the reference listing.
 */
class GCSLogStoreRealGcsIntegrationTest extends AnyFunSuite {

  private val logPathOpt = sys.env.get("DELTA_GCS_IT_LOG_PATH")
  private def v(n: Long): String = f"$n%020d"

  private def conf(fast: Boolean): Configuration = {
    val c = new Configuration()
    c.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    c.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    c.set("fs.gs.auth.type", "APPLICATION_DEFAULT")
    c.setBoolean(GCSLogStore.FAST_LIST_FROM_CONF, fast)
    c
  }

  private def entry(s: FileStatus): (String, Boolean) = (s.getPath.getName, s.isDirectory)

  private def ms(startNanos: Long): Long = (System.nanoTime() - startNanos) / 1000000

  private def versionOf(name: String): Option[Long] =
    if (name.length >= 20 && name.take(20).forall(_.isDigit)) Some(name.take(20).toLong) else None

  test("server-side listFrom equals listStatus on a real _delta_log") {
    assume(logPathOpt.isDefined, "set DELTA_GCS_IT_LOG_PATH=gs://<bucket>/<table>/_delta_log")
    val logDir = new Path(logPathOpt.get)
    val fastConf = conf(fast = true)
    val fs = logDir.getFileSystem(fastConf)
    val source = GCSListing.forFileSystem(fs)
    assert(source.isPresent, s"${fs.getClass.getName} has no listStatusStartingFrom")

    val checkpointVersion = {
      val in = fs.open(new Path(logDir, "_last_checkpoint"))
      try {
        val json = Source.fromInputStream(in, "UTF-8").mkString
        """"version"\s*:\s*(\d+)""".r.findFirstMatchIn(json).get.group(1).toLong
      } finally in.close()
    }

    // Reference: the whole directory, once.
    val refStart = System.nanoTime()
    val reference = fs.listStatus(logDir).toVector
    val refMs = ms(refStart)
    val maxRefVersion = reference.flatMap(s => versionOf(s.getPath.getName)).max
    // Ignore versions committed after (or while) the reference listing ran.
    val stableUpTo = maxRefVersion - 3
    def comparable(e: (String, Boolean)): Boolean = versionOf(e._1).forall(_ <= stableUpTo)

    // scalastyle:off println
    println(s"[gcs-it] $logDir")
    println(s"[gcs-it] reference listStatus: ${reference.size} entries in $refMs ms " +
      s"(last checkpoint $checkpointVersion, max version $maxRefVersion)")

    val starts = Seq(
      v(checkpointVersion - 2), v(checkpointVersion), v(stableUpTo), "_", "_staged_commits",
      "5", "zzzz")
    for (start <- starts) {
      val expected = reference.map(entry)
        .filter(e => e._1.compareTo(start) >= 0 && comparable(e)).sortBy(_._1)

      val calls = new AtomicInteger()
      val counting: GCSListing.PageSource = p => { calls.incrementAndGet(); source.get.listStartingFrom(p) }
      val t = System.nanoTime()
      val viaUtil = GCSListing.listFrom(counting, fs.makeQualified(new Path(logDir, start)), false,
        ListFilter.NONE).asScala.map(entry).toVector
      val utilMs = ms(t)

      val viaStore = new GCSLogStore(fastConf).listFrom(new Path(logDir, start), fastConf)
        .asScala.map(entry).toVector

      println(f"[gcs-it] start=$start%-22s fast: ${viaUtil.size}%5d entries, " +
        f"${calls.get}%3d requests, $utilMs%6d ms")
      assert(viaUtil.filter(comparable) == expected, s"start=$start (util)")
      assert(viaStore.filter(comparable) == expected, s"start=$start (GCSLogStore)")
      assert(viaUtil.filterNot(comparable).forall(e => versionOf(e._1).exists(_ > stableUpTo)))
    }

    // Recursive, skipping _staged_commits: fast path vs the default implementation on the same
    // store with server-side listing turned off.
    val skipStaged = ListFilter.prefixes(null, Set("_staged_commits").asJava, null)
    val recStart = new Path(logDir, v(checkpointVersion - 2))
    val t1 = System.nanoTime()
    val fastRec = new GCSLogStore(fastConf).listFrom(recStart, true, skipStaged, fastConf)
      .asScala.map(s => (s.getPath.toString, s.isDirectory)).toVector
    val fastRecMs = ms(t1)
    val slowConf = conf(fast = false)
    val t2 = System.nanoTime()
    val slowRec = RecursiveListing.listFrom(new GCSLogStore(slowConf), recStart, true, skipStaged,
      slowConf).asScala.map(s => (s.getPath.toString, s.isDirectory)).toVector
    val slowRecMs = ms(t2)
    def stable(xs: Vector[(String, Boolean)]) =
      xs.filter(e => versionOf(new Path(e._1).getName).forall(_ <= stableUpTo)).sorted
    println(s"[gcs-it] recursive, skip _staged_commits: fast ${fastRec.size} entries in " +
      s"$fastRecMs ms; previous implementation ${slowRec.size} entries in $slowRecMs ms")
    // scalastyle:on println
    assert(stable(fastRec) == stable(slowRec))
    assert(!fastRec.exists(_._1.contains("/_staged_commits/")))
  }
}
