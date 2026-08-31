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

package io.delta.storage

import java.io.{File, FileNotFoundException}
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the GCSLogStore.listFrom override, exercising the branches that
 * PublicGCSLogStoreSuite (local file system, so always the default path) and the GCS
 * integration test (real GCS) cannot cover in regular CI: the FileNotFoundException probe,
 * empty-past-latest, the flag-off and local-fs fallthroughs, and the fallback when the fast
 * path fails to link. Uses [[FakeGoogleHadoopFileSystem]] registered as the gs:// scheme.
 */
class GCSLogStoreFastListFromSuite extends AnyFunSuite {
  private val logDir = "tbl/_delta_log"

  private def v(version: Int): String = f"$logDir/$version%020d.json"

  private def gs(key: String): Path = new Path(s"gs://bucket/$key")

  private def newConf(fastListFrom: Boolean): Configuration = {
    val conf = new Configuration()
    conf.set("fs.gs.impl", classOf[FakeGoogleHadoopFileSystem].getName)
    conf.setBoolean("fs.gs.impl.disable.cache", true)
    conf.setBoolean("delta.enableFastGCSListFrom", fastListFrom)
    conf
  }

  private def listedKeys(store: GCSLogStore, conf: Configuration, path: Path): Seq[String] =
    store.listFrom(path, conf).asScala
      .map(s => s.getPath.toUri.getPath.stripPrefix("/")).toSeq

  test("missing parent directory throws FileNotFoundException") {
    FakeGoogleHadoopFileSystem.reset((1 to 3).map(v))
    val conf = newConf(fastListFrom = true)
    val store = new GCSLogStore(conf)
    assertThrows[FileNotFoundException] {
      store.listFrom(gs("othertable/_delta_log/00000000000000000001.json"), conf)
    }
  }

  test("listing past the latest version returns an empty iterator") {
    FakeGoogleHadoopFileSystem.reset((1 to 3).map(v))
    val conf = newConf(fastListFrom = true)
    val store = new GCSLogStore(conf)
    assert(listedKeys(store, conf, gs(v(5))).isEmpty)
    // The fast path was really taken.
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.nonEmpty)
  }

  test("fast path lists the tail, ordered, inclusive of the requested version") {
    FakeGoogleHadoopFileSystem.reset((1 to 9).map(v), newPageSize = 4)
    val conf = newConf(fastListFrom = true)
    val store = new GCSLogStore(conf)
    assert(listedKeys(store, conf, gs(v(4))) == (4 to 9).map(v))
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.nonEmpty)
    assert(FakeGoogleHadoopFileSystem.listStatusCalls.isEmpty)
  }

  test("fast path matches the default implementation's files on the same fixture") {
    val keys = (1 to 9).map(v) ++ Seq(s"$logDir/_staged_commits/", "tbl/data/part-00000")
    FakeGoogleHadoopFileSystem.reset(keys, newPageSize = 3)

    val fastConf = newConf(fastListFrom = true)
    val fast = listedKeys(new GCSLogStore(fastConf), fastConf, gs(v(4)))

    val slowConf = newConf(fastListFrom = false)
    val slowStore = new GCSLogStore(slowConf)
    val slowFiles = slowStore.listFrom(gs(v(4)), slowConf).asScala
      .filter(!_.isDirectory) // the default path also returns subdirectory entries
      .map(s => s.getPath.toUri.getPath.stripPrefix("/")).toSeq

    assert(fast == slowFiles)
  }

  test("flag off uses the default implementation and never calls the fast API") {
    FakeGoogleHadoopFileSystem.reset((1 to 3).map(v))
    val conf = newConf(fastListFrom = false)
    val store = new GCSLogStore(conf)
    assert(listedKeys(store, conf, gs(v(2))) == (2 to 3).map(v))
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.isEmpty)
    assert(FakeGoogleHadoopFileSystem.listStatusCalls.nonEmpty)
  }

  test("local file system uses the default implementation even with the flag on") {
    FakeGoogleHadoopFileSystem.reset(Seq.empty)
    val tempDir = Files.createTempDirectory("gcs-log-store-suite").toFile
    try {
      val log = new File(tempDir, "_delta_log")
      assert(log.mkdir())
      (1 to 3).foreach { i =>
        Files.write(new File(log, f"$i%020d.json").toPath, "{}".getBytes("UTF-8"))
      }
      val conf = newConf(fastListFrom = true)
      val store = new GCSLogStore(conf)
      val listed = store
        .listFrom(new Path(s"file://${log.getAbsolutePath}/${"%020d".format(2)}.json"), conf)
        .asScala.map(_.getPath.getName).toSeq
      assert(listed == (2 to 3).map(i => f"$i%020d.json"))
      assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.isEmpty)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("falls back to the default implementation when the fast path fails to link") {
    FakeGoogleHadoopFileSystem.reset((1 to 3).map(v))
    FakeGoogleHadoopFileSystem.maybeThrowOnListStartingFrom =
      Some(new NoSuchMethodError("listStatusStartingFrom"))
    val conf = newConf(fastListFrom = true)
    val store = new GCSLogStore(conf)

    // First call: attempts the fast path, catches the LinkageError, falls back.
    assert(listedKeys(store, conf, gs(v(2))) == (2 to 3).map(v))
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.size == 1)
    assert(FakeGoogleHadoopFileSystem.listStatusCalls.size == 1)

    // Second call: the failure is remembered, the fast path is not attempted again.
    assert(listedKeys(store, conf, gs(v(2))) == (2 to 3).map(v))
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.size == 1)
    assert(FakeGoogleHadoopFileSystem.listStatusCalls.size == 2)
  }

  private def deleteRecursively(dir: File): Unit = {
    Option(dir.listFiles()).foreach(_.foreach { f =>
      if (f.isDirectory) deleteRecursively(f) else f.delete()
    })
    dir.delete()
  }
}
