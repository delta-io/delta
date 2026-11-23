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

import java.io.FileNotFoundException
import java.nio.file.{Files, Path => JPath}
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.delta.storage.internal.GCSListing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FilterFileSystem, Path, RawLocalFileSystem}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Checks that GCSLogStore's server-side listing returns what the listStatus-based implementation
 * returns, on one fixture that covers the cases Delta relies on.
 *
 * The same tree is written to local disk. The reference is LocalLogStore (listStatus). The fast
 * path runs through GCSLogStore.listFrom with an in-memory stand-in for the connector's
 * listStatusStartingFrom: a flat, bucket-wide, key-ordered listing from an inclusive start, cut
 * into fixed-size raw pages, optionally dropping folder placeholders the way gcs-connector does.
 */
class GCSLogStoreListFromSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val NUL = "\u0000"
  private def v(n: Int): String = f"$n%020d"

  /** Fixture keys relative to the bucket root. A trailing "/" is an empty directory. */
  private val fixture: Seq[String] = {
    val log =
      (0 to 9).flatMap(n => Seq(s"${v(n)}.json", s"${v(n)}.crc")) ++
      Seq(s"${v(5)}.checkpoint.parquet", "_last_checkpoint", "_sidecars/s1.parquet") ++
      (0 until 300).map(i => f"_staged_commits/${v(10 + i)}.u$i%04d.json")
    log.map("t/_delta_log/" + _) ++ Seq(
      "t/day=2026-01-01/part-0.parquet",
      "t/day=2026-01-01/part-1.parquet",
      "t/day=2026-01-02/part-0.parquet",
      "t/_change_data/cdc-0.parquet",
      "t/_col=1/part-0.parquet",
      "t/.hidden/x.parquet",
      "t/metadata/v1.json",
      "t/emptydir/",
      "t/sub/a.txt",
      "t/sub/deep/b.txt",
      "t/sub.txt",
      "t/sub-x.txt",
      "t/part-root.parquet",
      "t/_SUCCESS",
      // Siblings that share the "t" prefix, and other keys after it in the bucket.
      "t2/_delta_log/00000000000000000000.json",
      "t_backup/x.parquet",
      "u/y.parquet")
  }

  private var root: JPath = _
  private var rootPath: Path = _
  private def rootPrefix: String = root.toString + "/"

  override def beforeAll(): Unit = {
    super.beforeAll()
    root = Files.createTempDirectory("gcs-listfrom").toRealPath()
    rootPath = new Path(root.toUri)
    fixture.foreach { key =>
      val target = root.resolve(key)
      if (key.endsWith("/")) {
        Files.createDirectories(target)
      } else {
        Files.createDirectories(target.getParent)
        Files.write(target, Array[Byte](1))
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      if (root != null) {
        Files.walk(root).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
      }
    } finally {
      super.afterAll()
    }
  }

  /** In-memory stand-in for GoogleHadoopFileSystem.listStatusStartingFrom. */
  private class FakeConnector(pageSize: Int, dropPlaceholders: Boolean)
      extends GCSListing.PageSource {
    val calls = new AtomicInteger()

    // (raw key as GCS would store it, isPlaceholder); keys are absolute local paths without the
    // leading "/", so they line up with the keys GCSListing derives from the paths it lists.
    private lazy val keys: Vector[(String, Boolean)] = {
      val prefix = root.toString.stripPrefix("/")
      fixture.map { k => (s"$prefix/$k", k.endsWith("/")) }.toVector.sortBy(_._1)
    }

    override def listStartingFrom(start: Path): Array[FileStatus] = {
      calls.incrementAndGet()
      val startKey = start.toUri.getPath.stripPrefix("/")
      var remaining = keys.dropWhile(_._1 < startKey)
      while (remaining.nonEmpty) {
        val (raw, rest) = remaining.splitAt(pageSize)
        val page = if (dropPlaceholders) raw.filterNot(_._2) else raw
        if (page.nonEmpty) {
          return page.map { case (key, placeholder) =>
            new FileStatus(
              if (placeholder) 0L else 1L, placeholder, 1, 1L, 0L,
              new Path("file", null, "/" + key.stripSuffix("/")))
          }.toArray
        }
        // The connector pages past raw pages that held only placeholders.
        remaining = rest
      }
      Array.empty
    }
  }

  private def conf(): Configuration = new Configuration()

  private def fastStore(c: Configuration, source: GCSListing.PageSource): GCSLogStore =
    new GCSLogStore(c) {
      override protected def fastListingSource(fs: FileSystem): Optional[GCSListing.PageSource] =
        Optional.of(source)
    }

  private def at(dir: String, start: String): Path = new Path(new Path(rootPath, dir), start)

  private def rel(s: FileStatus): (String, Boolean) =
    (s.getPath.toUri.getPath.stripPrefix(rootPrefix), s.isDirectory)

  private def run(it: java.util.Iterator[FileStatus]): Vector[(String, Boolean)] =
    it.asScala.map(rel).toVector

  private val vacuumLikeFilter = ListFilter.prefixes(
    Set("_", ".").asJava,
    Set("_", ".").asJava,
    Set("_change_data", "_delta_index", "_col=").asJava)

  private val pageSizes = Seq(1, 2, 3, 7, 5000)

  test("non-recursive _delta_log listing equals listStatus, for every start name and page size") {
    val c = conf()
    val starts = Seq(NUL, v(0), v(5), s"${v(7)}.json", "5", "_", "_staged_commits", "zzz")
    for (pageSize <- pageSizes; start <- starts) {
      val expected = run(new LocalLogStore(c).listFrom(at("t/_delta_log", start), c))
      val connector = new FakeConnector(pageSize, dropPlaceholders = true)
      val store = fastStore(c, connector)
      val clue = s"pageSize=$pageSize start=${start.replace(NUL, "NUL")}"
      assert(run(store.listFrom(at("t/_delta_log", start), c)) == expected, clue)
      assert(
        run(store.listFrom(at("t/_delta_log", start), false, ListFilter.NONE, c)) == expected,
        clue)
      assert(connector.calls.get() > 0, clue)
    }
  }

  test("subdirectories cost one request each instead of being paged through") {
    val c = conf()
    val connector = new FakeConnector(pageSize = 5, dropPlaceholders = true)
    val result = run(fastStore(c, connector).listFrom(at("t/_delta_log", v(8)), c))
    assert(result == Vector(
      (s"t/_delta_log/${v(8)}.crc", false), (s"t/_delta_log/${v(8)}.json", false),
      (s"t/_delta_log/${v(9)}.crc", false), (s"t/_delta_log/${v(9)}.json", false),
      ("t/_delta_log/_last_checkpoint", false),
      ("t/_delta_log/_sidecars", true),
      ("t/_delta_log/_staged_commits", true)))
    // 1: the 5 tail files, 2: _sidecars/, 3: _staged_commits/ (300 objects), 4: leaves the dir.
    // Paging through the subdirectories instead would take 60+ requests at this page size.
    assert(connector.calls.get() == 4)
  }

  test("non-recursive table root equals listStatus (same entries, key order)") {
    val c = conf()
    val expected = run(new LocalLogStore(c).listFrom(at("t", NUL), c))
    for (pageSize <- pageSizes) {
      val got = run(fastStore(c, new FakeConnector(pageSize, dropPlaceholders = false))
        .listFrom(at("t", NUL), c))
      assert(got.sorted == expected.sorted, s"pageSize=$pageSize")
      // Only documented difference: key order puts "sub-x.txt" and "sub.txt" before the
      // directory "sub" ('-' and '.' sort below '/'), a name sort puts "sub" first.
      def withoutSub(xs: Vector[(String, Boolean)]) = xs.filterNot(_._1.startsWith("t/sub"))
      assert(withoutSub(got) == withoutSub(expected), s"pageSize=$pageSize")
    }
  }

  test("connector placeholder filtering hides only empty directories") {
    val c = conf()
    val expected = run(new LocalLogStore(c).listFrom(at("t", NUL), c))
    val got = run(fastStore(c, new FakeConnector(7, dropPlaceholders = true))
      .listFrom(at("t", NUL), c))
    assert(expected.toSet -- got.toSet == Set(("t/emptydir", true)))
    assert(got.toSet subsetOf expected.toSet)
  }

  test("recursive listing equals recursive listStatus, with and without a VACUUM-like filter") {
    val c = conf()
    for (filter <- Seq(ListFilter.NONE, vacuumLikeFilter); pageSize <- pageSizes) {
      val expected =
        run(new LocalLogStore(c).listFrom(at("t", NUL), true, filter, c))
      val got = run(fastStore(c, new FakeConnector(pageSize, dropPlaceholders = false))
        .listFrom(at("t", NUL), true, filter, c))
      val clue = s"filter=$filter pageSize=$pageSize"
      assert(got.sorted == expected.sorted, clue)
      // Every directory is returned before anything under it.
      got.zipWithIndex.foreach { case ((path, _), i) =>
        val parent = path.substring(0, path.lastIndexOf('/'))
        if (parent != "t") {
          assert(got.take(i).contains((parent, true)), s"$clue: $parent before $path")
        }
      }
    }
  }

  test("VACUUM-like filter skips hidden subtrees and keeps the exceptions") {
    val c = conf()
    val connector = new FakeConnector(pageSize = 5, dropPlaceholders = false)
    val got = run(fastStore(c, connector).listFrom(at("t", NUL), true, vacuumLikeFilter, c))
      .map(_._1).toSet
    assert(!got.exists(_.startsWith("t/_delta_log")))
    assert(!got.exists(_.startsWith("t/.hidden")))
    assert(!got.contains("t/_SUCCESS"))
    assert(got.contains("t/_change_data/cdc-0.parquet"))
    assert(got.contains("t/_col=1/part-0.parquet"))
    assert(got.contains("t/day=2026-01-02/part-0.parquet"))
    assert(got.contains("t/sub/deep/b.txt"))
    // _delta_log (321 objects) is skipped with one request rather than paged through.
    assert(connector.calls.get() < 15)
  }

  test("recursive start name bounds only the top level") {
    val c = conf()
    val expected = run(new LocalLogStore(c).listFrom(at("t", "day=2026-01-02"), true,
      ListFilter.NONE, c))
    for (pageSize <- pageSizes) {
      val got = run(fastStore(c, new FakeConnector(pageSize, dropPlaceholders = false))
        .listFrom(at("t", "day=2026-01-02"), true, ListFilter.NONE, c))
      assert(got.sorted == expected.sorted, s"pageSize=$pageSize")
    }
    assert(!expected.exists(_._1.startsWith("t/day=2026-01-01")))
  }

  test("listing never leaves the directory, even for siblings that share its prefix") {
    val c = conf()
    val got = run(fastStore(c, new FakeConnector(2, dropPlaceholders = false))
      .listFrom(at("t", NUL), true, ListFilter.NONE, c))
    assert(got.nonEmpty)
    assert(got.forall(_._1.startsWith("t/")))
  }

  test("a missing directory throws FileNotFoundException") {
    val c = conf()
    val store = fastStore(c, new FakeConnector(5, dropPlaceholders = true))
    intercept[FileNotFoundException](store.listFrom(at("nope", "x"), c))
    intercept[FileNotFoundException](store.listFrom(at("nope", "x"), true, ListFilter.NONE, c))
    intercept[FileNotFoundException](new LocalLogStore(c).listFrom(at("nope", "x"), c))
  }

  test("delta.enableFastGCSListFrom=false uses listStatus") {
    val c = conf()
    c.setBoolean(GCSLogStore.FAST_LIST_FROM_CONF, false)
    val connector = new FakeConnector(5, dropPlaceholders = true)
    val store = fastStore(c, connector)
    val expected = run(new LocalLogStore(c).listFrom(at("t/_delta_log", v(3)), c))
    assert(run(store.listFrom(at("t/_delta_log", v(3)), c)) == expected)
    assert(run(store.listFrom(at("t", NUL), true, vacuumLikeFilter, c)).sorted ==
      run(new LocalLogStore(c).listFrom(at("t", NUL), true, vacuumLikeFilter, c)).sorted)
    assert(connector.calls.get() == 0)
  }

  test("a plain GCSLogStore on the local file system uses listStatus") {
    val c = conf()
    val expected = run(new LocalLogStore(c).listFrom(at("t/_delta_log", v(3)), c))
    assert(run(new GCSLogStore(c).listFrom(at("t/_delta_log", v(3)), c)) == expected)
  }

  test("forFileSystem finds listStatusStartingFrom, also behind FilterFileSystem") {
    class FakeGoogleHadoopFileSystem extends RawLocalFileSystem {
      def listStatusStartingFrom(start: Path): Array[FileStatus] = Array.empty
    }
    assert(GCSListing.forFileSystem(new FakeGoogleHadoopFileSystem).isPresent)
    assert(GCSListing.forFileSystem(new FilterFileSystem(new FakeGoogleHadoopFileSystem)).isPresent)
    assert(!GCSListing.forFileSystem(new RawLocalFileSystem).isPresent)
  }

  test("ListFilter.prefixes: keep prefixes override ignore prefixes, for directories only") {
    assert(vacuumLikeFilter.skipDirectory("_delta_log"))
    assert(vacuumLikeFilter.skipDirectory(".hidden"))
    assert(!vacuumLikeFilter.skipDirectory("_change_data"))
    assert(!vacuumLikeFilter.skipDirectory("_col=1"))
    assert(!vacuumLikeFilter.skipDirectory("day=2026-01-01"))
    assert(vacuumLikeFilter.skipFile("_SUCCESS"))
    assert(!vacuumLikeFilter.skipFile("part-0.parquet"))
    assert(!ListFilter.NONE.skipFile("_x") && !ListFilter.NONE.skipDirectory("_x"))
  }

  test("default recursive listFrom returns every file once, directories first") {
    val c = conf()
    val got = run(new LocalLogStore(c).listFrom(at("t", NUL), true, ListFilter.NONE, c))
    val files = fixture.filter(k => k.startsWith("t/") && !k.endsWith("/"))
    assert(got.filterNot(_._2).map(_._1).sorted == files.sorted.toVector)
    assert(got.map(_._1).distinct.size == got.size)
  }
}
