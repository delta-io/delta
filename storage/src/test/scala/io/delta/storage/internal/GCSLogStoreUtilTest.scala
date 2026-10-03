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

package io.delta.storage.internal

import java.net.URI

import scala.collection.JavaConverters._

import io.delta.storage.FakeGoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FilterFileSystem, Path, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite

class GCSLogStoreUtilTest extends AnyFunSuite {
  private val logDir = "tbl/_delta_log"

  /** The NUL character used by the successor-based page resume. */
  private val Nul: String = 0.toChar.toString

  private def v(version: Int): String = f"$logDir/$version%020d.json"

  private def p(key: String): Path = new Path(s"gs://bucket/$key")

  private def newFakeFs(keys: Seq[String], pageSize: Int = 1000): FakeGoogleHadoopFileSystem = {
    FakeGoogleHadoopFileSystem.reset(keys, pageSize)
    val fs = new FakeGoogleHadoopFileSystem
    fs.initialize(new URI("gs://bucket/"), new Configuration())
    fs
  }

  private def listFrom(fs: FakeGoogleHadoopFileSystem, version: Int): Seq[String] =
    GCSLogStoreUtil.gcsListFrom(fs, p(v(version)), p(logDir))
      .asScala.map(s => GCSLogStoreUtil.pathToKey(s.getPath)).toSeq

  test("pathToKey strips the leading slash") {
    assert("tbl/_delta_log/00.json"
      == GCSLogStoreUtil.pathToKey(new Path("gs://bucket/tbl/_delta_log/00.json")))
    assert("tbl/_delta_log" == GCSLogStoreUtil.pathToKey(p(logDir)))
  }

  test("isGoogleHadoopFileSystem recognizes the connector, wrappers, and rejects others") {
    val fake = newFakeFs(Seq.empty)
    assert(GCSLogStoreUtil.isGoogleHadoopFileSystem(fake))

    val local = new RawLocalFileSystem()
    local.initialize(new URI("file:///"), new Configuration())
    assert(!GCSLogStoreUtil.isGoogleHadoopFileSystem(local))

    val wrapped = new FilterFileSystem(fake)
    assert(GCSLogStoreUtil.isGoogleHadoopFileSystem(wrapped))
  }

  test("gcsListFrom rejects file systems that are not a GoogleHadoopFileSystem") {
    val local = new RawLocalFileSystem()
    local.initialize(new URI("file:///"), new Configuration())
    assertThrows[UnsupportedOperationException] {
      GCSLogStoreUtil.gcsListFrom(local, p(v(1)), p(logDir))
    }
  }

  test("gcsListFrom rejects listing from a bucket root") {
    val fake = newFakeFs(Seq("x.json"))
    assertThrows[UnsupportedOperationException] {
      GCSLogStoreUtil.gcsListFrom(fake, new Path("gs://bucket/x.json"), new Path("gs://bucket/"))
    }
  }

  test("single page: inclusive of the start path, ordered, tail only") {
    val fake = newFakeFs((1 to 10).map(v) :+ "aaa/before.json")
    assert(listFrom(fake, 4) == (4 to 10).map(v))
    // The requested path itself is the first server-side offset.
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.head == v(4))
  }

  test("empty when listing past the latest version") {
    // Nothing at all after the offset.
    val fake = newFakeFs((1 to 3).map(v))
    assert(listFrom(fake, 5).isEmpty)

    // Only keys outside the parent directory after the offset ('data' sorts after '_delta_log').
    val fake2 = newFakeFs((1 to 3).map(v) :+ "tbl/data/part-00000")
    assert(listFrom(fake2, 5).isEmpty)
  }

  test("pagination resumes from the NUL successor of the last listed key") {
    val fake = newFakeFs((1 to 7).map(v), pageSize = 3)
    assert(listFrom(fake, 1) == (1 to 7).map(v))
    // Page 1 raw: [v1 v2 v3]; resume at the NUL successor of v3: [v4 v5 v6]; then [v7];
    // then an empty page ends the listing.
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.toSeq == Seq(
      v(1),
      v(3) + Nul,
      v(6) + Nul,
      v(7) + Nul))
  }

  test("pages are fetched lazily as the consumer advances") {
    val fake = newFakeFs((1 to 7).map(v), pageSize = 3)
    val it = GCSLogStoreUtil.gcsListFrom(fake, p(v(1)), p(logDir))
    // The first page is fetched eagerly at construction.
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.size == 1)
    (1 to 3).foreach(_ => it.next())
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.size == 1)
    it.next() // element 4 needs page 2
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.size == 2)
  }

  test("a directory placeholder at a page boundary does not end the listing") {
    // Raw page 2 is [v2, placeholder]; the connector filters the placeholder AFTER cutting
    // the page, so the returned page is short. Files beyond the placeholder must still be
    // found via the successor resume.
    val placeholder = s"$logDir/zz-subdir/"
    val late = s"$logDir/zzz.json" // sorts after the placeholder
    // 'z' > digits, so order is: v1, v2, placeholder, late.
    val fake = newFakeFs(Seq(v(1), v(2), placeholder, late), pageSize = 2)
    assert(listFrom(fake, 1) == Seq(v(1), v(2), late))
  }

  test("a raw page consisting entirely of directory placeholders does not end the listing") {
    // The connector transparently continues past all-placeholder raw pages, so the file
    // beyond them must be listed; page size 2 makes [ph1, ph2] one full raw page.
    val ph1 = s"$logDir/za-subdir/"
    val ph2 = s"$logDir/zb-subdir/"
    val late = s"$logDir/zz.json"
    val fake = newFakeFs(Seq(v(1), v(2), ph1, ph2, late), pageSize = 2)
    assert(listFrom(fake, 1) == Seq(v(1), v(2), late))
  }

  test("stops at the first key outside the parent directory and fetches no further pages") {
    val keys = (1 to 3).map(v) ++ Seq("tbl/data/part-00000", "zzz/other.json")
    val fake = newFakeFs(keys, pageSize = 2)
    assert(listFrom(fake, 1) == (1 to 3).map(v))
    // No startOffset was ever positioned at or past the out-of-directory keys.
    assert(FakeGoogleHadoopFileSystem.listStartingFromCalls.forall(_ < "tbl/data/part-00000"))
  }

  test("entries under subdirectories of the parent are skipped, like fs.listStatus") {
    val staged = Seq(
      s"$logDir/_staged_commits/", // directory placeholder
      s"$logDir/_staged_commits/${"%020d".format(3)}.uuid1.json",
      s"$logDir/_staged_commits/${"%020d".format(4)}.uuid2.json")
    // '_' sorts after digits, so the staged entries are inside the listed tail.
    val fake = newFakeFs((1 to 3).map(v) ++ staged, pageSize = 2)
    assert(listFrom(fake, 1) == (1 to 3).map(v))
  }

  test("results carry the connector's FileStatus metadata") {
    val fake = newFakeFs(Seq(v(1)))
    val statuses: Seq[FileStatus] =
      GCSLogStoreUtil.gcsListFrom(fake, p(v(1)), p(logDir)).asScala.toSeq
    assert(statuses.map(_.getPath) == Seq(p(v(1))))
    assert(statuses.forall(!_.isDirectory))
    assert(statuses.forall(_.getLen > 0))
    assert(statuses.forall(_.getModificationTime > 0))
  }
}
