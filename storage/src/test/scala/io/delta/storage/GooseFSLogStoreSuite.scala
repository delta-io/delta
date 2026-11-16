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

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite

/**
 * GooseFS's Hadoop {@code FileSystem.rename} returns false when the destination file already
 * exists and leaves that file unchanged. These tests use a local filesystem with that behavior.
 */
class GooseFSLogStoreSuite extends AnyFunSuite {

  private val scheme = RenameReturnsFalseFileSystem.scheme

  private def newConf(): Configuration = {
    val conf = new Configuration()
    conf.set(s"fs.$scheme.impl", classOf[RenameReturnsFalseFileSystem].getName)
    conf.set(s"fs.$scheme.impl.disable.cache", "true")
    conf
  }

  test("rename conflict, temp cleanup, and list order") {
    RenameReturnsFalseFileSystem.renames.clear()
    val conf = newConf()
    val root = Files.createTempDirectory("goosefs-logstore").toFile
    val store = new GooseFSLogStore(conf)
    try {
      val logDir = new Path(s"$scheme://${root.getCanonicalPath}/_delta_log")
      val fs = logDir.getFileSystem(conf)
      assert(fs.mkdirs(logDir))

      val commit = new Path(logDir, "00000000000000000001.json")
      assert(store.isPartialWriteVisible(commit, conf))

      store.write(commit, Iterator("winner").asJava, false, conf)
      assert(store.read(commit, conf).asScala.toSeq == Seq("winner"))
      assert(RenameReturnsFalseFileSystem.renames.nonEmpty)
      RenameReturnsFalseFileSystem.renames.foreach { case (src, dst) =>
        assert(src.getParent == dst.getParent)
      }

      // writeWithRename checks exists() before rename. Hide that one check so the
      // second writer reaches rename, which returns false and leaves the winner intact.
      val renamesBeforeConflict = RenameReturnsFalseFileSystem.renames.size
      RenameReturnsFalseFileSystem.hideNextExists += commit.toString
      val conflict = intercept[FileAlreadyExistsException] {
        store.write(commit, Iterator("loser").asJava, false, conf)
      }
      assert(RenameReturnsFalseFileSystem.renames.size == renamesBeforeConflict + 1)
      assert(RenameReturnsFalseFileSystem.hideNextExists.isEmpty)
      assert(conflict.getMessage.contains(commit.toString))
      assert(store.read(commit, conf).asScala.toSeq == Seq("winner"))

      val namesAfterConflict = fs.listStatus(logDir).map(_.getPath.getName).toSeq
      assert(!namesAfterConflict.exists(_.startsWith(".00000000000000000001.json.")))
      assert(namesAfterConflict.contains("00000000000000000001.json"))

      val listedTmp = new Path(
        logDir, ".00000000000000000007.json.8c0e4f2a-6b31-4d5e-9a77-1f0c3b6d2e84.tmp")
      val out = fs.create(listedTmp, false)
      out.write("partial\n".getBytes(StandardCharsets.UTF_8))
      out.close()
      store.write(
        new Path(logDir, "00000000000000000008.json"),
        Iterator("next").asJava,
        false,
        conf)

      val listed = store.listFrom(listedTmp, conf).asScala.map(_.getPath.getName).toSeq
      assert(listed == listed.sorted)
      assert(listed.contains(listedTmp.getName))
      assert(listed.contains("00000000000000000008.json"))

      store.write(commit, Iterator("replaced").asJava, true, conf)
      assert(store.read(commit, conf).asScala.toSeq == Seq("replaced"))
    } finally {
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(root)
    }
  }
}

/**
 * Local filesystem whose {@code rename} returns false when the destination is an existing file.
 */
class RenameReturnsFalseFileSystem extends RawLocalFileSystem {
  override def getScheme: String = RenameReturnsFalseFileSystem.scheme
  override def getUri: URI = RenameReturnsFalseFileSystem.uri

  override def exists(path: Path): Boolean = {
    val hide = RenameReturnsFalseFileSystem.hideNextExists.synchronized {
      val index = RenameReturnsFalseFileSystem.hideNextExists.indexOf(path.toString)
      if (index >= 0) {
        RenameReturnsFalseFileSystem.hideNextExists.remove(index)
        true
      } else {
        false
      }
    }
    if (hide) false else super.exists(path)
  }

  override def rename(src: Path, dst: Path): Boolean = {
    RenameReturnsFalseFileSystem.renames.synchronized {
      RenameReturnsFalseFileSystem.renames += ((src, dst))
    }
    if (exists(dst) && !getFileStatus(dst).isDirectory) {
      return false
    }
    super.rename(src, dst)
  }
}

object RenameReturnsFalseFileSystem {
  val scheme = "gfsnoop"
  val uri: URI = URI.create(s"$scheme:///")
  val renames: ArrayBuffer[(Path, Path)] = ArrayBuffer.empty
  // Next exists() for these path strings reports the file as missing.
  val hideNextExists: ArrayBuffer[String] = ArrayBuffer.empty
}
