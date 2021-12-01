/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import collection.JavaConverters._

import java.io.{File, IOException}
import java.net.URI
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.sql.delta.{FakeFileSystem, LogStoreSuiteBase}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import java.nio.file.FileSystemException

// TODO: make this more generic
class ExternalLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[MemoryLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme \"fake\"",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  test("single write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val file = new File(tempDir, "test1");
      val path = new Path(s"file:${file.getCanonicalPath}")
      store.write(path, Iterator("foo", "bar"), false)
      val entry = MemoryLogStore.hash_map.get(path);
      assert(entry != null)
      assert(entry.isComplete);
    }
  }

  test("double write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val file = new File(tempDir, "test1");
      val path = new Path(s"file:${file.getCanonicalPath}")
      store.write(path, Iterator("foo", "bar"), false)
      assert(MemoryLogStore.hash_map.containsKey(path))
      assertThrows[java.nio.file.FileSystemException] {
        store.write(path, Iterator("foo", "bar"), false)
      }
    }
  }

  test("overwrite") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val file = new File(tempDir, "test1");
      val path = new Path(s"file:${file.getCanonicalPath}")
      store.write(path, Iterator("foo", "bar"), false)
      assert(MemoryLogStore.hash_map.containsKey(path))
      store.write(path, Iterator("foo", "bar"), true)
      assert(MemoryLogStore.hash_map.containsKey(path))
    }
  }

  test("recovery") {
    withSQLConf("fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true") {
        FailingFileSystem.failOnSuffix = "test1"
        withTempDir { tempDir =>
          val store = createLogStore(spark)
          val file = new File(tempDir, "test1");
          val path = new Path(s"failing:${file.getCanonicalPath}")
          store.write(path, Iterator("foo", "bar"), false)
          val entry = MemoryLogStore.hash_map.get(path)
          assert(entry != null)
          assert(!entry.isComplete)

          assertThrows[java.io.FileNotFoundException] {
            store.read(path)
          }
          val contents = store.read(entry.tempPath).toList

          FailingFileSystem.failOnSuffix = null
          val files = store.listFrom(s"failing:${file.getCanonicalPath}")
          val entry2 = MemoryLogStore.hash_map.get(path)
          assert(entry2 != null)
          assert(entry2.isComplete)
          assert(store.read(entry2.path).toList == contents)
        }
      }
  }

  test("listFrom exceptions") {
    val store = createLogStore(spark)
    assertThrows[java.io.FileNotFoundException] {
      store.listFrom("/non-existing-path/with-parent")
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

class MemoryLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import MemoryLogStore._;

  override def invalidateCache(): Unit = {
    hash_map.clear()
  }

  override protected def listFromCache(
    fs: FileSystem,
    resolvedPath: Path): Iterator[LogEntryMetadata] = {
    val pathKey = getPathKey(resolvedPath)
    hash_map
      .asScala
      .iterator
      .filter {
        case (path, _) => path.getParent == pathKey.getParent && path.getName >= pathKey.getName
      }
      .map {
        case (_, logEntry) => logEntry
      }
  }

  override protected def writeCache(
    fs: FileSystem,
    logEntry: LogEntryMetadata,
    overwrite: Boolean = false): Unit = {

    logDebug(s"WriteExternalCache: ${logEntry.path} (overwrite=$overwrite)")

    if (overwrite) {
      hash_map.put(logEntry.path, logEntry)
    } else {
      val curr_val = hash_map.putIfAbsent(logEntry.path, logEntry);
      if (curr_val != null) {
        throw new java.nio.file.FileAlreadyExistsException(
          s"TransactionLog exists ${logEntry.path}"
        )
      }
    }
  }

  override protected def lookupInCache(
      fs: FileSystem,
      resolvedPath: Path): Option[LogEntryMetadata] = {
    if (hash_map.containsKey(resolvedPath)) {
      Some(hash_map.get(resolvedPath))
    } else {
      None
    }
  }

  override protected def deleteFromCacheAllOlderThan(
      fs: FileSystem,
      parentPath: Path,
      expirationTime: Long): Iterator[Path] = {
    val result = hash_map
      .asScala
      .iterator
      .filter { case (path, entry) =>
        path.getParent == parentPath && entry.modificationTime <= expirationTime
      }
      .map {
        case (_, logEntry) => logEntry
      }

    result.foreach { entry => hash_map.remove(entry.path) }

    result.map { entry => entry.tempPath }
  }
}

object MemoryLogStore {
  // Map from LogEntryMetadata.path => LogEntryMetadata
  val hash_map = new ConcurrentHashMap[Path, LogEntryMetadata]()
}

class FailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FailingFileSystem.scheme

  override def getUri: URI = FailingFileSystem.uri

  override def create(
    path: Path,
    overwrite: Boolean): FSDataOutputStream = {
      if (FailingFileSystem.failOnSuffix != null
        && path.toString.endsWith(FailingFileSystem.failOnSuffix)) {
          throw new java.nio.file.FileSystemException("fail")
      }
      super.create(path, overwrite)
  }
}

object FailingFileSystem {
  private val scheme = "failing"
  private val uri: URI = URI.create(s"$scheme:///")

  var failOnSuffix: String = null
}
