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
import org.apache.spark.sql.delta.util.FileNames

class ExternalLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[MemoryLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme \"fake\"",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true"
  )

  test("single write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), false)
      val entry = MemoryLogStore.hashMap.get(path);
      assert(entry != null)
      assert(entry.complete);
    }
  }

  test("double write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), false)
      assert(MemoryLogStore.hashMap.containsKey(path))
      assertThrows[java.nio.file.FileSystemException] {
        store.write(path, Iterator("foo", "bar"), false)
      }
    }
  }

  test("overwrite") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), false)
      assert(MemoryLogStore.hashMap.containsKey(path))
      store.write(path, Iterator("foo", "bar"), true)
      assert(MemoryLogStore.hashMap.containsKey(path))
    }
  }

  test("recovery") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      FailingFileSystem.failOnSuffix = Some("00000000000000000000.json")
      withTempDir { tempDir =>
        val store = createLogStore(spark)
        val path = FileNames.deltaFile(
          new Path(new Path(s"failing:${tempDir.getCanonicalPath}"), "_delta_log"),
          0
        )
        store.write(path, Iterator("foo", "bar"), false)
        val entry = MemoryLogStore.hashMap.get(path)
        assert(entry != null)
        assert(!entry.complete)
        assert(entry.tempPath.nonEmpty)

        assertThrows[java.io.FileNotFoundException] {
          store.read(path)
        }
        val contents = store.read(entry.absoluteTempPath()).toList

        FailingFileSystem.failOnSuffix = None
        val files = store.listFrom(path.toString)
        val entry2 = MemoryLogStore.hashMap.get(path)
        assert(entry2 != null)
        assert(entry2.complete)
        assert(store.read(entry2.absoluteJsonPath()).toList == contents)
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
    hashMap.clear()
  }

  override protected def getLatestDbEntry(tablePath: Path): Option[LogEntry] = {
    hashMap.asScala.iterator
      .filter(r => r._2.tablePath == tablePath)
      .toList
      .sortBy(r => r._1)
      .reverse
      .find(r => true)
      .map(r => r._2)
  }

  override protected def getDbEntry(tablePath: Path, path: Path): Option[LogEntry] = {
    hashMap.asScala.get(path)
  }

  override protected def putDbEntry(
      logEntry: LogEntry,
      overwrite: Boolean = false
  ): Unit = {

    logDebug(
      s"WriteExternalCache: ${logEntry.absoluteJsonPath} (overwrite=$overwrite)"
    )

    if (overwrite) {
      hashMap.put(logEntry.absoluteJsonPath, logEntry)
    } else {
      val curr_val = hashMap.putIfAbsent(logEntry.absoluteJsonPath, logEntry);
      if (curr_val != null) {
        throw new java.nio.file.FileAlreadyExistsException(
          s"TransactionLog exists ${logEntry.absoluteJsonPath}"
        )
      }
    }
  }
}

object MemoryLogStore {
  val hashMap = new ConcurrentHashMap[Path, LogEntry]()
}

class TestLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration
) extends MemoryLogStore(sparkConf, hadoopConf) {}

class FailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FailingFileSystem.scheme

  override def getUri: URI = FailingFileSystem.uri

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {

    FailingFileSystem.failOnSuffix match {
      case Some(suffix) =>
        if (path.toString.endsWith(suffix)) {
          throw new java.nio.file.FileSystemException("fail")
        }
      case None => ;
    }
    super.create(path, overwrite)
  }
}

object FailingFileSystem {
  private val scheme = "failing"
  private val uri: URI = URI.create(s"$scheme:///")

  var failOnSuffix: Option[String] = None
}
