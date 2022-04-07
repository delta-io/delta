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

import java.net.URI

import org.apache.hadoop.fs._

import org.apache.spark.sql.delta.FakeFileSystem
import org.apache.spark.sql.delta.util.FileNames

class ExternalLogStoreSuite extends org.apache.spark.sql.delta.PublicLogStoreSuite {
  override protected val publicLogStoreClassName: String =
    classOf[MemoryLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme \"fake\"",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true"
  )

  test("single write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      val entry = MemoryLogStore.get(path);
      assert(entry != null)
      assert(entry.complete);
    }
  }

  test("double write") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      assertThrows[java.nio.file.FileSystemException] {
        store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      }
    }
  }

  test("overwrite") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      store.write(path, Iterator("foo", "bar"), overwrite = true, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
    }
  }

  test("recovery") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempDir { tempDir =>
        val store = createLogStore(spark)
        val path = FileNames.deltaFile(
          new Path(new Path(s"failing:${tempDir.getCanonicalPath}"), "_delta_log"),
          0
        )

        // fail to write to FileSystem when we try to commit 0000.json
        FailingFileSystem.failOnSuffix = Some(path.getName)

        // try and commit 0000.json
        store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)

        // check that entry was written to external store and that it doesn't exist in FileSystem
        val entry = MemoryLogStore.get(path)
        assert(entry != null)
        assert(!entry.complete)
        assert(entry.tempPath.nonEmpty)
        assertThrows[java.io.FileNotFoundException] {
          store.read(path)
        }

        // Now perform a `listFrom` read, which should fix the transaction log
        val contents = store.read(entry.absoluteTempPath()).toList
        FailingFileSystem.failOnSuffix = None
        store.listFrom(path.toString)
        val entry2 = MemoryLogStore.get(path)
        assert(entry2 != null)
        assert(entry2.complete)
        assert(store.read(entry2.absoluteFilePath()).toList == contents)
      }
    }
  }

  test("write to new Delta table but a DynamoDB entry for it already exists") {
    withTempDir { tempDir =>
      val store = createLogStore(spark)

      // write 0000.json
      val path = FileNames.deltaFile(new Path(new Path(tempDir.toURI), "_delta_log"), 0)
      store.write(path, Iterator("foo"), overwrite = false, sessionHadoopConf)

      // delete 0000.json from FileSystem
      val fs = path.getFileSystem(sessionHadoopConf)
      fs.delete(path, false)

      // try and write a new 0000.json, while the external store entry still exists
      val e = intercept[java.nio.file.FileSystemException] {
        store.write(path, Iterator("bar"), overwrite = false, sessionHadoopConf)
      }.getMessage

      val tablePath = path.getParent.getParent
      assert(e == s"Old entries for table ${tablePath} still exist in the external store")
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

/**
 * This utility enables failure simulation on file system.
 * Providing a matching suffix results in an exception being
 * thrown that allows to test file system failure scenarios.
 */
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
