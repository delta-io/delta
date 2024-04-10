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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.storage.{HDFSLogStore, LogStore, S3SingleDriverLogStore}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait S3SingleDriverLogStoreSuiteBase extends LogStoreSuiteBase {

  private def checkLogStoreList(
      store: LogStore,
      path: Path,
      expectedVersions: Seq[Int],
      hadoopConf: Configuration): Unit = {
    assert(store.listFrom(path, hadoopConf).map(FileNames.deltaVersion).toSeq === expectedVersions)
  }

  private def checkFileSystemList(fs: FileSystem, path: Path, expectedVersions: Seq[Int]): Unit = {
    val fsList = fs.listStatus(path.getParent).filter(_.getPath.getName >= path.getName)
    assert(fsList.map(FileNames.deltaVersion).sorted === expectedVersions)
  }

  testHadoopConf(
    ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  test("file system has priority over cache") {
    withTempDir { dir =>
      val store = createLogStore(spark)
      val deltas = Seq(0, 1, 2).map(i => FileNames.unsafeDeltaFile(new Path(dir.toURI), i))
      store.write(deltas(0), Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(deltas(1), Iterator("one"), overwrite = false, sessionHadoopConf)
      store.write(deltas(2), Iterator("two"), overwrite = false, sessionHadoopConf)

      // delete delta file 2 and its checksum from file system
      val fs = new Path(dir.getCanonicalPath).getFileSystem(sessionHadoopConf)
      val delta2CRC = FileNames.checksumFile(new Path(dir.toURI), 2)
      fs.delete(deltas(2), true)
      fs.delete(delta2CRC, true)

      // magically create a different version of file 2 in the FileSystem only
      val hackyStore = new HDFSLogStore(sparkConf, sessionHadoopConf)
      hackyStore.write(deltas(2), Iterator("foo"), overwrite = true, sessionHadoopConf)

      // we should see "foo" (FileSystem value) instead of "two" (cache value)
      assert(store.read(deltas(2), sessionHadoopConf).head == "foo")
    }
  }

  test("cache works") {
    withTempDir { dir =>
      val store = createLogStore(spark)
      val deltas =
        Seq(0, 1, 2, 3, 4).map(i => FileNames.unsafeDeltaFile(new Path(dir.toURI), i))
      store.write(deltas(0), Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(deltas(1), Iterator("one"), overwrite = false, sessionHadoopConf)
      store.write(deltas(2), Iterator("two"), overwrite = false, sessionHadoopConf)

      // delete delta file 2 from file system
      val fs = new Path(dir.getCanonicalPath).getFileSystem(sessionHadoopConf)
      fs.delete(deltas(2), true)

      // file system listing doesn't see file 2
      checkFileSystemList(fs, deltas(0), Seq(0, 1))

      // can't re-write because cache says it still exists
      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(2), Iterator("two"), overwrite = false, sessionHadoopConf)
      }

      // log store list still sees file 2 as it's cached
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2), sessionHadoopConf)

      if (canInvalidateCache) {
        // clear the cache
        store.invalidateCache()

        // log store list doesn't see file 2 anymore
        checkLogStoreList(store, deltas(0), Seq(0, 1), sessionHadoopConf)

        // write a new file 2
        store.write(deltas(2), Iterator("two"), overwrite = false, sessionHadoopConf)
      }

      // add a file 3 to cache only
      store.write(deltas(3), Iterator("three"), overwrite = false, sessionHadoopConf)
      fs.delete(deltas(3), true)

      // log store listing returns a union of:
      // 1) file system listing: 0, 1, 2
      // 2a) cache listing - canInvalidateCache=true: 2, 3
      // 2b) cache listing - canInvalidateCache=false: 0, 1, 2, 3
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2, 3), sessionHadoopConf)
    }
  }

  test("cache works correctly when writing an initial log version") {
    withTempDir { rootDir =>
      val dir = new File(rootDir, "_delta_log")
      dir.mkdir()
      val store = createLogStore(spark)
      val deltas =
        Seq(0, 1, 2).map(i => FileNames.unsafeDeltaFile(new Path(dir.toURI), i))
      store.write(deltas(0), Iterator("log version 0"), overwrite = false, sessionHadoopConf)
      store.write(deltas(1), Iterator("log version 1"), overwrite = false, sessionHadoopConf)
      store.write(deltas(2), Iterator("log version 2"), overwrite = false, sessionHadoopConf)

      val fs = new Path(dir.getCanonicalPath).getFileSystem(sessionHadoopConf)
      // delete all log files
      fs.delete(deltas(2), true)
      fs.delete(deltas(1), true)
      fs.delete(deltas(0), true)

      // can't write a new version 1 as it's in cache
      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(1), Iterator("new log version 1"), overwrite = false, sessionHadoopConf)
      }

      // all three log files still in cache
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2), sessionHadoopConf)

      // can write a new version 0 as it's the initial version of the log
      store.write(deltas(0), Iterator("new log version 0"), overwrite = false, sessionHadoopConf)

      // writing a new initial version invalidates all files in that log
      checkLogStoreList(store, deltas(0), Seq(0), sessionHadoopConf)
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false

  /**
   * S3SingleDriverLogStore.scala can invalidate cache
   * S3SingleDriverLogStore.java cannot invalidate cache
   */
  protected def canInvalidateCache: Boolean
}

class S3SingleDriverLogStoreSuite extends S3SingleDriverLogStoreSuiteBase {
  override val logStoreClassName: String = classOf[S3SingleDriverLogStore].getName

  override protected def canInvalidateCache: Boolean = true
}
