/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.storage.{LogStore, S3LogStore}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

class S3LogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[S3LogStore].getName

  private def checkLogStoreList(store: LogStore, path: Path, expectedVersions: Seq[Int]): Unit = {
    assert(
      store.listFrom(path).map(f => FileNames.deltaVersion(f.getPath)).toSeq === expectedVersions)
  }

  private def checkFileSystemList(fs: FileSystem, path: Path, expectedVersions: Seq[Int]): Unit = {
    val fsList = fs.listStatus(path.getParent).filter(_.getPath.getName >= path.getName)
    assert(fsList.map(f => FileNames.deltaVersion(f.getPath)).sorted === expectedVersions)
  }

  testHadoopConf(
    "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  test("cache works") {
    withTempDir { dir =>
      val store = createLogStore(spark)
      val deltas =
        Seq(0, 1, 2, 3, 4).map(i => FileNames.deltaFile(new Path(dir.toURI), i))
      store.write(deltas(0), Iterator("zero"))
      store.write(deltas(1), Iterator("one"))
      store.write(deltas(2), Iterator("two"))

      // delete delta file 2 from file system
      val fs = new Path(dir.getCanonicalPath).getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(deltas(2), true)

      // file system listing doesn't see file 2
      checkFileSystemList(fs, deltas(0), Seq(0, 1))

      // can't re-write because cache says it still exists
      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(2), Iterator("two"))
      }

      // log store list still sees file 2 as it's cached
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2))

      // clear the cache
      store.invalidateCache()

      // log store list doesn't see file 2 anymore
      checkLogStoreList(store, deltas(0), Seq(0, 1))

      // write a new file 2
      store.write(deltas(2), Iterator("two"))

      // add a file 3 to cache only
      store.write(deltas(3), Iterator("three"))
      fs.delete(deltas(3), true)

      // log store listing returns a union of:
      // 1) file system listing: 0, 1, 2
      // 2) cache listing: 2, 3
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2, 3))
    }
  }

  test("cache works correctly when writing an initial log version") {
    withTempDir { dir =>
      val store = createLogStore(spark)
      val deltas =
        Seq(0, 1, 2).map(i => FileNames.deltaFile(new Path(dir.toURI), i))
      store.write(deltas(0), Iterator("log version 0"))
      store.write(deltas(1), Iterator("log version 1"))
      store.write(deltas(2), Iterator("log version 2"))

      val fs = new Path(dir.getCanonicalPath).getFileSystem(spark.sessionState.newHadoopConf())
      // delete all log files
      fs.delete(deltas(2), true)
      fs.delete(deltas(1), true)
      fs.delete(deltas(0), true)

      // can't write a new version 1 as it's in cache
      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(1), Iterator("new log version 1"))
      }

      // all three log files still in cache
      checkLogStoreList(store, deltas(0), Seq(0, 1, 2))

      // can write a new version 0 as it's the initial version of the log
      store.write(deltas(0), Iterator("new log version 0"))

      // writing a new initial version invalidates all files in that log
      checkLogStoreList(store, deltas(0), Seq(0))
    }
  }
}
