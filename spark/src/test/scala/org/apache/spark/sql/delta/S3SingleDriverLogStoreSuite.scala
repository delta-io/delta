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

import scala.collection.JavaConverters._

import io.delta.storage.HDFSLogStore
import org.apache.spark.sql.delta.storage.LogStore
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
      val hackyStore = new HDFSLogStore(sessionHadoopConf)
      hackyStore.write(deltas(2), Iterator("foo").asJava, true, sessionHadoopConf)

      // we should see "foo" (FileSystem value) instead of "two" (cache value)
      assert(store.read(deltas(2), sessionHadoopConf).head == "foo")
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}
