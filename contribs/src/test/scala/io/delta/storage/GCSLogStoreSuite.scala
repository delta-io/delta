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

import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.delta.{FakeFileSystem, FakeGCSFileSystem, LogStoreSuiteBase}

class GCSLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[GCSLogStore].getName

  testHadoopConf(
    expectedErrMsg = ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false

  test("gcs write should happen in a new thread") {
    withTempDir { tempDir =>
      // Use `FakeGCSFileSystem` to verify we write in the correct thread.
      withSQLConf(
          "fs.gs.impl" -> classOf[FakeGCSFileSystem].getName,
          "fs.gs.impl.disable.cache" -> "true") {
        val store = createLogStore(spark)
        store.write(
          new Path(s"gs://${tempDir.getCanonicalPath}", "1.json"),
          Iterator("foo"),
          overwrite = false,
          sessionHadoopConf)
      }
    }
  }
}
