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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.test.SharedSparkSession

class CheckpointsSuite extends QueryTest with SharedSparkSession {

  protected override def sparkConf = {
    super.sparkConf.set("spark.delta.logStore.gs.impl", classOf[LocalLogStore].getName)
  }

  ignore("isGCSPath") {
    val conf = new Configuration()
    assert(Checkpoints.isGCSPath(conf, new Path("gs://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("non-gs://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("/foo")))
    // Set the default file system and verify we can detect it
    conf.set("fs.defaultFS", "gs://foo/")
    conf.set("fs.gs.impl", classOf[FakeGSCFileSystem].getName)
    conf.set("fs.gs.impl.disable.cache", "true")
    assert(Checkpoints.isGCSPath(conf, new Path("/foo")))
  }

  test("writeCheckpoint on GCS") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      withSQLConf(
          "fs.gs.impl" -> classOf[FakeGSCFileSystem].getName,
          "fs.gs.impl.disable.cache" -> "true") {
        DeltaLog.clearCache()
        val gsPath = new Path(s"gs://${tempDir.getCanonicalPath}")
        val deltaLog = DeltaLog.forTable(spark, gsPath)
        deltaLog.checkpoint()
      }
    }
  }
}

/**
 * A fake GCS file system to verify checkpoint files and delta commits are written in a separate
 * gcs thread.
 */
class FakeGSCFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs:/")

  private def assertGCSThread(f: Path): Unit = {
    if (f.getName.contains(".checkpoint.parquet") || f.getName.contains(".json")) {
      assert(
        Thread.currentThread().getName.contains("delta-gcs-"),
        s"writing $f was happening in non gcs thread: ${Thread.currentThread()}")
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}
