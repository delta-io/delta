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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaDataFrameHadoopOptionsSuite extends QueryTest
  with DeltaSQLTestUtils
  with SharedSparkSession
  with DeltaSQLCommandTest {

  protected override def sparkConf =
    super.sparkConf.set("spark.delta.logStore.fake.impl", classOf[LocalLogStore].getName)

  /**
   * Create Hadoop file system options for `FakeFileSystem`. If Delta doesn't pick up them,
   * it won't be able to read/write any files using `fake://`.
   */
  private def fakeFileSystemOptions: Map[String, String] = {
    Map(
      "fs.fake.impl" -> classOf[FakeFileSystem].getName,
      "fs.fake.impl.disable.cache" -> "true"
    )
  }

  /** Create a fake file system path to test from the dir path. */
  private def fakeFileSystemPath(dir: File): String = s"fake://${dir.getCanonicalPath}"

  /** Clear cache to make sure we don't reuse the cached snapshot */
  private def clearCachedDeltaLogToForceReload(): Unit = {
    DeltaLog.clearCache()
  }

  // read/write parquet format check cache
  test("SC-86916: " +
      "read/write Delta paths using DataFrame should pick up Hadoop file system options") {
    withTempPath { dir =>
      val path = fakeFileSystemPath(dir)
      spark.range(1, 10)
        .write
        .format("delta")
        .options(fakeFileSystemOptions)
        .save(path)
      clearCachedDeltaLogToForceReload()
      spark.read.format("delta").options(fakeFileSystemOptions).load(path).foreach(_ => {})
      // Test time travel
      clearCachedDeltaLogToForceReload()
      spark.read.format("delta").options(fakeFileSystemOptions).load(path + "@v0").foreach(_ => {})
      clearCachedDeltaLogToForceReload()
      spark.read.format("delta").options(fakeFileSystemOptions).option("versionAsOf", 0)
        .load(path).foreach(_ => {})

    }
  }

  testQuietly("SC-86916: disabling the conf should not pick up Hadoop file system options") {
    withSQLConf(DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS.key -> "false") {
      withTempPath { dir =>
        val path = fakeFileSystemPath(dir)
        intercept[Exception] {
          spark.read.format("delta").options(fakeFileSystemOptions).load(path)
        }
      }
    }
  }

  test("SC-86916: checkpoint should pick up Hadoop file system options") {
    withSQLConf(DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "1") {
      withTempPath { dir =>
        val path = fakeFileSystemPath(dir)
        spark.range(1, 10).write.format("delta")
          .options(fakeFileSystemOptions)
          .mode("append")
          .save(path)
        spark.range(1, 10).write.format("delta")
          .options(fakeFileSystemOptions)
          .mode("append")
          .save(path)
        // Ensure we did write the checkpoint and read it back
        val deltaLog = DeltaLog.forTable(spark, new Path(path), fakeFileSystemOptions)
        assert(deltaLog.readLastCheckpointFile().get.version == 1)
      }
    }
  }

  test("SC-86916: invalidateCache should invalidate all DeltaLogs of the given path") {
    withTempPath { dir =>
      val pathStr = fakeFileSystemPath(dir)
      val path = new Path(pathStr)
      spark.range(1, 10).write.format("delta")
        .options(fakeFileSystemOptions)
        .mode("append")
        .save(pathStr)
      val deltaLog = DeltaLog.forTable(spark, path, fakeFileSystemOptions)
      spark.range(1, 10).write.format("delta")
        .options(fakeFileSystemOptions)
        .mode("append")
        .save(pathStr)
      val cachedDeltaLog = DeltaLog.forTable(spark, path, fakeFileSystemOptions)
      assert(deltaLog eq cachedDeltaLog)
      withSQLConf(fakeFileSystemOptions.toSeq: _*) {
        DeltaLog.invalidateCache(spark, path)
      }
      spark.range(1, 10).write.format("delta")
        .options(fakeFileSystemOptions)
        .mode("append")
        .save(pathStr)
      val newDeltaLog = DeltaLog.forTable(spark, path, fakeFileSystemOptions)
      assert(deltaLog ne newDeltaLog)
    }
  }

  test("SC-86916: Delta log cache should respect options") {
    withTempPath { dir =>
      val path = fakeFileSystemPath(dir)
      DeltaLog.clearCache()
      spark.range(1, 10).write.format("delta")
        .options(fakeFileSystemOptions)
        .mode("append")
        .save(path)
      assert(DeltaLog.cacheSize == 1)

      // Accessing the same table should not create a new entry in the cache
      spark.read.format("delta").options(fakeFileSystemOptions).load(path).foreach(_ => {})
      assert(DeltaLog.cacheSize == 1)

      // Accessing the table with different options should create a new entry
      spark.read.format("delta")
        .options(fakeFileSystemOptions ++ Map("fs.foo" -> "foo")).load(path).foreach(_ => {})
      assert(DeltaLog.cacheSize == 2)

      // Accessing the table without options should create a new entry
      withSQLConf(fakeFileSystemOptions.toSeq: _*) {
        spark.read.format("delta").load(path).foreach(_ => {})
      }
      assert(DeltaLog.cacheSize == 3)

      // Make sure we don't break existing cache logic
      DeltaLog.clearCache()
      withSQLConf(fakeFileSystemOptions.toSeq: _*) {
        spark.read.format("delta").load(path).foreach(_ => {})
        spark.read.format("delta").load(path).foreach(_ => {})
      }
      assert(DeltaLog.cacheSize == 1)
    }
  }

}
