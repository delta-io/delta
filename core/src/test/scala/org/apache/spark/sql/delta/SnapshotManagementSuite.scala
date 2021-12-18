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

import java.io.{File, RandomAccessFile}
import java.util.concurrent.ExecutionException

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class SnapshotManagementSuite extends QueryTest with SQLTestUtils with SharedSparkSession {


  /**
   * Truncate an existing checkpoint file to create a corrupt file.
   *
   * @param path the Delta table path
   * @param checkpointVersion the checkpoint version to be updated
   * @param shouldBeEmpty whether to create an empty checkpoint file
   */
  private def makeCorruptCheckpointFile(
      path: String,
      checkpointVersion: Long,
      shouldBeEmpty: Boolean): Unit = {
    val checkpointFile =
      FileNames.checkpointFileSingular(new Path(path, "_delta_log"), checkpointVersion).toString
    val cp = new RandomAccessFile(checkpointFile, "rw")
    cp.setLength(if (shouldBeEmpty) 0 else 10)
    cp.close()
  }

  private def deleteLogVersion(path: String, version: Long): Unit = {
    val deltaFile = new File(FileNames.deltaFile(new Path(path, "_delta_log"), version).toString)
    assert(deltaFile.exists(), s"Could not find $deltaFile")
    assert(deltaFile.delete(), s"Failed to delete $deltaFile")
  }

  testQuietly("recover from a corrupt checkpoint: previous checkpoint doesn't exist") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 0, shouldBeEmpty = testEmptyCheckpoint)
        DeltaLog.clearCache()
        // Checkpoint 1 is corrupted. Verify that we can still create the snapshot using
        // existing json files.
        DeltaLog.forTable(spark, path).snapshot
      }
    }
  }

  testQuietly("recover from a corrupt checkpoint: previous checkpoint exists") {
    withTempDir { tempDir =>
      // Create checkpoint 0 and 1
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(10).write.format("delta").mode("append").save(path)
      deltaLog.update()
      deltaLog.checkpoint()

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1, shouldBeEmpty = testEmptyCheckpoint)
        // Checkpoint 1 is corrupted. Verify that we can still create the snapshot using
        // checkpoint 0.
        DeltaLog.clearCache()
        DeltaLog.forTable(spark, path).snapshot
      }
    }
  }

  testQuietly("should not recover when the current checkpoint is broken but we don't have the " +
    "entire history") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      deleteLogVersion(path, version = 0)

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1, shouldBeEmpty = testEmptyCheckpoint)
        val e = intercept[Exception] {
          DeltaLog.clearCache()
          DeltaLog.forTable(spark, path).snapshot
        }
        if (testEmptyCheckpoint) {
          // - checkpoint 1 is NOT in the list result
          // - try to get an alternative LogSegment in `getLogSegmentForVersion`
          // - fail to get an alternative LogSegment
          // - throw the below exception
          assert(e.isInstanceOf[IllegalStateException] &&
            e.getMessage.contains("Couldn't find all part files of the checkpoint version: 1"))
        } else {
          // - checkpoint 1 is in the list result
          // - fail to read checkpoint 1
          // - fail to get an alternative LogSegment
          // - cannot find log file 0 so throw the above checkpoint 1 read failure
          // Guava cache wraps the root cause
          assert(e.isInstanceOf[ExecutionException] && e.getCause.isInstanceOf[SparkException])
        }
      }
    }
  }

  testQuietly("should not recover when both the current and previous checkpoints are broken") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(10).write.format("delta").mode("append").save(path)
      deltaLog.update()
      deltaLog.checkpoint()
      deleteLogVersion(path, version = 0)
      makeCorruptCheckpointFile(path, checkpointVersion = 0, shouldBeEmpty = false)

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1, shouldBeEmpty = testEmptyCheckpoint)
        val e = intercept[Exception] {
          DeltaLog.clearCache()
          DeltaLog.forTable(spark, path).snapshot
        }
        // testEmptyCheckpoint = true:
        // - checkpoint 1 is NOT in the list result.
        // - fallback to load version 0 using checkpoint 0
        // - fail to read checkpoint 0
        // - cannot find log file 0 so throw the above checkpoint 0 read failure
        // testEmptyCheckpoint = false:
        // - checkpoint 1 is in the list result.
        // - fail to read checkpoint 1
        // - fallback to load version 0 using checkpoint 0
        // - fail to read checkpoint 0
        // - cannot find log file 0 so throw the above checkpoint 1 read failure
        // Guava cache wraps the root cause
        assert(e.isInstanceOf[ExecutionException] && e.getCause.isInstanceOf[SparkException])
      }
    }
  }

  test("should throw a clear exception when checkpoint exists but its corresponding delta file " +
    "doesn't exist") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      // Delete delta files
      new File(tempDir, "_delta_log").listFiles().filter(_.getName.endsWith(".json"))
        .foreach(_.delete())
      val e = intercept[IllegalStateException] {
        DeltaLog.clearCache()
        DeltaLog.forTable(spark, path).snapshot
      }
      assert(e.getMessage.contains("Could not find any delta files for version 0"))
    }
  }

  test("should throw an exception when trying to load a non-existent version") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      val e = intercept[IllegalStateException] {
        DeltaLog.clearCache()
        DeltaLog.forTable(spark, path).getSnapshotAt(2)
      }
      assert(e.getMessage.contains("Trying to load a non-existent version 2"))
    }
  }

  test("should throw a clear exception when the checkpoint is corrupt " +
    "but could not find any delta files") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      // Delete delta files
      new File(tempDir, "_delta_log").listFiles().filter(_.getName.endsWith(".json"))
        .foreach(_.delete())
      makeCorruptCheckpointFile(path, checkpointVersion = 0, shouldBeEmpty = false)
      val e = intercept[IllegalStateException] {
        DeltaLog.clearCache()
        DeltaLog.forTable(spark, path).snapshot
      }
      assert(e.getMessage.contains("Could not find any delta files for version 0"))
    }
  }

  test("verifyDeltaVersions") {
    import SnapshotManagement.verifyDeltaVersions
    // empty array
    verifyDeltaVersions(
      spark,
      versions = Array.empty,
      expectedStartVersion = None,
      expectedEndVersion = None)
    // contiguous versions
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = None,
      expectedEndVersion = None)
    // contiguous versions with correct `expectedStartVersion` and `expectedStartVersion`
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = None,
      expectedEndVersion = Some(3))
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = Some(1),
      expectedEndVersion = None)
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = Some(1),
      expectedEndVersion = Some(3))
    // `expectedStartVersion` or `expectedEndVersion` doesn't match
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 2),
        expectedStartVersion = Some(0),
        expectedEndVersion = None)
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 2),
        expectedStartVersion = None,
        expectedEndVersion = Some(3))
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array.empty,
        expectedStartVersion = Some(0),
        expectedEndVersion = None)
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array.empty,
        expectedStartVersion = None,
        expectedEndVersion = Some(3))
    }
    // non contiguous versions
    intercept[IllegalStateException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 3),
        expectedStartVersion = None,
        expectedEndVersion = None)
    }
    // unsorted versions
    intercept[IllegalStateException] {
      verifyDeltaVersions(
        spark,
        versions = Array(3, 2, 1),
        expectedStartVersion = None,
        expectedEndVersion = None)
    }
  }
}
