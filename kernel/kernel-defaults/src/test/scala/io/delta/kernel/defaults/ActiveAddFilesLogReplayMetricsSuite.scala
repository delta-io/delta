/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.replay.ActiveAddFilesIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite to test the metrics captured during log replay to find the active `AddFile`s
 * in a table snapshot.
 */
class ActiveAddFilesLogReplayMetricsSuite extends AnyFunSuite with TestUtils {

  test("active add files log replay metrics: only delta files") {
    withTempDirAndEngine { (path, engine) =>
      for (_ <- 0 to 9) {
        appendCommit(path)
      }
      loadAndCheckLogReplayMetrics(
        engine,
        path,
        expNumAddFilesSeen = 20, // each commit creates 2 files
        expNumAddFilesSeenFromDeltaFiles = 20,
        expNumActiveAddFiles = 20)
    }
  }


  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + delta files") {
      withTempDirAndEngine { (path, engine) =>
        for (_ <- 0 to 3) {
          appendCommit(path)
        }
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000)
        for (_ <- 4 to 9) {
          appendCommit(path)
        }

        loadAndCheckLogReplayMetrics(
          engine,
          path,
          expNumAddFilesSeen = 20, // each commit creates 2 files
          expNumAddFilesSeenFromDeltaFiles = 12, // checkpoint is created at version 3
          expNumActiveAddFiles = 20)
      }
    }
  }

  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + " +
        s"delta files + tombstones") {
      withTempDirAndEngine { (path, engine) =>
        for (_ <- 0 to 3) {
          appendCommit(path)
        } // has 8 add files
        deleteCommit(path) // version 4 - deletes 4 files and adds 1 file
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000) // version 4
        appendCommit(path) // version 5 - adds 2 files
        deleteCommit(path) // version 6 - deletes 1 file and adds 1 file
        appendCommit(path) // version 7 - adds 2 files
        appendCommit(path) // version 8 - adds 2 files
        deleteCommit(path) // version 9 - deletes 2 files and adds 1 file

        loadAndCheckLogReplayMetrics(
          engine,
          path,
          expNumAddFilesSeen = 5 /* checkpoint */ + 8, /* delta */
          expNumAddFilesSeenFromDeltaFiles = 8,
          expNumActiveAddFiles = 10,
          expNumTombstonesSeen = 3
        )
      }
    }
  }

  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + delta files +" +
        s" tombstones + duplicate adds") {
      withTempDirAndEngine { (path, engine) =>
        for (_ <- 0 to 1) {
          appendCommit(path)
        } // activeAdds = 4
        deleteCommit(path) // ver 2 - deletes 2 files and adds 1 file, activeAdds = 3
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000) // version 2
        appendCommit(path) // ver 3 - adds 2 files, activeAdds = 5
        recomputeStats(path) // ver 4 - adds the same 5 add files again, activeAdds = 5, dupes = 5
        deleteCommit(path) // ver 5 - removes 1 file and adds 1 file, activeAdds = 5, dupes = 5
        appendCommit(path) // ver 6 - adds 2 files, activeAdds = 7, dupes = 4
        recomputeStats(path) // ver 7 - adds the same 7 add files again, activeAdds = 7, dupes = 12
        deleteCommit(path) // ver 8 - removes 1 file and adds 1 files, activeAdds = 7, dupes = 12

        loadAndCheckLogReplayMetrics(
          engine,
          path,
          expNumAddFilesSeen = 3 /* checkpoint */ + 18, /* delta */
          expNumAddFilesSeenFromDeltaFiles = 18,
          expNumActiveAddFiles = 7,
          expNumTombstonesSeen = 2,
          expNumDuplicateAddFiles = 12
        )
      }
    }
  }

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////
  def loadAndCheckLogReplayMetrics(
    engine: Engine,
    tablePath: String,
    expNumAddFilesSeen: Long,
    expNumAddFilesSeenFromDeltaFiles: Long,
    expNumActiveAddFiles: Long,
    expNumDuplicateAddFiles: Long = 0L,
    expNumTombstonesSeen: Long = 0L): Unit = {

    val scanFileIter = Table.forPath(engine, tablePath)
      .getLatestSnapshot(engine)
      .getScanBuilder(engine)
      .build()
      .getScanFiles(engine)

    // this will trigger the log replay, consumes actions and closes the iterator
    scanFileIter.toSeq

    val metrics = scanFileIter.asInstanceOf[ActiveAddFilesIterator].getMetrics
    assert(metrics.getNumAddFilesSeen == expNumAddFilesSeen)
    assert(metrics.getNumAddFilesSeenFromDeltaFiles == expNumAddFilesSeenFromDeltaFiles)
    assert(metrics.getNumActiveAddFiles == expNumActiveAddFiles)
    assert(metrics.getNumDuplicateAddFiles == expNumDuplicateAddFiles)
    assert(metrics.getNumTombstonesSeen == expNumTombstonesSeen)


    val expResults = spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_))
    checkTable(tablePath, expResults)
  }

  def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    val engine = DefaultEngine.create(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })
    withTempDir { dir => f(dir.getAbsolutePath, engine) }
  }

  def appendCommit(path: String): Unit =
    spark.range(10).repartition(2).write.format("delta").mode("append").save(path)

  def deleteCommit(path: String): Unit = {
    spark.sql("DELETE FROM delta.`%s` WHERE id = 5".format(path))
  }

  def recomputeStats(path: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, new Path(path))
    StatisticsCollection.recompute(spark, deltaLog, catalogTable = None)
  }

  def checkpoint(path: String, actionsPerFile: Int): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> actionsPerFile.toString) {
      DeltaLog.forTable(spark, path).checkpoint()
    }
  }
}
