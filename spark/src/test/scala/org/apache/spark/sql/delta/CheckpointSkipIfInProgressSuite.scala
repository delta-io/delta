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

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/** Tests for skipping a post-commit checkpoint while another checkpoint is in progress. */
class CheckpointSkipIfInProgressSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private val skippedOpType = "delta.checkpoint.skippedInProgress"

  private def checkpointVersions(log: DeltaLog): Set[Long] = {
    val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
    fs.listStatus(log.logPath)
      .filter(FileNames.isCheckpointFile)
      .map(FileNames.checkpointVersion)
      .toSet
  }

  private def append(path: String): Unit =
    spark.range(1).write.format("delta").mode("append").save(path)

  private def numSkippedCheckpoints(records: Seq[UsageRecord]): Int =
    records.count(r => r.metric == "tahoeEvent" && r.tags.get("opType").contains(skippedOpType))

  /**
   * Creates a table with a checkpoint every two commits, at version 0, and runs `f` while
   * another checkpoint of the table seems to be in progress in this process.
   */
  private def withCheckpointInProgress(f: (String, DeltaLog) => Unit): Unit = {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "2") {
        append(path)
      }
      val log = DeltaLog.forTable(spark, path)
      log.numCheckpointsInProgress.incrementAndGet()
      try {
        f(path, log)
      } finally {
        log.numCheckpointsInProgress.decrementAndGet()
      }
    }
  }

  test("post-commit checkpoint is skipped while another checkpoint is in progress") {
    withCheckpointInProgress { (path, log) =>
      val records = Log4jUsageLogger.track {
        append(path)
        // Version 2 is due a checkpoint.
        append(path)
      }
      assert(log.update().version === 2)
      assert(!checkpointVersions(log).contains(2))
      assert(numSkippedCheckpoints(records) === 1)
      assert(log.numCheckpointsInProgress.get() === 1)
    }
  }

  test("post-commit checkpoint is written once no other checkpoint is in progress") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "2") {
        append(path)
      }
      val log = DeltaLog.forTable(spark, path)
      val records = Log4jUsageLogger.track {
        append(path)
        append(path)
      }
      assert(checkpointVersions(log).contains(2))
      assert(numSkippedCheckpoints(records) === 0)
      assert(log.numCheckpointsInProgress.get() === 0)
    }
  }

  test("explicit checkpoint is not skipped while another checkpoint is in progress") {
    withCheckpointInProgress { (path, log) =>
      append(path)
      log.checkpoint(log.update())
      assert(checkpointVersions(log).contains(1))
    }
  }

  test("post-commit checkpoint is not skipped when the conf is off") {
    withCheckpointInProgress { (path, log) =>
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_SKIP_IF_IN_PROGRESS.key -> "false") {
        val records = Log4jUsageLogger.track {
          append(path)
          append(path)
        }
        assert(checkpointVersions(log).contains(2))
        assert(numSkippedCheckpoints(records) === 0)
      }
    }
  }
}
