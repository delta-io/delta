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
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.util.ManualClock

trait CheckpointProtectionTestUtilsMixin
    extends DeltaSQLCommandTest { self: DeltaRetentionSuiteBase =>

  def testRequireCheckpointProtectionBeforeVersion(
      createNumCommitsOutsideRetentionPeriod: Int,
      createNumCommitsWithinRetentionPeriod: Int,
      createCheckpoints: Set[Int],
      requireCheckpointProtectionBeforeVersion: Int,
      additionalFeatureToEnable: Option[TableFeature] = None,
      expectedCommitsAfterCleanup: Set[Int],
      expectedCheckpointsAfterCleanup: Set[Int]): Unit = {
    withTempDir { dir =>
      val currentTime = System.currentTimeMillis()
      val clock = new ManualClock(currentTime)
      val deltaLog = DeltaLog.forTable(spark, dir, clock)
      val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
      val propertyKey = DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.key
      val additionalFeatureEnablement =
        additionalFeatureToEnable.map(f => s"delta.feature.${f.name} = 'supported',")
          .getOrElse("")

      // Commit 0.
      sql(
        s"""CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta
           |TBLPROPERTIES (
           |delta.feature.${CheckpointProtectionTableFeature.name} = 'supported',
           |$additionalFeatureEnablement
           |$propertyKey = $requireCheckpointProtectionBeforeVersion
           |)""".stripMargin)
      if (createCheckpoints.contains(0)) deltaLog.checkpoint(deltaLog.update())

      // Rest createNumCommitsOutsideRetentionPeriod - 1 commits.
      (1 to createNumCommitsOutsideRetentionPeriod - 1).foreach { n =>
        spark.range(n, n + 1).write.format("delta").mode("append").save(dir.getCanonicalPath)
        if (createCheckpoints.contains(n)) deltaLog.checkpoint(deltaLog.update())

        setModificationTime(deltaLog, startTime = currentTime, version = n, dayNum = 0, fs)
      }

      val millisToAdvance =
        intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) + TimeUnit.DAYS.toMillis(3)
      clock.advance(millisToAdvance)

      // Commits within retention period.
      val daysToAdvance = TimeUnit.MILLISECONDS.toDays(millisToAdvance).toInt
      (0 to createNumCommitsWithinRetentionPeriod - 1)
        .foreach { n =>
          val m = createNumCommitsOutsideRetentionPeriod + n
          spark.range(m, m + 1).write.format("delta").mode("append").save(dir.getCanonicalPath)
          if (createCheckpoints.contains(m)) deltaLog.checkpoint(deltaLog.update())

          // Advance the timestamp of the commit/checkpoint we just created.
          setModificationTime(
            deltaLog,
            startTime = currentTime,
            version = m,
            // The files were created somewhere between day 32 and day 33.
            dayNum = daysToAdvance - 1,
            fs)
        }

      deltaLog.cleanUpExpiredLogs(deltaLog.update())

      val logPath = new File(deltaLog.logPath.toUri)
      assert(getDeltaVersions(logPath) === expectedCommitsAfterCleanup)
      assert(getCheckpointVersions(logPath) === expectedCheckpointsAfterCleanup)
    }
  }
}
