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

import java.sql.Timestamp

import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_COMMIT_TIMESTAMP, CDC_COMMIT_VERSION}

import org.apache.spark.sql.{DataFrame, QueryTest, Row}

trait CheckCDCAnswer extends QueryTest {
  /**
   * Check the result of a CDC operation. The expected answer should include only CDC type and
   * log version - the timestamp is nondeterministic, so we'll check just that it matches the
   * correct value in the Delta log.
   *
   * @param log            The Delta log for the table CDC is being extracted from.
   * @param df             The computed dataframe, which should match the default CDC result schema.
   *                       Callers doing projections on top should use checkAnswer directly.
   * @param expectedAnswer The expected results for the CDC query, excluding the CDC_LOG_TIMESTAMP
   *                       column which we handle inside this method.
   */
  def checkCDCAnswer(log: DeltaLog, df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    checkAnswer(df.drop(CDC_COMMIT_TIMESTAMP), expectedAnswer)

    val timestampsByVersion = df.select(CDC_COMMIT_VERSION, CDC_COMMIT_TIMESTAMP).collect()
      .map { row =>
        val version = row.getLong(0)
        val ts = row.getTimestamp(1)
        (version -> ts)
      }.toMap
    val correctTimestampsByVersion = {
      // Results should match the fully monotonized commits. Note that this map will include
      // all versions of the table but only the ones in timestampsByVersion are checked for
      // correctness.
      val commits = DeltaHistoryManager.getCommits(
        log.store,
        log.logPath,
        start = 0,
        end = None,
        log.newDeltaHadoopConf())

      // Note that the timestamps come from filesystem modification timestamps, so they're
      // milliseconds since epoch and we don't need to deal with timezones.
      commits.map(f => (f.version -> new Timestamp(f.timestamp))).toMap
    }

    timestampsByVersion.keySet.foreach { version =>
      assert(timestampsByVersion(version) === correctTimestampsByVersion(version))
    }
  }

  def checkCDCAnswer(log: DeltaLog, df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkCDCAnswer(log, df, expectedAnswer.collect())
  }
}
