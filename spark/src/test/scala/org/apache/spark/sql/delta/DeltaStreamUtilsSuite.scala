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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.spark.sql.delta.sources.DeltaStreamUtils

class DeltaStreamUtilsSuite extends SparkFunSuite {

  // ========== getStartingVersionFromCommitAtTimestamp ==========

  test("getStartingVersionFromCommitAtTimestamp - " +
    "commit at timestamp returns commitVersion") {
    val timeZone = "UTC"
    val commitTs = 1000L
    val commitVersion = 2L
    val latestVersion = 5L
    val timestamp = new Timestamp(1000)
    val result = DeltaStreamUtils.getStartingVersionFromCommitAtTimestamp(
      timeZone, commitTs, commitVersion, latestVersion, timestamp)
    assert(result == 2L)
  }

  test("getStartingVersionFromCommitAtTimestamp - " +
    "commit after timestamp returns commitVersion") {
    val timeZone = "UTC"
    val commitTs = 2000L
    val commitVersion = 2L
    val latestVersion = 5L
    val timestamp = new Timestamp(1000)
    val result = DeltaStreamUtils.getStartingVersionFromCommitAtTimestamp(
      timeZone, commitTs, commitVersion, latestVersion, timestamp)
    assert(result == 2L)
  }

  test("getStartingVersionFromCommitAtTimestamp - " +
    "commit before timestamp returns commitVersion+1") {
    val timeZone = "UTC"
    val commitTs = 1000L
    val commitVersion = 2L
    val latestVersion = 5L
    val timestamp = new Timestamp(2000)
    val result = DeltaStreamUtils.getStartingVersionFromCommitAtTimestamp(
      timeZone, commitTs, commitVersion, latestVersion, timestamp)
    assert(result == 3L)
  }

  test("getStartingVersionFromCommitAtTimestamp - " +
    "timestamp after latest throws when canExceedLatest false") {
    val timeZone = "UTC"
    val commitTs = 1000L
    val commitVersion = 5L
    val latestVersion = 5L
    val timestamp = new Timestamp(2000)
    val e = intercept[Exception] {
      DeltaStreamUtils.getStartingVersionFromCommitAtTimestamp(
        timeZone, commitTs, commitVersion, latestVersion, timestamp, canExceedLatest = false)
    }
    assert(e.getMessage.contains("DELTA_TIMESTAMP_GREATER_THAN_COMMIT"))
  }

  test("getStartingVersionFromCommitAtTimestamp - " +
    "timestamp after latest returns commitVersion+1 when canExceedLatest true") {
    val timeZone = "UTC"
    val commitTs = 1000L
    val commitVersion = 5L
    val latestVersion = 5L
    val timestamp = new Timestamp(2000)
    val result = DeltaStreamUtils.getStartingVersionFromCommitAtTimestamp(
      timeZone, commitTs, commitVersion, latestVersion, timestamp, canExceedLatest = true)
    assert(result == 6L)
  }
}
