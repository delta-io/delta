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

import org.apache.spark.sql.delta.test.V2ForceTest

/**
 * Test suite that runs DeltaSourceSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaSourceDSv2Suite extends DeltaSourceSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  /**
   * Override disableLogCleanup to use DeltaLog API instead of SQL ALTER TABLE.
   * Path-based ALTER TABLE doesn't work properly with V2_ENABLE_MODE=STRICT.
   */
  override protected def disableLogCleanup(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val metadata = deltaLog.snapshot.metadata
    val newConfiguration = metadata.configuration ++ Map(
      DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key -> "false"
    )
    deltaLog.startTransaction().commit(
      metadata.copy(configuration = newConfiguration) :: Nil,
      DeltaOperations.SetTableProperties(
        Map(DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key -> "false"))
    )
  }

  private lazy val testsToRun = Set(
    // ========== Core streaming tests ==========
    "basic",
    "initial snapshot ends at base index of next version",
    "new commits arrive after stream initialization",
    "new commits arrive after stream initialization with startingVersion",
    "SC-11561: can consume new data without update",
    "Delta sources don't write offsets with null json",

    // ========== startingVersion option tests ==========
    "startingVersion",
    "startingVersion latest",
    "startingVersion latest defined before started",
    "startingVersion latest works on defined but empty table",

    // ========== Rate limiting tests ==========
    "maxFilesPerTrigger",
    "maxBytesPerTrigger: process at least one file",
    "maxFilesPerTrigger: change and restart",
    "maxFilesPerTrigger: invalid parameter",
    "maxFilesPerTrigger: ignored when using Trigger.Once",
    "maxBytesPerTrigger: change and restart",
    "maxBytesPerTrigger: invalid parameter",
    "maxBytesPerTrigger: max bytes and max files together",
    "startingVersion should work with rate time"
    // "maxBytesPerTrigger: metadata checkpoint", // fails: got 9 batches instead of 20
    // "maxFilesPerTrigger: metadata checkpoint", // similar issue: different batch count

    // ========== Schema Evolution & Validation tests ==========
    // "allow to change schema before starting a streaming query",
    // "no schema should throw an exception",
    // "disallow user specified schema",
    // "disallow to change schema after starting a streaming query",
    // "restarting a query should pick up latest table schema and recover",
    // "handling nullability schema changes",

    // ========== Trigger.AvailableNow tests ==========
    // "maxFilesPerTrigger: Trigger.AvailableNow respects read limits",
    // "maxBytesPerTrigger: Trigger.AvailableNow respects read limits",
    // "Trigger.AvailableNow with an empty table",
    // "ES-445863: delta source should not hang or reprocess data when using AvailableNow",

    // ========== startingTimestamp option tests ==========
    // "startingTimestamp",
    // "startingVersion and startingTimestamp are both set",

    // ========== startingVersion advanced tests ==========
    // "startingVersion should be ignored when restarting from a checkpoint, withRowTracking = true",
    // "startingVersion should be ignored when restarting from a checkpoint, withRowTracking = false",
    // "startingVersion: user defined start works with mergeSchema",
    // "startingVersion latest calls update when starting",

    // ========== Data loss detection tests ==========
    // "fail on data loss - starting from missing files",
    // "fail on data loss - gaps of files",
    // "fail on data loss - starting from missing files with option off",
    // "fail on data loss - gaps of files with option off",

    // ========== excludeRegex option tests ==========
    // "excludeRegex works and doesn't mess up offsets across restarts - parquet version",
    // "excludeRegex throws good error on bad regex pattern",

    // ========== Metadata/non-data operations tests ==========
    // "Delta source advances with non-data inserts and generates empty dataframe for non-data operations",
    // "Rate limited Delta source advances with non-data inserts",
    // "skip change commits",

    // ========== Error handling & edge cases tests ==========
    // "recreate the reservoir should fail the query",
    // "start from corrupt checkpoint",
    // "can delete old files of a snapshot without update",
    // "Delta sources should verify the protocol reader version",
    // "should not attempt to read a non exist version",
    // "SC-46515: deltaSourceIgnoreChangesError contains removeFile, version, tablePath",
    // "SC-46515: deltaSourceIgnoreDeleteError contains removeFile, version, tablePath",

    // ========== V1-specific tests ==========
    // "streaming delta source should not drop null columns",
    // "streaming delta source should drop null columns without feature flag",
    // "allow user specified schema if consistent: v1 source",
    // "createSource should create source with empty or matching table schema provided",
    // "a fast writer should not starve a Delta source",

    // ========== Miscellaneous tests ==========
    // "make sure that the delta sources works fine",
    // "self union a Delta table should pass the catalog table assert",

    // ========== DeltaSourceOffset unit tests ==========
    // "isInitialSnapshot serializes as isStartingVersion",
    // "unknown sourceVersion value",
    // "invalid sourceVersion value",
    // "missing sourceVersion",
    // "unmatched reservoir id",
    // "DeltaSourceOffset deserialization",
    // "DeltaSourceOffset deserialization error",
    // "DeltaSourceOffset serialization",
    // "DeltaSourceOffset.validateOffsets"
  )

  override protected def shouldSkipTest(testName: String): Boolean = {
    !testsToRun.contains(testName)
  }
}
