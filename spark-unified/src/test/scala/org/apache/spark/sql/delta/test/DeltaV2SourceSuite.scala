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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.DeltaSourceSuite

/**
 * Test suite that runs DeltaSourceSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SourceSuite extends DeltaSourceSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  /**
   * Override disableLogCleanup to use DeltaLog API instead of SQL ALTER TABLE.
   * Path-based ALTER TABLE doesn't work properly with V2_ENABLE_MODE=STRICT.
   * TODO(#5731): pending kernel v2 connector support.
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

  private lazy val shouldPassTests = Set(
    // ========== Core streaming tests ==========
    "basic",
    "initial snapshot ends at base index of next version",
    "new commits arrive after stream initialization - with explicit startingVersion",
    "SC-11561: can consume new data without update",
    "Delta sources don't write offsets with null json",

    // === Schema Evolution ===
    "restarting a query should pick up latest table schema and recover",
    "disallow to change schema after starting a streaming query",
    "allow to change schema before starting a streaming query",

    // ========== startingVersion option tests ==========
    "startingVersion",
    "startingVersion latest",
    "startingVersion latest defined before started",
    "startingVersion latest works on defined but empty table",
    "startingVersion specific version: new commits arrive after stream initialization",
    "startingVersion: user defined start works with mergeSchema",
    "startingVersion latest calls update when starting",
    "startingVersion should be ignored when restarting from a checkpoint, withRowTracking = true",
    "startingVersion should be ignored when restarting from a checkpoint, withRowTracking = false",
    "startingVersion and startingTimestamp are both set",
    "startingTimestamp",

    // ========== Rate limiting tests ==========
    "maxFilesPerTrigger",
    "maxBytesPerTrigger: process at least one file",
    "maxFilesPerTrigger: change and restart",
    "maxFilesPerTrigger: invalid parameter",
    "maxFilesPerTrigger: ignored when using Trigger.Once",
    "maxFilesPerTrigger: Trigger.AvailableNow respects read limits",
    "maxBytesPerTrigger: change and restart",
    "maxBytesPerTrigger: invalid parameter",
    "maxBytesPerTrigger: Trigger.AvailableNow respects read limits",
    "maxBytesPerTrigger: max bytes and max files together",
    "Trigger.AvailableNow with an empty table",
    "Rate limited Delta source advances with non-data inserts",
    "ES-445863: delta source should not hang or reprocess data when using AvailableNow",
    "startingVersion should work with rate time",
    "maxFilesPerTrigger: metadata checkpoint",
    "maxBytesPerTrigger: metadata checkpoint",

    // ========== Error handling tests ==========
    "SC-46515: deltaSourceIgnoreDeleteError contains removeFile, version, tablePath",
    "excludeRegex throws good error on bad regex pattern",

    // ========== Misc tests ==========
    "a fast writer should not starve a Delta source",
    "should not attempt to read a non exist version"
  )

  private lazy val shouldFailTests = Set(
    // === Null Type Column Handling ===
    "streaming delta source should not drop null columns",
    "streaming delta source should drop null columns without feature flag",

    // === read options ===
    "skip change commits",
    "excludeRegex works and doesn't mess up offsets across restarts - parquet version",

    // === Data Loss Detection ===
    "fail on data loss - starting from missing files",
    "fail on data loss - gaps of files",
    "fail on data loss - starting from missing files with option off",
    "fail on data loss - gaps of files with option off",

    // === Misc ===
    "no schema should throw an exception",
    "recreate the reservoir should fail the query",
    "SC-46515: deltaSourceIgnoreChangesError contains removeFile, version, tablePath",
    "Delta sources should verify the protocol reader version",
    "can delete old files of a snapshot without update",
    "Delta source advances with non-data inserts and generates empty dataframe for " +
      "non-data operations",
    "Delta source advances with non-data inserts and generates empty dataframe for addl files",
    "start from corrupt checkpoint",

    // === Tests that bypass V2 by not using loadStreamWithOptions ===
    "disallow user specified schema", // Uses .schema() directly
    "make sure that the delta sources works fine", // Uses .delta() directly
    "self union a Delta table should pass the catalog table assert", // Uses .table() directly
    "handling nullability schema changes", // Uses .table() directly
    "allow user specified schema if consistent: v1 source", // Uses DataSource directly
    // Calls deltaSource.createSource() directly
    "createSource should create source with empty or matching table schema provided",
    // Unit test for internal API
    "DeltaLog.createDataFrame should drop null columns with feature flag",
    // Unit test for internal API
    "DeltaLog.createDataFrame should not drop null columns without feature flag",
    "unknown sourceVersion value", // Unit test for DeltaSourceOffset
    "invalid sourceVersion value", // Unit test for DeltaSourceOffset
    "missing sourceVersion", // Unit test for DeltaSourceOffset
    "unmatched reservoir id", // Unit test for DeltaSourceOffset
    "isInitialSnapshot serializes as isStartingVersion", // Unit test for DeltaSourceOffset
    "DeltaSourceOffset deserialization", // Unit test for DeltaSourceOffset
    "DeltaSourceOffset deserialization error", // Unit test for DeltaSourceOffset
    "DeltaSourceOffset serialization", // Unit test for DeltaSourceOffset
    "DeltaSourceOffset.validateOffsets" // Unit test for DeltaSourceOffset
  )

  override protected def shouldFail(testName: String): Boolean = {
    val inPassList = shouldPassTests.contains(testName)
    val inFailList = shouldFailTests.contains(testName)

    assert(inPassList || inFailList, s"Test '$testName' not in shouldPassTests or shouldFailTests")
    assert(!(inPassList && inFailList),
      s"Test '$testName' in both shouldPassTests and shouldFailTests")

    inFailList
  }
}
