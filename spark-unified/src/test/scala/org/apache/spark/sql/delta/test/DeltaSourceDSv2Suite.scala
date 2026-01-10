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
class DeltaSourceDSv2Suite extends DeltaSourceSuite with V2ForceTest {

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

    // ========== Error handling tests ==========
    "SC-46515: deltaSourceIgnoreDeleteError contains removeFile, version, tablePath",
    "excludeRegex throws good error on bad regex pattern",

    // ========== Misc tests ==========
    "a fast writer should not starve a Delta source",
    "make sure that the delta sources works fine",
    "should not attempt to read a non exist version",
    "self union a Delta table should pass the catalog table assert",
    "DeltaLog.createDataFrame should drop null columns with feature flag",
    "DeltaLog.createDataFrame should not drop null columns without feature flag"
  )

  private lazy val shouldFailTests = Set(
    // === Schema Evolution ===
    // Tests schema evolution: table changes from "id" to "id,value" schema. FAILS: V2 returns 0
    // rows with empty schema instead of 10 rows with evolved schema.
    "allow to change schema before starting a streaming query",
    // Tests that restarting a stream picks up latest schema after schema changes. FAILS: throws
    // NoSuchNamespaceException for schema `delta` - appears to be test setup issue with V2.
    "restarting a query should pick up latest table schema and recover",
    // Tests schema changes after stream starts should fail gracefully. FAILS: test behavior
    // differs with V2 connector.
    "disallow to change schema after starting a streaming query",
    "handling nullability schema changes",  // Does not use loadStreamWithOptions
    "allow user specified schema if consistent: v1 source",  // Does not use loadStreamWithOptions
    "disallow user specified schema",  // Does not use loadStreamWithOptions
    "createSource should create source with empty or matching table schema provided",  // Does not use loadStreamWithOptions

    // === Null Type Column Handling ===
    "streaming delta source should not drop null columns",  // Does not use loadStreamWithOptions
    // Tests error handling for streaming null columns without feature flag. FAILS: throws
    // KernelException instead of expected StreamingQueryException.
    "streaming delta source should drop null columns without feature flag",

    // === Read options ===
    // Tests skipChangeCommits option for ignoring UPDATE/DELETE/MERGE operations. FAILS: test
    // behavior differs with V2 connector.
    "skip change commits",
    // Tests excludeRegex option for filtering files. FAILS: test behavior differs with V2 connector.
    "excludeRegex works and doesn't mess up offsets across restarts - parquet version",

    // === startingTimestamp option ===
    // Tests startingTimestamp option for starting stream from a specific timestamp. FAILS: V2
    // connector doesn't support startingTimestamp option (only startingVersion, maxFilesPerTrigger,
    // maxBytesPerTrigger are supported).
    "startingTimestamp",

    // === Error handling tests ===
    // Tests error message format for CDC changes. FAILS: test expectations don't match V2 behavior.
    "SC-46515: deltaSourceIgnoreChangesError contains removeFile, version, tablePath",
    // Tests protocol version verification. FAILS: throws KernelException instead of expected
    // InvalidProtocolVersionException, and test assertion logic doesn't handle this.
    "Delta sources should verify the protocol reader version",
    // Tests that recreating a table should fail existing streams. FAILS: test behavior differs
    // with V2 connector.
    "recreate the reservoir should fail the query",
    // Tests error handling for tables without schema. FAILS: throws TableNotFoundException
    // instead of expected AnalysisException.
    "no schema should throw an exception",
    // Tests reading from corrupt checkpoint. FAILS: throws InvalidTableException from kernel.
    "start from corrupt checkpoint",

    // === Data Loss Detection ===
    // Tests failOnDataLoss with missing version files. FAILS: V2 connector doesn't support
    // failOnDataLoss option, throws StreamingQueryException about unsupported option.
    "fail on data loss - starting from missing files with option off",
    "fail on data loss - gaps of files with option off",
    // Tests data loss detection when files are missing. FAILS: error message format differs
    // between V1 and V2 (kernel provides different error messages).
    "fail on data loss - starting from missing files",
    "fail on data loss - gaps of files",

    // === Rate Limiting with Metadata Checkpoints ===
    // Tests maxFilesPerTrigger with metadata checkpoint (20 files, expects 20 batches). FAILS:
    // V2 only produces 9 batches instead of 20 - rate limiting doesn't work correctly with
    // metadata checkpoints in V2 connector.
    "maxFilesPerTrigger: metadata checkpoint",
    // Tests maxBytesPerTrigger with metadata checkpoint (20 files, expects 20 batches). FAILS:
    // V2 only produces 9 batches instead of 20 - same rate limiting issue as maxFilesPerTrigger.
    "maxBytesPerTrigger: metadata checkpoint",

    // === Source Offset / Version Handling (unit tests without streaming) ===
    "unknown sourceVersion value",
    "invalid sourceVersion value",
    "missing sourceVersion",
    "unmatched reservoir id",
    "isInitialSnapshot serializes as isStartingVersion",
    "DeltaSourceOffset deserialization",
    "DeltaSourceOffset deserialization error",
    "DeltaSourceOffset serialization",
    "DeltaSourceOffset.validateOffsets",

    // === Misc (tests that don't use loadStreamWithOptions) ===
    "can delete old files of a snapshot without update",
    "Delta source advances with non-data inserts and generates empty dataframe for " +
      "non-data operations",
    "Delta source advances with non-data inserts and generates empty dataframe for addl files"
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
