/*
 * Copyright (2025) The Delta Lake Project Authors.
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

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

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

  override protected def shouldPassTests: Set[String] = DeltaV2SourceSuite.PassingTests

  override protected def shouldFailTests: Set[String] = DeltaV2SourceSuite.FailingTests
}

/**
 * Shared V2-connector test classifications for `DeltaSourceSuite`. Other V2 suites that inherit
 * from `DeltaSourceSuite` (e.g. the V2 column-mapping suites) can compose these sets with
 * their own additions or overrides.
 */
object DeltaV2SourceSuite {

  val PassingTests: Set[String] = Set(
    // ========== Core streaming tests ==========
    "basic",
    "new commits arrive after stream initialization - with explicit startingVersion",
    "initial snapshot ends at base index of next version",
    "can consume new data without update",
    "Delta sources don't write offsets with null json",
    "reading from partitioned table succeeds during restart",
    "__metadata is reachable when a user _metadata column collides",

    // === Schema Evolution ===
    "add column: restarting with new DataFrame should recover",
    "add column: restarting with stale DataFrame should fail",
    "relax nullability: restarting with new DataFrame should recover",
    "type widening: restarting with new DataFrame should recover",
    "disallow to change schema after starting a streaming query",
    "allow to change schema before starting a streaming query",
    "drop column: should fail with non-additive schema change error",
    "drop column: should succeed with unsafe column mapping schema change flag enabled",
    "rename column: should fail with non-additive schema change error",
    "rename column: should throw schema change error with unsafe flag enabled",
    "type widening: should fail with non-additive schema change error when enable schema tracking",

    // === Read options ===
    "excludeRegex works and doesn't mess up offsets across restarts - parquet version",
    "read options [ignoreDeletes]: ignores delete, rejects change",
    "read options [skipChangeCommits]: ignores delete, skips change",
    "read options [ignoreChanges]: ignores delete, includes change AddFiles",
    "read options [ignoreFileDeletion] (deprecated): equivalent to ignoreChanges",
    "read options [ignoreDeletes, ignoreChanges]: equivalent to ignoreChanges",
    "read options [ignoreChanges, skipChangeCommits]: equivalent to skipChangeCommits",
    "read options [ignoreDeletes, skipChangeCommits]: equivalent to skipChangeCommits",
    "read options [ignoreDeletes, ignoreChanges, skipChangeCommits]: " +
      "equivalent to skipChangeCommits",

    // === Commit/Checkpoint file missing detection ===
    "incremental: first commit file missing, fails",
    "incremental: commit file gap between versions, fails",
    "incremental: first commit file missing, failOnDataLoss=false succeeds",
    "initial snapshot: commit file missing but checkpoint intact, succeeds",
    "initial snapshot: checkpoint missing but all commit files intact, succeeds",
    "initial snapshot: both checkpoint and commit file missing, fails",
    "initial snapshot: log retention deletes old checkpoint and commit files mid-stream," +
      " restart fails",
    "streaming processes 100 sequential single-value commits and contains all values 0 to 99",

    // ========== Passthrough options ==========
    "batch-only options are ignored in streaming",

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
    "startingTimestamp with mid-history ICT",

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
    "delta source should not hang or reprocess data when using AvailableNow",
    "startingVersion should work with rate time",
    "maxFilesPerTrigger: metadata checkpoint",
    "maxBytesPerTrigger: metadata checkpoint",

    // ========== Error handling tests ==========
    "streaming query should fail when table is deleted and recreated with new id",
    "deltaSourceIgnoreDeleteError contains removeFile, version, tablePath",
    "deltaSourceIgnoreChangesError contains changeInfo, version, tablePath",
    "excludeRegex throws good error on bad regex pattern",
    "Delta sources should verify the protocol reader version",

    // ========== Misc tests ==========
    "a fast writer should not starve a Delta source",
    "should not attempt to read a non exist version",
    "can delete old files of a snapshot without update",
    "Delta source advances with non-data inserts and generates empty dataframe for " +
      "non-data operations",
    "reading from table with multiple partition columns succeeds during restart",
    "streaming read returns correct data from table with partition column in middle",
    "streaming read with column pruning and partition column in middle",
    "streaming read with column mapping id and partition column in middle",
    "streaming read after column rename with partition column in middle",
    "streaming read preserves percent-literal string partition value",
    "initial snapshot: checkpoint resume produces all rows without duplicates",
    "initial snapshot: Trigger.AvailableNow processes all data and terminates",
    "initial snapshot: checkpoint resume after new commits produces all rows"
  )

  val FailingTests: Set[String] = Set(
    // === Null Type Column Handling ===
    "streaming delta source should not drop null columns",
    "streaming delta source should drop null columns without feature flag",

    // === Schema Evolution ===
    // TODO(#6232): DSv2 pins the table schema when the DataFrame is loaded, so restarting from a
    //  stale DataFrame can't adopt the schema change. Enable once the V2 relation refreshes its
    //  schema without rebuilding the DataFrame.
    "relax nullability: restarting with stale DataFrame should recover",
    "type widening: restarting with stale DataFrame should recover",

    // === Data Loss Detection ===
    // V2 only tolerates missing start versions with failOnDataLoss=false; mid-log gaps still
    // throw InvalidTableException because non-contiguous versions are not a log-retention scenario.
    "incremental: commit file gap between versions, failOnDataLoss=false succeeds",
    // These tests directly use DeltaSource, not applicable to the v2 path.
    "fail on missing trailing commit - trailing commit disappears between latestOffset and" +
      " getBatch readChangeFeed=true midVersionEndOffset=true",
    "fail on missing trailing commit - trailing commit disappears between latestOffset and" +
      " getBatch readChangeFeed=true midVersionEndOffset=false",
    "fail on missing trailing commit - trailing commit disappears between latestOffset and" +
      " getBatch readChangeFeed=false midVersionEndOffset=true",
    "fail on missing trailing commit - trailing commit disappears between latestOffset and" +
      " getBatch readChangeFeed=false midVersionEndOffset=false",
    "fail on missing trailing commit - empty batch from startIndex >= endIndex is not a" +
      " false positive readChangeFeed=true",
    "fail on missing trailing commit - empty batch from startIndex >= endIndex is not a" +
      " false positive readChangeFeed=false",

    // === Misc ===
    // TODO(#5900): fix exception mismatch
    "no schema should throw an exception",
    // TODO(#5895): gracefully handle corrupt checkpoint
    "start from corrupt checkpoint",

    // === Tests that bypass V2 by not using loadStreamWithOptions ===
    "disallow user specified schema", // Uses .schema() directly
    "make sure that the delta sources works fine", // Uses .delta() directly
    "self union a Delta table should pass the catalog table assert", // Uses .table() directly
    "handling nullability schema changes", // Uses .table() directly
    "allow user specified schema if consistent: v1 source", // Uses DataSource directly
    // Calls deltaSource.createSource() directly
    "createSource should create source with empty or matching table schema provided"
  )
}

/**
 * Runs DeltaV2SourceSuite with the distributed initial snapshot path enabled.
 * Every test that reads from the beginning (no startingVersion) automatically
 * exercises the DataFrame-based snapshot cache.
 */
class DeltaV2SourceDistributedInitialSnapshotSuite extends DeltaV2SourceSuite {
  import org.apache.spark.sql.delta.sources.DeltaSQLConf

  override protected def sparkConf: org.apache.spark.SparkConf = {
    super.sparkConf.set(
      DeltaSQLConf.DELTA_STREAMING_USE_DISTRIBUTED_INITIAL_SNAPSHOT.key, "true")
  }
}
