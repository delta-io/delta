/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.{DeltaCDCStreamSuite, DeltaConfigs, DeltaLog, DeltaOperations}

/**
 * Test suite that runs DeltaCDCStreamSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2CDCStreamSuite extends DeltaCDCStreamSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected def enableCDF(path: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val metadata = deltaLog.update().metadata
    deltaLog.startTransaction().commit(
      metadata.copy(configuration =
        metadata.configuration + (DeltaConfigs.CHANGE_DATA_FEED.key -> "true")) :: Nil,
      DeltaOperations.SetTableProperties(
        Map(DeltaConfigs.CHANGE_DATA_FEED.key -> "true")))
  }

  private lazy val shouldPassTests = Set(
    // ========== Core CDC streaming tests ==========
    "no startingVersion should result fetch the entire snapshot",
    "CDC initial snapshot should end at base index of next version",
    "startingVersion = latest",
    "user provided startingVersion",
    "user provided startingTimestamp",
    "startingVersion and startingTimestamp are both set",
    "cdc streams should respect checkpoint",
    "cdc streams with noop merge",
    "streams updating latest offset with readChangeFeed=true",
    "streams updating latest offset with readChangeFeed=false",

    // ========== File action variant tests ==========
    "cdc streams should be able to get offset when there only RemoveFiles",
    "cdc streams should work starting from RemoveFile",
    "cdc streams should work starting from AddCDCFile",

    // ========== Rate limiting tests ==========
    "rateLimit - maxFilesPerTrigger - overall",
    "rateLimit - maxBytesPerTrigger - overall",
    "rateLimit - maxFilesPerTrigger - starting from initial snapshot",
    "rateLimit - maxBytesPerTrigger - starting from initial snapshot",
    "rateLimit - maxFilesPerTrigger - should not deadlock",
    "rateLimit - maxBytesPerTrigger - should not deadlock",
    "maxFilesPerTrigger - 2 successive AddCDCFile commits",
    "maxFilesPerTrigger with Trigger.AvailableNow respects read limits",
    "maxFilesPerTrigger - batch reject stops iteration to prevent data loss",

    // ========== Misc tests ==========
    "excludeRegex works with cdc",
    "excludeRegex on cdcPath should not return Add/RemoveFiles",
    "schema check for cdc stream",
    "should not attempt to read a non exist version"
  )

  private lazy val shouldFailTests = Set(
    // === Error message format differs in V2 (missing [DELTA_VERSION_NOT_FOUND] prefix) ===
    "starting[Version/Timestamp] > latest version",
    // === V2 doesn't support allowOutOfRange for startingVersion (follow-up PR) ===
    "check starting[Version/Timestamp] > latest version without error",
    // === sql("DELETE FROM delta.`...`") not supported under STRICT V2 mode ===
    "double delete-only on the same file"
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
