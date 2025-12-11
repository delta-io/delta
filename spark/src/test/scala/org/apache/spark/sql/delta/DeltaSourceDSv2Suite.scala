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
    "startingVersion",
    "startingVersion latest",
    "startingVersion latest defined before started",
    "startingVersion latest works on defined but empty table",
    "new commits arrive after stream initialization with startingVersion"
  )

  override protected def shouldSkipTest(testName: String): Boolean = {
    !testsToRun.contains(testName)
  }
}
