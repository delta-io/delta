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

package org.apache.spark.sql.delta.test.columnmapping

import org.apache.spark.sql.delta.columnmapping.RemoveColumnMappingStreamingReadSuite
import org.apache.spark.sql.delta.test.V2ForceTest

/**
 * Test suite that runs [[RemoveColumnMappingStreamingReadSuite]] using the V2 connector
 * (V2_ENABLE_MODE=STRICT).
 *
 * SparkTable (V2) is read-only and does not support DDL, so DDL/DML operations are routed
 * through the V1 connector via `executeDml`. Only streaming reads use the V2 connector.
 */
class RemoveColumnMappingStreamingReadV2Suite
  extends RemoveColumnMappingStreamingReadSuite
    with V2ForceTest {

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def shouldPassTests: Set[String] = Set(
    // Tests that run without schema tracking.
    "Upgrade, StartStreamRead, Downgrade, FailNonAdditiveChange",
    "Upgrade, Downgrade, StartStreamRead, Success",
    "StartStreamRead, Upgrade, Rename, Downgrade, FailNonAdditiveChange",
    "StartStreamRead, Upgrade, Drop, Downgrade, FailNonAdditiveChange",
    "StartStreamRead, Upgrade, Rename, Downgrade, Upgrade, FailNonAdditiveChange",
    "StartStreamRead, Upgrade, Drop, Downgrade, Upgrade, FailNonAdditiveChange",
    "Upgrade, StartStreamRead, Rename, Downgrade, FailNonAdditiveChange",
    "Upgrade, StartStreamRead, Drop, Downgrade, FailNonAdditiveChange",
    "Upgrade, StartStreamRead, Rename, Downgrade, Upgrade, FailNonAdditiveChange",
    "Upgrade, StartStreamRead, Drop, Downgrade, Upgrade, FailNonAdditiveChange",
    "Upgrade, Rename, StartStreamRead, Downgrade, FailNonAdditiveChange",
    "Upgrade, Rename, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange",
    "Upgrade, Drop, StartStreamRead, Downgrade, FailNonAdditiveChange",
    "Upgrade, Drop, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange",
    "Upgrade, Rename, Downgrade, StartStreamRead, Success",
    "Upgrade, Drop, Downgrade, StartStreamRead, Success",
    "Upgrade, Rename, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking",
    "Upgrade, Drop, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking",
    "StartStreamRead, Upgrade, Downgrade, SuccessAndFailSchemaTracking",

    // Tests that run with schema tracking enabled.
    "StartStreamRead, Upgrade, Downgrade, SuccessAndFailSchemaTracking with schema tracking",
    "Upgrade, StartStreamRead, Downgrade, FailNonAdditiveChange with schema tracking",
    "Upgrade, Downgrade, StartStreamRead, Success with schema tracking",
    "StartStreamRead, Upgrade, Rename, Downgrade, FailNonAdditiveChange with schema tracking",
    "StartStreamRead, Upgrade, Drop, Downgrade, FailNonAdditiveChange with schema tracking",
    "StartStreamRead, Upgrade, Rename, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "StartStreamRead, Upgrade, Drop, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "Upgrade, StartStreamRead, Rename, Downgrade, FailNonAdditiveChange with schema tracking",
    "Upgrade, StartStreamRead, Drop, Downgrade, FailNonAdditiveChange with schema tracking",
    "Upgrade, StartStreamRead, Rename, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "Upgrade, StartStreamRead, Drop, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "Upgrade, Rename, StartStreamRead, Downgrade, FailNonAdditiveChange with schema tracking",
    "Upgrade, Rename, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "Upgrade, Drop, StartStreamRead, Downgrade, FailNonAdditiveChange with schema tracking",
    "Upgrade, Drop, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange" +
      " with schema tracking",
    "Upgrade, Rename, Downgrade, StartStreamRead, Success with schema tracking",
    "Upgrade, Drop, Downgrade, StartStreamRead, Success with schema tracking",
    "Upgrade, Rename, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking" +
      " with schema tracking",
    "Upgrade, Drop, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking" +
      " with schema tracking")
}
