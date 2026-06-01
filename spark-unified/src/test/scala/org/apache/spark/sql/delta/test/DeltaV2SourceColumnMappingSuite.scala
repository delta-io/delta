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

import org.apache.spark.sql.delta.ColumnMappingStreamingBlockedWorkflowSuiteBase
import org.apache.spark.sql.delta.DeltaSourceIdColumnMappingSuite
import org.apache.spark.sql.delta.DeltaSourceNameColumnMappingSuite
import org.apache.spark.sql.delta.DeltaSourceSuite

/**
 * Test suite that runs DeltaSourceColumnMappingSuite using the V2 connector
 * (V2_ENABLE_MODE=STRICT).
 *
 * SparkTable (V2) is read-only and does not support DDL, so DDL/DML operations are routed
 * through the V1 connector via `executeDml`. Only streaming reads use the V2 connector.
 *
 * The pass/fail sets compose [[DeltaV2SourceSuite.PassingTests]] and
 * [[DeltaV2SourceSuite.FailingTests]] with the column-mapping-specific tests added to the
 * pass list - we want all of them to pass eventually, so any real failure should surface as a
 * test failure rather than be silenced via shouldFailTests.
 */
trait DeltaV2SourceColumnMappingSuiteBase
  extends V2ForceTest
    with DeltaColumnMappingSelectedTestMixin {
  self: ColumnMappingStreamingBlockedWorkflowSuiteBase with DeltaSourceSuite =>

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  // V1's selection - DeltaV2SourceSuite already covers the rest, no point rerunning them
  // under column mapping.
  override protected def runOnlyTests: Seq[String] = Seq(
    "basic",
    "maxBytesPerTrigger: metadata checkpoint",
    "maxFilesPerTrigger: metadata checkpoint",
    "allow to change schema before starting a streaming query",
    "column mapping + streaming - allowed workflows - column addition",
    "column mapping + streaming - allowed workflows - upgrade to name mode",
    "column mapping + streaming: blocking workflow - drop column",
    "column mapping + streaming: blocking workflow - rename column",
    "column mapping + streaming: blocking workflow - " +
      "should not generate latestOffset past schema change"
  )

  override protected def shouldPassTests: Set[String] =
    DeltaV2SourceSuite.PassingTests ++ Set(
      // Column-mapping-only blocked-workflow tests from
      // [[ColumnMappingStreamingBlockedWorkflowSuiteBase]].
      "column mapping + streaming - allowed workflows - column addition",
      "column mapping + streaming - allowed workflows - upgrade to name mode",
      "column mapping + streaming: blocking workflow - drop column",
      "column mapping + streaming: blocking workflow - rename column",
      "column mapping + streaming: blocking workflow - " +
        "should not generate latestOffset past schema change",
      "unsafe flag can unblock drop or rename column"
    )

  override protected def shouldFailTests: Set[String] = DeltaV2SourceSuite.FailingTests ++ Set(
    // V2 source doesn't use delta log.
    "deltaLog snapshot should not be updated outside of the stream"
  )
}

class DeltaV2SourceIdColumnMappingSuite
  extends DeltaSourceIdColumnMappingSuite
    with DeltaV2SourceColumnMappingSuiteBase

class DeltaV2SourceNameColumnMappingSuite
  extends DeltaSourceNameColumnMappingSuite
    with DeltaV2SourceColumnMappingSuiteBase
