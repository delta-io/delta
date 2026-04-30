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

import org.apache.spark.sql.delta.{
  DeltaLog,
  DeltaSourceSchemaEvolutionCDCIdColumnMappingSuite,
  DeltaSourceSchemaEvolutionCDCNameColumnMappingSuite,
  DeltaSourceSchemaEvolutionIdColumnMappingSuite,
  DeltaSourceSchemaEvolutionNameColumnMappingSuite,
  StreamingSchemaEvolutionSuiteBase
}

/**
 * Test suite that runs DeltaSourceSchemaEvolutionSuite using the V2 connector
 * (V2_ENABLE_MODE=STRICT).
 */

/**
 * Base trait for V2 schema evolution streaming tests.
 * Provides common overrides shared by all V2 schema evolution suites.
 */
trait DeltaV2SourceSchemaEvolutionSuiteBase extends V2ForceTest {
  self: StreamingSchemaEvolutionSuiteBase =>

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  // V2ForceTest handles test selection via shouldFail/shouldPass,
  // so disable DeltaColumnMappingSelectedTestMixin's runOnlyTests filter.
  override protected def runAllTests: Boolean = true
  override protected def runOnlyTests: Seq[String] = Seq()

  // Override testSchemaEvolution to use test() instead of super.test() so that
  // V2ForceTest.test() can intercept and apply shouldFail logic.
  // In the V1 base, testSchemaEvolution calls super.test() which resolves to
  // DeltaColumnMappingSelectedTestMixin.test() in the linearization, bypassing
  // V2ForceTest.test(). Using test() ensures the full override chain is invoked.
  override protected def testSchemaEvolution(
      testName: String,
      columnMapping: Boolean = true,
      tags: Seq[org.scalatest.Tag] = Seq.empty)(f: DeltaLog => Unit): Unit = {
    test(testName, tags: _*) {
      if (columnMapping) {
        withStarterTable { log =>
          f(log)
        }
      } else {
        withColumnMappingConf("none") {
          withStarterTable { log =>
            f(log)
          }
        }
      }
    }
  }

  // TODO(#5319): Move tests to shouldPassTests as V2 schema tracking log support is implemented.
  override protected def shouldPassTests: Set[String] = Set(
    // ========== Schema log unit test ==========
    "schema location not under checkpoint",
    "schema location same as checkpoint",
    "schema location using a different file system",
    "schema / checkpoint location unit tests - " +
      "checkpoint location and schema location are the same",
    "schema / checkpoint location unit tests - " +
      "schema location is under checkpoint location",
    "schema / checkpoint location unit tests - " +
      "schema location is not under checkpoint location",
    "schema / checkpoint location unit tests - " +
      "schema location and checkpoint location are on different file systems",
    "schema / checkpoint location unit tests - " +
      "schema location and checkpoint location are the same but with explicit file scheme",
    "schema / checkpoint location unit tests - special characters in schema location",
    "concurrent schema log modification should be detected",
    "schema log replace current",
    "backward-compat: latest version can read back older JSON",
    "forward-compat: older version can read back newer JSON",

    // ========== Schema log core ==========
    "multiple delta source sharing same schema log is blocked"
  )

  override protected def shouldFailTests: Set[String] = Set(
    // ========== Schema log core ==========
    "schema log is applied",
    "schema log initialization with additive schema changes",
    "detect incompatible schema change while streaming",
    "detect incompatible schema change during first getBatch",
    "identity columns shouldn't cause schema mismatches",
    "detect invalid offset during initialization before " +
      "initializing schema log - rename",
    "detect invalid offset during initialization before " +
      "initializing schema log - drop",
    "no need to block schema log initialization if " +
      "constructed batch ends on schema change",
    "resolve the most encompassing schema during getBatch " +
      "to initialize schema log",

    // ========== Trigger modes ==========
    "trigger.Once with deferred commit should work",
    "trigger.AvailableNow should work",

    // ========== Schema evolution scenarios ==========
    "consecutive schema evolutions without schema merging",
    "consecutive schema evolutions",
    "upgrade and downgrade",
    "multiple sources with schema evolution",
    "schema evolution with Delta sink",
    "latestOffset should not progress before schema evolved",
    "unblock with sql conf",
    "schema tracking interacting with unsafe escape flag",
    "streaming with a column mapping upgrade",
    "partition evolution"
  )
}

// Non-CDC suites

class DeltaV2SourceSchemaEvolutionNameColumnMappingSuite
  extends DeltaSourceSchemaEvolutionNameColumnMappingSuite
    with DeltaV2SourceSchemaEvolutionSuiteBase

class DeltaV2SourceSchemaEvolutionIdColumnMappingSuite
  extends DeltaSourceSchemaEvolutionIdColumnMappingSuite
    with DeltaV2SourceSchemaEvolutionSuiteBase

// CDC suites
// TODO(#5319): Support CDC non-additive schema evolution
trait DeltaV2SourceSchemaEvolutionCDCSuiteBase extends DeltaV2SourceSchemaEvolutionSuiteBase {
  self: StreamingSchemaEvolutionSuiteBase =>

  override protected def shouldPassTests: Set[String] = Set.empty[String]

  override protected def shouldFailTests: Set[String] =
    super.shouldPassTests ++ super.shouldFailTests ++ Set(
      // Additional tests from CDCStreamingSchemaEvolutionSuiteBase
      "CDC streaming with schema evolution",
      "protocol and configuration evolution"
    )
}

class DeltaV2SourceSchemaEvolutionCDCNameColumnMappingSuite
  extends DeltaSourceSchemaEvolutionCDCNameColumnMappingSuite
    with DeltaV2SourceSchemaEvolutionCDCSuiteBase

class DeltaV2SourceSchemaEvolutionCDCIdColumnMappingSuite
  extends DeltaSourceSchemaEvolutionCDCIdColumnMappingSuite
    with DeltaV2SourceSchemaEvolutionCDCSuiteBase
