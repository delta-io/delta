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

package org.apache.spark.sql.delta.test.typewidening

import org.apache.spark.sql.delta.test.V2ForceTest
import org.apache.spark.sql.delta.typewidening.{
  TypeWideningStreamingSourceSchemaTrackingSuite,
  TypeWideningStreamingSourceSuite,
  TypeWideningStreamingSourceTestMixin
}

/**
 * Base trait for V2 type widening streaming source tests.
 * Provides common shouldFail logic shared by both suites.
 */
trait TypeWideningStreamingV2SourceSuiteBase extends V2ForceTest {
  self: TypeWideningStreamingSourceTestMixin =>

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  // TODO(#5319): Move tests to shouldPassTests as V2 schema tracking log support is implemented.
  override protected def shouldPassTests: Set[String] = Set.empty[String]

  // Tests from TypeWideningStreamingSourceTests, shared by both suites.
  // Override in subclasses to add suite-specific tests.
  override protected def shouldFailTests: Set[String] = Set(
    "type change - filter",
    "type change - projection",
    "type change - projection partition column",
    "type change - widen unused scala udf field",
    "type change - widen scala udf argument",
    "type change - widen aggregation grouping key",
    "type change - widen aggregation expression",
    "type change - widen aggregation expression partition column",
    "type change - widen aggregation expression after projection",
    "type change - widen limit",
    "type change - widen distinct",
    "type change - widen drop duplicates",
    "type change - widen drop duplicates with watermark",
    "type change - widen flatMap groups with state",
    "widening type change then restore back",
    "narrowing type changes are not supported",
    "arbitrary type changes are not supported",
    "type change in delta source writing to a delta sink"
  )
}

class TypeWideningStreamingV2SourceSuite
  extends TypeWideningStreamingSourceSuite
    with TypeWideningStreamingV2SourceSuiteBase {

  // All tests pass without schema tracking enabled, except where noted in shouldFailTests.
  override protected def shouldPassTests: Set[String] =
    super.shouldFailTests -- shouldFailTests

  override protected def shouldFailTests: Set[String] = Set(
    // Delta log event is not supported in V2, so event-logging tests are not meaningful.
    "schema changed event is logged for type widening",
    "schema changed event is not logged when there are no schema changes",
    // TODO(#5319): Partition column schema has a bug in V2 causing these to fail.
    "type change - projection partition column",
    "type change - widen aggregation expression partition column",
    // TODO(#5319): V2 lacks the implementation of
    //  validateAndInitMetadataLogForPlannedBatchesDuringStreamStart, so the
    //  2nd testStream restart does not throw on the incompatible type change.
    "widening type change then restore back",
    "narrowing type changes are not supported",
    "arbitrary type changes are not supported"
  )
}

class TypeWideningStreamingV2SourceSchemaTrackingSuite
  extends TypeWideningStreamingSourceSchemaTrackingSuite
    with TypeWideningStreamingV2SourceSuiteBase {

  override protected def shouldFailTests: Set[String] = super.shouldFailTests ++ Set(
    // Additional tests from TypeWideningStreamingSourceSchemaTrackingTests
    "type change first without schemaTrackingLocation and unblock using schemaTrackingLocation",
    "unblocking stream with sql conf after type change - unblock all",
    "unblocking stream with sql conf after type change - unblock stream",
    "unblocking stream with sql conf after type change - unblock version",
    "unblocking stream with reader option after type change - unblock stream",
    "unblocking stream with reader option after type change - unblock version",
    "overwrite schema with type change and dropped column",
    "disable schema tracking log using internal conf"
  )
}
