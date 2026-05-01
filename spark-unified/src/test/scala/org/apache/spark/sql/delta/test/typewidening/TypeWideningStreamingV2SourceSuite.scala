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
 *
 * The base lists every test from `TypeWideningStreamingSourceTests` as passing: V2 supports
 * type-widening streaming reads. Subclasses move specific tests back to `shouldFailTests` when
 * there is a concrete V2 limitation (e.g., partition-column schema bug, missing event logging).
 */
trait TypeWideningStreamingV2SourceSuiteBase extends V2ForceTest {
  self: TypeWideningStreamingSourceTestMixin =>

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def shouldPassTests: Set[String] = Set(
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

  // Failures that affect both the schema-tracking and non-schema-tracking suites.
  override protected def shouldFailTests: Set[String] = Set(
    // Delta log event is not supported in V2, so event-logging tests are not meaningful.
    "schema changed event is logged for type widening",
    "schema changed event is not logged when there are no schema changes"
  )
}

class TypeWideningStreamingV2SourceSuite
  extends TypeWideningStreamingSourceSuite
    with TypeWideningStreamingV2SourceSuiteBase

class TypeWideningStreamingV2SourceSchemaTrackingSuite
  extends TypeWideningStreamingSourceSchemaTrackingSuite
    with TypeWideningStreamingV2SourceSuiteBase {

  // Schema-tracking-specific tests from TypeWideningStreamingSourceSchemaTrackingTests, on top of
  // the base type-widening tests inherited from the trait, minus tests with known V2 issues.
  override protected def shouldPassTests: Set[String] =
    super.shouldPassTests ++ Set(
      "type change first without schemaTrackingLocation and unblock using schemaTrackingLocation",
      "unblocking stream with sql conf after type change - unblock all",
      "unblocking stream with sql conf after type change - unblock stream",
      "unblocking stream with sql conf after type change - unblock version",
      "unblocking stream with reader option after type change - unblock stream",
      "unblocking stream with reader option after type change - unblock version",
      "overwrite schema with type change and dropped column",
      "disable schema tracking log using internal conf"
    ) -- shouldFailTests

  // TODO(#5319): Move to PASS after consecutive schema merger is supported
  override  protected def shouldFailTests: Set[String] =
    super.shouldFailTests ++ Set(
      "type change in delta source writing to a delta sink"
    )
}
