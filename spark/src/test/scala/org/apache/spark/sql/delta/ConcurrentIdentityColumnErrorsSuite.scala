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

package org.apache.spark.sql.delta

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for the internal CIC reservation-guard error builders in
 * [[ConcurrentIdentityColumnErrors]]. EMPTY_RESERVE_RANGE, METADATA_MISMATCH, and
 * USING_WRONG_GENERATOR are invariants/backstops that a wired write path is not expected to hit,
 * so they are verified directly on the builder output with `checkError` (error class, SQLSTATE,
 * and parameters , never the message prose). The user-triggerable variants
 * (CONVERSION_INCOMPLETE, DISABLED, SEQUENCE_NOT_FOUND) are exercised through their real paths in
 * ConcurrentIdentityColumnServiceBackendSuite and ConcurrentIdentityColumnConversionSuite.
 */
class ConcurrentIdentityColumnErrorsSuite extends SparkFunSuite {

  test("emptyReserveRange reports EMPTY_RESERVE_RANGE with its parameters") {
    checkError(
      ConcurrentIdentityColumnErrors.emptyReserveRange(sequenceId = "seq-1", tableId = "tbl-1"),
      condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_EMPTY_RESERVE_RANGE",
      sqlState = "XXKDS",
      parameters = Map("sequenceId" -> "seq-1", "tableId" -> "tbl-1"))
  }

  test("metadataMismatch reports METADATA_MISMATCH with its parameters") {
    checkError(
      ConcurrentIdentityColumnErrors.metadataMismatch(
        grantedStep = 7L, sequenceId = "seq-1", columnStep = 3L),
      condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_METADATA_MISMATCH",
      sqlState = "XXKDS",
      parameters = Map(
        "property" -> "step",
        "grantedStep" -> "7",
        "sequenceId" -> "seq-1",
        "columnStep" -> "3"))
  }

  test("usingWrongGenerator reports USING_WRONG_GENERATOR with its parameters") {
    checkError(
      ConcurrentIdentityColumnErrors.usingWrongGenerator(
        columnNames = Seq("c1", "c2"), tableId = "tbl-1"),
      condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_USING_WRONG_GENERATOR",
      sqlState = "XXKDS",
      parameters = Map("columnNames" -> "c1, c2", "tableId" -> "tbl-1"))
  }
}
