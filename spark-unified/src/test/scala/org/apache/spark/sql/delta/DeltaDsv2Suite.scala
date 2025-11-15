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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.test.Dsv2ForceTest

/**
 * Test suite that runs DeltaSuite tests with DataSourceV2 mode forced to STRICT.
 */
class DeltaDsv2Suite extends DeltaSuite with Dsv2ForceTest {

  /**
   * Skip tests that require write operations or features not yet supported by Kernel.
   *
   * Kernel's SparkTable currently only implements SupportsRead, not SupportsWrite.
   * Tests involving any write operations after initial table creation are skipped.
   */
  override protected def shouldSkipTest(testName: String): Boolean = {
    val skippedTests = Set(
      // NullType schema issues - Kernel not support NullType columns
      "null struct with NullType field kept as null",
      "null struct with NullType field, with backticks in the column name, kept as null",
      
      // Write operations - Kernel SparkTable is read-only
      "get touched files for update, delete and merge",
      
      // Java8 API - requires write operations
      "support Java8 API for DATE type",
      "support Java8 API for TIMESTAMP type",
      
      // Special characters in paths - may involve write issues
      "all operations with special characters in path",
      
      // History/metadata - requires write operations to generate history
      "history includes user-defined metadata for SQL API",
      "SC-77958 - history includes user-defined metadata for createOrReplace",
      "SC-77958 - history includes user-defined metadata for saveAsTable",
      
      // isBlindAppend - requires saveAsTable/write operations
      "isBlindAppend with save and saveAsTable",
      "isBlindAppend with DataFrameWriterV2",
      "isBlindAppend with RTAS",
      
      // Idempotent writes - all require write operations
      "idempotent write: idempotent SQL insert",
      "idempotent write: idempotent SQL merge",
      "idempotent write: idempotent SQL update",
      "idempotent write: idempotent SQL delete",
      "idempotent write: auto reset txnVersion",
      "Idempotent non-legacy Dataframe saveAsTable: append"
    )

    skippedTests.contains(testName)
  }
}

