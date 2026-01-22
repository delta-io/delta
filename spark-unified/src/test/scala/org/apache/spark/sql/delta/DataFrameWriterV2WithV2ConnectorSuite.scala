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

import org.apache.spark.sql.delta.test.V2ForceTest

/**
 * Test suite that runs OpenSourceDataFrameWriterV2Tests with Delta V2 connector
 * mode forced to STRICT.
 */
class DataFrameWriterV2WithV2ConnectorSuite
  extends OpenSourceDataFrameWriterV2Tests
  with V2ForceTest {

  /**
   * Tests that we expect to fail because they require write operations after initial
   * table creation.
   *
   * Kernel's SparkTable (V2 connector) only implements SupportsRead, not SupportsWrite.
   * Tests that perform append/replace operations after table creation are expected to fail.
   */
  override protected def shouldFail(testName: String): Boolean = {
    val shouldFailTests = Set(
      // Append operations - require SupportsWrite
      "Append: basic append",
      "Append: by name not position",

      // Overwrite operations - require SupportsWrite
      "Overwrite: overwrite by expression: true",
      "Overwrite: overwrite by expression: id = 3",
      "Overwrite: by name not position",

      // OverwritePartitions operations - require SupportsWrite
      "OverwritePartitions: overwrite conflicting partitions",
      "OverwritePartitions: overwrite all rows if not partitioned",
      "OverwritePartitions: by name not position",

      // Create operations - TODO: fix SparkTable's name() to match DeltaTableV2
      // SparkTable.name() returns simple table name, but tests expect catalog.schema.table format
      "Create: basic behavior",
      "Create: with using",
      "Create: with property",
      "Create: identity partitioned table",
      "Create: fail if table already exists",

      // Replace operations - require SupportsWrite
      "Replace: basic behavior",
      "Replace: partitioned table",

      // CreateOrReplace operations - require SupportsWrite
      "CreateOrReplace: table does not exist",
      "CreateOrReplace: table exists"
    )

    shouldFailTests.contains(testName)
  }
}
