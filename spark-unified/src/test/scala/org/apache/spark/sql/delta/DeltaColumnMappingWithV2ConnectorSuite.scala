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
 * Test suite that runs DeltaColumnMappingSuite with Delta V2 connector mode forced to STRICT.
 *
 * Tests that are expected to fail with the V2 connector are listed in `shouldFailTests` and
 * converted to ignored tests. This list should be updated as V2 connector support improves.
 */
class DeltaColumnMappingWithV2ConnectorSuite
  extends DeltaColumnMappingSuite
  with V2ForceTest {

  override protected def shouldFail(testName: String): Boolean = {
    shouldFailTests.contains(testName)
  }

  // Note: must be lazy val to avoid NPE during superclass constructor initialization,
  // since V2ForceTest.test() is called before subclass fields are initialized.
  // Tests expected to fail because they use V1-specific internals, unsupported DDL,
  // or features not yet implemented in the V2 connector.
  private lazy val shouldFailTests = Set(
    // --- V1 internal API tests (DeltaLog, DeltaTableV2, internal properties) ---
    "Enable column mapping with schema change on table with no schema",
    "update column mapped table invalid max id property is blocked",
    "should block CM upgrade when commit has FileActions and CDF enabled",
    "upgrade with dot column name should not be blocked",
    "explicit id matching",
    "verify internal table properties only if property exists in spec and existing metadata",
    "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES exception should include column names",
    "enabling column mapping disallowed if column mapping metadata already exists",
    "unit test physical name assigning is case-insensitive",
    "Illegal null value specified for delta.columnMapping.mode option",
    "try modifying restricted max id property should fail",
    "isColumnMappingReadCompatible",
    "is drop/rename column operation",
    "getPhysicalNameFieldMap",

    // --- Table creation tests (V1 DeltaLog-based schema checking) ---
    "create table through raw schema API should auto bump the version and retain input metadata",
    "create table through dataframe should auto bumps the version and rebuild " +
      "schema metadata/drop dataframe metadata",
    "create table with none mode",
    "create table in column mapping mode without defining ids explicitly",

    // --- Schema evolution / ALTER TABLE tests (V1-specific internal paths) ---
    "alter column order in schema on new protocol",
    "add column in schema on new protocol",
    "add nested column in schema on new protocol",
    "change mode on new protocol table",
    "upgrade first and then change mode",
    "upgrade and change mode in one ALTER TABLE cmd",
    "illegal mode changes",
    "column mapping upgrade with table features",

    // --- Write/merge/overwrite tests that use V1-specific partitioned write paths ---
    "write/merge df to table",
    "overwrite a column mapping table should preserve column mapping metadata",

    // --- CONVERT TO DELTA not supported in V2 ---
    "block CONVERT TO DELTA",

    // --- CDF + column mapping interaction (V1 internals) ---
    "CDF and Column Mapping: should block when CDF=true",
    "CDF and Column Mapping: should not block when CDF=false",

    // --- RESTORE not supported in V2 ---
    "restore Delta table with name column mapping enabled",

    // --- External table / REPLACE TABLE operations not supported in V2 ---
    "drop and recreate external Delta table with name column mapping enabled",
    "replace external Delta table with name column mapping enabled",
    "replace delta table will reuse the field id only when column name and type unchanged",
    "replace delta table will not reuse the field id when name mapping mode changed",

    // --- Read path tests that depend on V1-specific filter push-down or streaming internals ---
    "filters pushed down to parquet use physical names",
    "stream read from column mapping does not leak metadata",

    // --- Column mapping metadata stripping tests (V1 internals) ---
    "column mapping metadata are stripped when feature is disabled - name",
    "column mapping metadata are stripped when feature is disabled - id"
  )
}
