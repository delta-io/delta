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

package org.apache.spark.sql.delta.test.shims

/**
 * Test shim for UNSUPPORTED_FEATURE.TABLE_OPERATION error codes that changed between
 * Spark versions. In Spark 4.2, the error code is UNSUPPORTED_FEATURE.TABLE_OPERATION
 * (same as Spark 4.1)
 */
object UnsupportedTableOperationErrorShims {
  val UNSUPPORTED_TABLE_OPERATION_ERROR_CODE: String = "UNSUPPORTED_FEATURE.TABLE_OPERATION"

  /**
   * Returns the parameters map for UPDATE TABLE error in Spark 4.2 (same as Spark 4.1)
   * @param tableSQLIdentifier The table identifier (e.g., "test_delta_table")
   */
  def updateTableErrorParameters(tableSQLIdentifier: String): Map[String, String] = {
    // Construct the full table name with catalog prefix
    val fullTableName = s"`spark_catalog`.`default`.`$tableSQLIdentifier`"
    Map(
      "tableName" -> fullTableName,
      "operation" -> "UPDATE TABLE")
  }
}

