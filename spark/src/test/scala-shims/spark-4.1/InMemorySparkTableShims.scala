/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.connector.catalog.BufferedRows
import org.apache.spark.sql.types.StructType

/** Shim for [[InMemorySparkTable]] - exposes APIs that differ between Spark versions. */
object InMemorySparkTableShims {
  // [[InMemoryBaseTable.alterTableWithData]] is available in Spark 4.1, but schema evolution is
  // not properly supported in that version anyway, so fail.
  def migrateData(
      table: InMemorySparkTable,
      data: Array[BufferedRows],
      newSchema: StructType): Unit = {
    throw new UnsupportedOperationException(
      "Schema evolution on InMemorySparkTable is not supported on Spark 4.1")
  }
}
