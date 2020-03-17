/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.sources

import java.util.Locale

object DeltaSourceUtils {
  val NAME = "delta"
  val ALT_NAME = "delta"

  // Batch relations don't pass partitioning columns to `CreatableRelationProvider`s, therefore
  // as a hack, we pass in the partitioning columns among the options.
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  def isDeltaDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME || name.toLowerCase(Locale.ROOT) == ALT_NAME
  }

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(provider: Option[String]): Boolean = {
    provider.exists(isDeltaDataSourceName)
  }
}
