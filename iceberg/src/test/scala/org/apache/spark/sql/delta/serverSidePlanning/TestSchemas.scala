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

package org.apache.spark.sql.delta.serverSidePlanning

import shadedForDelta.org.apache.iceberg.Schema
import shadedForDelta.org.apache.iceberg.types.Types

/**
 * Shared test schemas used across server-side planning filtering and projection test suites.
 */
object TestSchemas {
  /**
   * Standard test schema with common field types used for testing filters and projections.
   * Includes: id (long), name (string), age (int), price (double), rating (float), active (boolean)
   */
  val defaultSchema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get),
    Types.NestedField.required(2, "name", Types.StringType.get),
    Types.NestedField.required(3, "age", Types.IntegerType.get),
    Types.NestedField.required(4, "price", Types.DoubleType.get),
    Types.NestedField.required(5, "rating", Types.FloatType.get),
    Types.NestedField.required(6, "active", Types.BooleanType.get))
}

