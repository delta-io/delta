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

  /**
   * Comprehensive test schema with all major data types for extensive filter testing.
   * Includes: intCol, longCol, doubleCol, floatCol, stringCol, boolCol, decimalCol,
   * dateCol, timestampCol
   */
  val comprehensiveSchema = new Schema(
    Types.NestedField.required(1, "intCol", Types.IntegerType.get),
    Types.NestedField.required(2, "longCol", Types.LongType.get),
    Types.NestedField.required(3, "doubleCol", Types.DoubleType.get),
    Types.NestedField.required(4, "floatCol", Types.FloatType.get),
    Types.NestedField.required(5, "stringCol", Types.StringType.get),
    Types.NestedField.required(6, "boolCol", Types.BooleanType.get),
    Types.NestedField.required(7, "decimalCol", Types.DecimalType.of(10, 2)),
    Types.NestedField.required(8, "dateCol", Types.DateType.get),
    Types.NestedField.required(9, "timestampCol", Types.TimestampType.withoutZone))

  /**
   * Nested schema with struct fields for testing nested column access.
   * Structure:
   * - address (struct):
   *   - intCol, longCol, doubleCol, floatCol
   * - metadata (struct):
   *   - stringCol, boolCol, decimalCol, dateCol, timestampCol
   */
  val nestedSchema = new Schema(
    Types.NestedField.required(1, "address", Types.StructType.of(
      Types.NestedField.required(11, "intCol", Types.IntegerType.get),
      Types.NestedField.required(12, "longCol", Types.LongType.get),
      Types.NestedField.required(13, "doubleCol", Types.DoubleType.get),
      Types.NestedField.required(14, "floatCol", Types.FloatType.get)
    )),
    Types.NestedField.required(2, "metadata", Types.StructType.of(
      Types.NestedField.required(21, "stringCol", Types.StringType.get),
      Types.NestedField.required(22, "boolCol", Types.BooleanType.get),
      Types.NestedField.required(23, "decimalCol", Types.DecimalType.of(10, 2)),
      Types.NestedField.required(24, "dateCol", Types.DateType.get),
      Types.NestedField.required(25, "timestampCol", Types.TimestampType.withoutZone)
    ))
  )

  /**
   * Comprehensive schema with flat fields and minimal nested examples.
   * Used for comprehensive filter conversion testing.
   *
   * Structure:
   * - Flat fields: intCol, longCol, doubleCol, floatCol, stringCol, boolCol,
   *                decimalCol, dateCol, timestampCol
   * - Nested examples (to test string pass-through):
   *   - address.intCol (one numeric nested example)
   *   - metadata.stringCol (one non-numeric nested example)
   */
  val comprehensiveSchemaWithNesting = new Schema(
    // Flat fields (IDs 1-9)
    Types.NestedField.required(1, "intCol", Types.IntegerType.get),
    Types.NestedField.required(2, "longCol", Types.LongType.get),
    Types.NestedField.required(3, "doubleCol", Types.DoubleType.get),
    Types.NestedField.required(4, "floatCol", Types.FloatType.get),
    Types.NestedField.required(5, "stringCol", Types.StringType.get),
    Types.NestedField.required(6, "boolCol", Types.BooleanType.get),
    Types.NestedField.required(7, "decimalCol", Types.DecimalType.of(10, 2)),
    Types.NestedField.required(8, "dateCol", Types.DateType.get),
    Types.NestedField.required(9, "timestampCol", Types.TimestampType.withoutZone),

    // Minimal nested examples (IDs 10-11)
    Types.NestedField.required(10, "address", Types.StructType.of(
      Types.NestedField.required(101, "intCol", Types.IntegerType.get)
    )),

    Types.NestedField.required(11, "metadata", Types.StructType.of(
      Types.NestedField.required(111, "stringCol", Types.StringType.get)
    ))
  )
}

