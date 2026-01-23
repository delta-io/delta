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

package org.apache.spark.sql.delta.serverSidePlanning

import shadedForDelta.org.apache.iceberg.Schema
import shadedForDelta.org.apache.iceberg.types.Types
import org.apache.spark.sql.types._

private[serverSidePlanning] object TestSchemas {
  /**
   * Shared test schema used across all server-side planning test suites.
   * Structure:
   * - Flat fields (9 types): intCol, longCol, doubleCol, floatCol, stringCol, boolCol,
   *                          decimalCol, dateCol, timestampCol
   * - Nested struct (ID 10): address with intCol - tests nested field access
   * - Nested struct (ID 11): metadata with stringCol - tests nested string field
   * - Nested struct with dotted field (ID 12): parent with "child.name" - tests escaping at
   *   nested level
   * - Literal top-level dotted columns (IDs 13-14): address.city, a.b.c - tests top-level
   *   escaping
   */
  val testSchema = new Schema(
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

    // Nested struct for testing nested field access (ID 10)
    Types.NestedField.required(10, "address", Types.StructType.of(
      Types.NestedField.required(101, "intCol", Types.IntegerType.get)
    )),

    // Nested struct for testing nested string field (ID 11)
    Types.NestedField.required(11, "metadata", Types.StructType.of(
      Types.NestedField.required(111, "stringCol", Types.StringType.get)
    )),

    // Nested struct with field that has dots in its name (ID 12)
    // Tests escaping at nested level: parent.`child.name`
    Types.NestedField.required(12, "parent", Types.StructType.of(
      Types.NestedField.required(121, "child.name", Types.StringType.get)
    )),

    // Literal top-level column names with dots (IDs 13-14) - Test escaping
    Types.NestedField.required(13, "address.city", Types.StringType.get),
    Types.NestedField.required(14, "a.b.c", Types.StringType.get)
  )

  /**
   * Spark StructType corresponding to the testSchema above.
   * Used for filter conversion in tests.
   */
  val sparkSchema: StructType = StructType(Seq(
    StructField("intCol", IntegerType, nullable = false),
    StructField("longCol", LongType, nullable = false),
    StructField("doubleCol", DoubleType, nullable = false),
    StructField("floatCol", FloatType, nullable = false),
    StructField("stringCol", StringType, nullable = false),
    StructField("boolCol", BooleanType, nullable = false),
    StructField("decimalCol", DecimalType(10, 2), nullable = false),
    StructField("dateCol", DateType, nullable = false),
    StructField("timestampCol", TimestampType, nullable = false),
    // Nested struct for testing nested field access
    StructField("address", StructType(Seq(
      StructField("intCol", IntegerType, nullable = false)
    )), nullable = false),
    // Nested struct for testing nested string field
    StructField("metadata", StructType(Seq(
      StructField("stringCol", StringType, nullable = false)
    )), nullable = false),
    // Nested struct with field that has dots in its name
    // Tests escaping at nested level: parent.`child.name`
    StructField("parent", StructType(Seq(
      StructField("child.name", StringType, nullable = false)
    )), nullable = false),
    // Literal top-level column names with dots - Test escaping
    StructField("address.city", StringType, nullable = false),
    StructField("a.b.c", StringType, nullable = false)
  ))
}

