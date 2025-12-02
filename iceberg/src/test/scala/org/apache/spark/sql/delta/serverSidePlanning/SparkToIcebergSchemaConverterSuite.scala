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
import shadedForDelta.org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.SharedSparkSession

class SparkToIcebergSchemaConverterSuite extends SharedSparkSession with DeltaSQLTestUtils {

  test("convert simple primitive types") {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("active", BooleanType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.columns().size() == 3)
    assert(schema.findField("id").`type`() == Types.IntegerType.get())
    assert(schema.findField("id").isRequired)
    assert(schema.findField("name").`type`() == Types.StringType.get())
    assert(schema.findField("name").isOptional)
    assert(schema.findField("active").`type`() == Types.BooleanType.get())
    assert(schema.findField("active").isRequired)
  }

  test("convert all numeric types") {
    val sparkSchema = StructType(Seq(
      StructField("byte_col", ByteType, nullable = false),
      StructField("short_col", ShortType, nullable = false),
      StructField("int_col", IntegerType, nullable = false),
      StructField("long_col", LongType, nullable = false),
      StructField("float_col", FloatType, nullable = false),
      StructField("double_col", DoubleType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    // Byte and Short map to Integer in Iceberg
    assert(schema.findField("byte_col").`type`() == Types.IntegerType.get())
    assert(schema.findField("short_col").`type`() == Types.IntegerType.get())
    assert(schema.findField("int_col").`type`() == Types.IntegerType.get())
    assert(schema.findField("long_col").`type`() == Types.LongType.get())
    assert(schema.findField("float_col").`type`() == Types.FloatType.get())
    assert(schema.findField("double_col").`type`() == Types.DoubleType.get())
  }

  test("convert date and timestamp types") {
    val sparkSchema = StructType(Seq(
      StructField("date_col", DateType, nullable = false),
      StructField("timestamp_col", TimestampType, nullable = false),
      StructField("timestamp_ntz_col", TimestampNTZType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.findField("date_col").`type`() == Types.DateType.get())
    assert(schema.findField("timestamp_col").`type`() == Types.TimestampType.withZone())
    assert(schema.findField("timestamp_ntz_col").`type`() == Types.TimestampType.withoutZone())
  }

  test("convert decimal type with precision and scale") {
    val sparkSchema = StructType(Seq(
      StructField("price", DecimalType(10, 2), nullable = false),
      StructField("amount", DecimalType(38, 18), nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    val priceType = schema.findField("price").`type`().asInstanceOf[Types.DecimalType]
    assert(priceType.precision() == 10)
    assert(priceType.scale() == 2)
    assert(schema.findField("price").isRequired)

    val amountType = schema.findField("amount").`type`().asInstanceOf[Types.DecimalType]
    assert(amountType.precision() == 38)
    assert(amountType.scale() == 18)
    assert(schema.findField("amount").isOptional)
  }

  test("convert binary and string types") {
    val sparkSchema = StructType(Seq(
      StructField("data", BinaryType, nullable = false),
      StructField("text", StringType, nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.findField("data").`type`() == Types.BinaryType.get())
    assert(schema.findField("text").`type`() == Types.StringType.get())
  }

  test("convert nested struct type") {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("address", StructType(Seq(
        StructField("street", StringType, nullable = true),
        StructField("city", StringType, nullable = true),
        StructField("zip", IntegerType, nullable = false)
      )), nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.findField("id").`type`() == Types.IntegerType.get())

    val addressType = schema.findField("address").`type`().asInstanceOf[Types.StructType]
    assert(addressType.fields().size() == 3)
    assert(addressType.field("street").`type`() == Types.StringType.get())
    assert(addressType.field("street").isOptional)
    assert(addressType.field("city").`type`() == Types.StringType.get())
    assert(addressType.field("zip").`type`() == Types.IntegerType.get())
    assert(addressType.field("zip").isRequired)
  }

  test("convert array type with required elements") {
    val sparkSchema = StructType(Seq(
      StructField("tags", ArrayType(StringType, containsNull = false), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    val tagsType = schema.findField("tags").`type`().asInstanceOf[Types.ListType]
    assert(tagsType.elementType() == Types.StringType.get())
    assert(tagsType.isElementRequired)
    assert(schema.findField("tags").isRequired)
  }

  test("convert array type with optional elements") {
    val sparkSchema = StructType(Seq(
      StructField("scores", ArrayType(IntegerType, containsNull = true), nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    val scoresType = schema.findField("scores").`type`().asInstanceOf[Types.ListType]
    assert(scoresType.elementType() == Types.IntegerType.get())
    assert(scoresType.isElementOptional)
    assert(schema.findField("scores").isOptional)
  }

  test("convert array of structs") {
    val sparkSchema = StructType(Seq(
      StructField("items", ArrayType(
        StructType(Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = true)
        )), containsNull = false
      ), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    val itemsType = schema.findField("items").`type`().asInstanceOf[Types.ListType]
    val structType = itemsType.elementType().asInstanceOf[Types.StructType]
    assert(structType.fields().size() == 2)
    assert(structType.field("id").`type`() == Types.IntegerType.get())
    assert(structType.field("name").`type`() == Types.StringType.get())
  }

  test("convert deeply nested structure") {
    val sparkSchema = StructType(Seq(
      StructField("level1", StructType(Seq(
        StructField("level2", StructType(Seq(
          StructField("level3", StringType, nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    val level1 = schema.findField("level1").`type`().asInstanceOf[Types.StructType]
    val level2 = level1.field("level2").`type`().asInstanceOf[Types.StructType]
    assert(level2.field("level3").`type`() == Types.StringType.get())
  }

  test("field IDs are assigned sequentially starting from 1") {
    val sparkSchema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = false),
      StructField("c", BooleanType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.findField("a").fieldId() == 1)
    assert(schema.findField("b").fieldId() == 2)
    assert(schema.findField("c").fieldId() == 3)
  }

  test("nested struct fields have global sequential IDs") {
    val sparkSchema = StructType(Seq(
      StructField("outer1", IntegerType, nullable = false),
      StructField("nested", StructType(Seq(
        StructField("inner1", StringType, nullable = false),
        StructField("inner2", BooleanType, nullable = false)
      )), nullable = false),
      StructField("outer2", LongType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    // Top-level fields get IDs 1, 2, 5 (nested struct uses 2, inner fields use 3-4)
    assert(schema.findField("outer1").fieldId() == 1)
    assert(schema.findField("nested").fieldId() == 2)
    assert(schema.findField("outer2").fieldId() == 5)

    // Nested fields continue the global sequence
    val nestedType = schema.findField("nested").`type`().asInstanceOf[Types.StructType]
    assert(nestedType.field("inner1").fieldId() == 3)
    assert(nestedType.field("inner2").fieldId() == 4)
  }

  test("convert schema with mixed nullability") {
    val sparkSchema = StructType(Seq(
      StructField("required_int", IntegerType, nullable = false),
      StructField("optional_string", StringType, nullable = true),
      StructField("required_bool", BooleanType, nullable = false),
      StructField("optional_long", LongType, nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.findField("required_int").isRequired)
    assert(schema.findField("optional_string").isOptional)
    assert(schema.findField("required_bool").isRequired)
    assert(schema.findField("optional_long").isOptional)
  }

  test("unsupported type - MapType returns None") {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("metadata", MapType(StringType, StringType), nullable = true)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isEmpty, "Schema with MapType should return None")
  }

  test("unsupported type in nested struct returns None") {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("data", StructType(Seq(
        StructField("map_field", MapType(StringType, IntegerType), nullable = true)
      )), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isEmpty, "Schema with nested MapType should return None")
  }

  test("unsupported type in array returns None") {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("items", ArrayType(
        MapType(StringType, IntegerType), containsNull = false
      ), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isEmpty, "Schema with array of MapType should return None")
  }

  test("empty schema") {
    val sparkSchema = StructType(Seq.empty)

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)
    assert(result.get.columns().size() == 0)
  }

  test("single column schema") {
    val sparkSchema = StructType(Seq(
      StructField("value", LongType, nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.columns().size() == 1)
    assert(schema.findField("value").`type`() == Types.LongType.get())
  }

  test("complex realistic schema") {
    val sparkSchema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("username", StringType, nullable = false),
      StructField("email", StringType, nullable = true),
      StructField("created_at", TimestampType, nullable = false),
      StructField("balance", DecimalType(18, 2), nullable = false),
      StructField("is_active", BooleanType, nullable = false),
      StructField("profile", StructType(Seq(
        StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("birth_date", DateType, nullable = true),
        StructField("addresses", ArrayType(StructType(Seq(
          StructField("street", StringType, nullable = false),
          StructField("city", StringType, nullable = false),
          StructField("postal_code", StringType, nullable = true)
        )), containsNull = false), nullable = false)
      )), nullable = true),
      StructField("tags", ArrayType(StringType, containsNull = false), nullable = false)
    ))

    val result = SparkToIcebergSchemaConverter.convert(sparkSchema)
    assert(result.isDefined)

    val schema = result.get
    assert(schema.columns().size() == 8)

    // Verify top-level fields
    assert(schema.findField("user_id").`type`() == Types.LongType.get())
    assert(schema.findField("username").`type`() == Types.StringType.get())
    assert(schema.findField("created_at").`type`() == Types.TimestampType.withZone())

    // Verify decimal
    val balanceType = schema.findField("balance").`type`().asInstanceOf[Types.DecimalType]
    assert(balanceType.precision() == 18)
    assert(balanceType.scale() == 2)

    // Verify nested structure
    val profileType = schema.findField("profile").`type`().asInstanceOf[Types.StructType]
    assert(profileType.fields().size() == 4)

    // Verify nested array of structs
    val addressesType = profileType.field("addresses").`type`().asInstanceOf[Types.ListType]
    val addressStructType = addressesType.elementType().asInstanceOf[Types.StructType]
    assert(addressStructType.fields().size() == 3)
  }
}
