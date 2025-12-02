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

import scala.jdk.CollectionConverters._

import shadedForDelta.org.apache.iceberg.Schema
import shadedForDelta.org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.types._

/**
 * ICEBERG-SPECIFIC IMPLEMENTATION DETAIL - NOT SHARED INFRASTRUCTURE
 *
 * Converts Spark StructType schemas to Iceberg Schema objects for Iceberg REST catalog
 * integration. This converter is specific to Iceberg and should NOT be used as a generic
 * schema conversion utility for other catalog implementations.
 *
 * Catalog-Agnostic Design Pattern:
 * The `ServerSidePlanningClient` interface uses Spark's standard `StructType` as the
 * universal representation for projection pushdown. Each catalog implementation provides
 * its own converter to translate Spark schemas to their native format:
 *  - Iceberg catalogs: Use this converter (Spark StructType to Iceberg Schema)
 *  - Unity Catalog: Should implement UC-specific converter (Spark StructType to UC schema)
 *  - Other catalogs: Implement their own conversion logic as needed
 *
 * Field ID Assignment:
 * Iceberg schemas require unique field IDs for each field. This converter assigns sequential
 * IDs starting from 1. This is sufficient for projection pushdown where we're describing
 * which columns to read (the server already has the full schema with proper field IDs).
 *
 * Supported Types:
 * - Primitives: Boolean, Integer, Long, Float, Double, String, Binary, Byte, Short
 * - Date/Time: Date, Timestamp (both with and without timezone)
 * - Decimal: DecimalType with precision and scale
 * - Nested: StructType (nested structures)
 * - Collections: ArrayType (lists)
 *
 * Unsupported Types:
 * - MapType: Not yet implemented
 * - Complex nested collections: Arrays of maps, maps of arrays, etc.
 * - User-defined types (UDTs)
 *
 * Returns None for schemas containing unsupported types to enable graceful degradation.
 */
private[serverSidePlanning] object SparkToIcebergSchemaConverter {

  /**
   * Convert a Spark StructType to an Iceberg Schema.
   * Returns None if the schema contains unsupported types.
   *
   * @param sparkSchema The Spark StructType to convert
   * @return Some(Schema) if conversion succeeds, None if unsupported types encountered
   */
  def convert(sparkSchema: StructType): Option[Schema] = {
    try {
      val fieldIdCounter = new FieldIdCounter()
      val fields = sparkSchema.fields.map { field =>
        convertField(field, fieldIdCounter)
      }
      Some(new Schema(fields.toList.asJava))
    } catch {
      case _: UnsupportedOperationException => None
    }
  }

  /**
   * Mutable counter for generating unique field IDs across entire schema.
   */
  private class FieldIdCounter {
    private var nextId = 1
    def getNext(): Int = {
      val id = nextId
      nextId += 1
      id
    }
  }

  /**
   * Convert a Spark StructField to an Iceberg nested field.
   */
  private def convertField(field: StructField, counter: FieldIdCounter): Types.NestedField = {
    val fieldId = counter.getNext()
    val icebergType = convertType(field.dataType, counter)
    if (field.nullable) {
      Types.NestedField.optional(fieldId, field.name, icebergType)
    } else {
      Types.NestedField.required(fieldId, field.name, icebergType)
    }
  }

  /**
   * Convert a Spark DataType to an Iceberg Type.
   * Throws UnsupportedOperationException for unsupported types.
   */
  private def convertType(sparkType: DataType, counter: FieldIdCounter): Type = sparkType match {
    // Primitive types
    case BooleanType => Types.BooleanType.get()
    case IntegerType => Types.IntegerType.get()
    case LongType => Types.LongType.get()
    case FloatType => Types.FloatType.get()
    case DoubleType => Types.DoubleType.get()
    case StringType => Types.StringType.get()
    case BinaryType => Types.BinaryType.get()
    case ByteType => Types.IntegerType.get()  // Iceberg doesn't have byte, use int
    case ShortType => Types.IntegerType.get()  // Iceberg doesn't have short, use int

    // Date/Time types
    case DateType => Types.DateType.get()
    case TimestampType => Types.TimestampType.withZone()
    case TimestampNTZType => Types.TimestampType.withoutZone()

    // Decimal type
    case DecimalType.Fixed(precision, scale) =>
      Types.DecimalType.of(precision, scale)

    // Struct type (nested structure)
    case struct: StructType =>
      val fields = struct.fields.map { field =>
        convertField(field, counter)
      }
      Types.StructType.of(fields.toList.asJava)

    // Array type
    case ArrayType(elementType, containsNull) =>
      val elementId = counter.getNext()
      val icebergElementType = convertType(elementType, counter)
      if (containsNull) {
        Types.ListType.ofOptional(elementId, icebergElementType)
      } else {
        Types.ListType.ofRequired(elementId, icebergElementType)
      }

    // Unsupported types
    case _: MapType =>
      throw new UnsupportedOperationException(
        s"MapType is not yet supported for projection pushdown")

    case other =>
      throw new UnsupportedOperationException(
        s"Type ${other.typeName} is not supported for projection pushdown")
  }
}
