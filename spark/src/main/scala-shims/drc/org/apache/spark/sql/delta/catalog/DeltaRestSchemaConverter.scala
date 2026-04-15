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

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  ArrayType => UCArrayType,
  DecimalType => UCDecimalType,
  DeltaType,
  MapType => UCMapType,
  PrimitiveType => UCPrimitiveType,
  StructField => UCStructField,
  StructType => UCStructType
}

import org.apache.spark.sql.types._

/**
 * Direct converter from UC Delta REST Catalog StructType to Spark StructType.
 * No JSON roundtrip -- maps the UC DeltaType hierarchy to Spark DataType
 * field-by-field.
 */
object DeltaRestSchemaConverter {

  def toSparkStructType(ucSchema: UCStructType): StructType = {
    if (ucSchema == null || ucSchema.getFields == null) return new StructType()
    StructType(ucSchema.getFields.asScala.map(toSparkStructField).toArray)
  }

  private def toSparkStructField(field: UCStructField): StructField = {
    StructField(
      field.getName,
      toSparkDataType(field.getType),
      field.getNullable)
  }

  private def toSparkDataType(deltaType: DeltaType): DataType = deltaType match {
    case dt: UCDecimalType =>
      DecimalType(dt.getPrecision, dt.getScale)

    case at: UCArrayType =>
      ArrayType(toSparkDataType(at.getElementType), at.getContainsNull)

    case mt: UCMapType =>
      MapType(
        toSparkDataType(mt.getKeyType),
        toSparkDataType(mt.getValueType),
        mt.getValueContainsNull)

    case st: UCStructType =>
      toSparkStructType(st)

    case _ =>
      // Primitive types and fallback: use the type string
      val typeName = deltaType.getType
      typeName match {
        case "string" => StringType
        case "integer" | "int" => IntegerType
        case "long" => LongType
        case "short" => ShortType
        case "byte" => ByteType
        case "float" => FloatType
        case "double" => DoubleType
        case "boolean" => BooleanType
        case "binary" => BinaryType
        case "date" => DateType
        case "timestamp" => TimestampType
        case "timestamp_ntz" => TimestampNTZType
        case other if other.startsWith("decimal") =>
          // Parse decimal(p,s) from type string
          DataType.fromDDL(other)
        case other => DataType.fromDDL(other)
      }
  }
}
