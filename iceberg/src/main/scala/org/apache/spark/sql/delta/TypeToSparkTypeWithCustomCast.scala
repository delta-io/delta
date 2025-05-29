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

package org.apache.spark.sql.delta.commands.convert

import java.util

import scala.collection.JavaConverters._

import org.apache.iceberg.MetadataColumns
import org.apache.iceberg.Schema
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID._
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.TypeUtil

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampNTZType
import org.apache.spark.sql.types.TimestampType

/**
 * This class is copied from [[org.apache.iceberg.spark.TypeToSparkType]] to
 * add custom type casting. Currently, it supports the following casting
 * * Iceberg TIME -> Spark Long
 *
 */
class TypeToSparkTypeWithCustomCast extends TypeUtil.SchemaVisitor[DataType] {

  val METADATA_COL_ATTR_KEY = "__metadata_col";

  override def schema(schema: Schema, structType: DataType): DataType = structType

  override def struct(struct: Types.StructType, fieldResults: util.List[DataType]): DataType = {
    val fields = struct.fields();
    val sparkFields: util.List[StructField] =
      Lists.newArrayListWithExpectedSize(fieldResults.size())
    for (i <- 0 until fields.size()) {
      val field = fields.get(i)
      val `type` = fieldResults.get(i)
      val metadata = fieldMetadata(field.fieldId())
      var sparkField = StructField.apply(field.name(), `type`, field.isOptional(), metadata)
      if (field.doc() != null) {
        sparkField = sparkField.withComment(field.doc())
      }
      sparkFields.add(sparkField)
    }

    StructType.apply(sparkFields)
  }

  override def field(field: Types.NestedField, fieldResult: DataType): DataType = fieldResult

  override def list(list: Types.ListType, elementResult: DataType): DataType =
    ArrayType.apply(elementResult, list.isElementOptional())

  override def map(map: Types.MapType, keyResult: DataType, valueResult: DataType): DataType =
    MapType.apply(keyResult, valueResult, map.isValueOptional())

  override def primitive(primitive: Type.PrimitiveType): DataType = {
    primitive.typeId() match {
      case BOOLEAN => BooleanType
      case INTEGER => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case DATE => DateType
      // This line is changed to allow casting TIME to Spark Long.
      // The result is microseconds since midnight.
      case TIME => LongType
      case TIMESTAMP =>
        val ts = primitive.asInstanceOf[Types.TimestampType]
        if (ts.shouldAdjustToUTC()) {
          TimestampType
        } else {
          TimestampNTZType
        }
      case STRING => StringType
      case UUID => // use String
        StringType
      case FIXED => BinaryType
      case BINARY => BinaryType
      case DECIMAL =>
        val decimal = primitive.asInstanceOf[Types.DecimalType]
        DecimalType.apply(decimal.precision(), decimal.scale());
      case _ =>
        throw new UnsupportedOperationException(
            "Cannot convert unknown type to Spark: " + primitive);
    }
  }

  private def fieldMetadata(fieldId: Int): Metadata = {
    if (MetadataColumns.metadataFieldIds().contains(fieldId)) {
      return new MetadataBuilder().putBoolean(METADATA_COL_ATTR_KEY, value = true).build()
    }

    Metadata.empty
  }
}
