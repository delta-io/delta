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

package org.apache.spark.sql.delta

import scala.util.Try

import org.apache.spark.sql.delta.DeltaColumnMapping._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.{ListLogicalTypeAnnotation, MapLogicalTypeAnnotation}

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.parquet.{ParquetSchemaConverter, ParquetWriteSupport}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class DeltaParquetWriteSupport extends ParquetWriteSupport {

  private def getNestedFieldId(field: StructField, path: Seq[String]): Int = {
    field.metadata
      .getMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
      .getLong(path.mkString("."))
      .toInt
  }

  private def findFieldInSparkSchema(schema: StructType, path: Seq[String]): StructField = {
    schema.findNestedField(path, true) match {
      case Some((_, field)) => field
      case None => throw QueryCompilationErrors.invalidFieldName(Seq(path.head), path, Origin())
    }
  }

  override def init(configuration: Configuration): WriteContext = {
    val writeContext = super.init(configuration)
    // Parse the Spark schema. This is the same as is done in super.init, however, the
    // parsed schema is stored in [[ParquetWriteSupport.schema]], which is private so
    // we can't access it here and need to parse it again.
    val schemaString = configuration.get(ParquetWriteSupport.SPARK_ROW_SCHEMA)
    // This code is copied from Spark StructType.fromString because it is not accessible here
    val parsedSchema = Try(DataType.fromJson(schemaString)).getOrElse(
      LegacyTypeStringParser.parseString(schemaString)) match {
        case t: StructType => t
        case _ =>
          // This code is copied from DataTypeErrors.failedParsingStructTypeError because
          // it is not accessible here
          throw new SparkRuntimeException(
            errorClass = "FAILED_PARSE_STRUCT_TYPE",
            messageParameters = Map("raw" -> s"'$schemaString'"))
    }

    val messageType = writeContext.getSchema
    val newMessageTypeBuilder = Types.buildMessage()
    messageType.getFields.forEach { field =>
      val parentField = findFieldInSparkSchema(parsedSchema, Seq(field.getName))
      newMessageTypeBuilder.addField(convert(
        field, parentField, parsedSchema, Seq(field.getName), Seq(field.getName)))
    }
    val newMessageType = newMessageTypeBuilder.named(
      ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
    new WriteContext(newMessageType, writeContext.getExtraMetaData)
  }

  /**
   * Recursively rewrites the parquet [[Type]] by adding the nested field
   * IDs to list and map subtypes as defined in the schema. The
   * recursion needs to keep track of the absolute field path in order
   * to correctly identify the StructField in the spark schema for a
   * corresponding parquet field. As nested field IDs are referenced
   * by their relative path in a field's metadata, the recursion also needs
   * to keep track of the relative path.
   *
   * For example, consider the following column type
   * col1 STRUCT(a INT, b STRUCT(c INT, d ARRAY(INT)))
   *
   * The absolute path to the nested [[element]] field of the list is
   * col1.b.d.element whereas the relative path is d.element, i.e. relative
   * to the parent struct field.
   */
  private def convert(
      field: Type,
      parentField: StructField,
      sparkSchema: StructType,
      absolutePath: Seq[String],
      relativePath: Seq[String]): Type = {
    field.getLogicalTypeAnnotation match {
      case _: ListLogicalTypeAnnotation =>
        val relElemFieldPath = relativePath :+ PARQUET_LIST_ELEMENT_FIELD_NAME
        val id = getNestedFieldId(parentField, relElemFieldPath)
        val elementField =
          field.asGroupType().getFields.get(0).asGroupType().getFields.get(0).withId(id)
        val builder = Types
          .buildGroup(field.getRepetition).as(LogicalTypeAnnotation.listType())
          .addField(
            Types.repeatedGroup()
              .addField(convert(elementField, parentField, sparkSchema,
                absolutePath :+ PARQUET_LIST_ELEMENT_FIELD_NAME, relElemFieldPath))
              .named("list"))
          if (field.getId != null) {
            builder.id(field.getId.intValue())
          }
          builder.named(field.getName)
      case _: MapLogicalTypeAnnotation =>
        val relKeyFieldPath = relativePath :+ PARQUET_MAP_KEY_FIELD_NAME
        val relValFieldPath = relativePath :+ PARQUET_MAP_VALUE_FIELD_NAME
        val keyId = getNestedFieldId(parentField, relKeyFieldPath)
        val valId = getNestedFieldId(parentField, relValFieldPath)
        val keyField =
          field.asGroupType().getFields.get(0).asGroupType().getFields.get(0).withId(keyId)
        val valueField =
          field.asGroupType().getFields.get(0).asGroupType().getFields.get(1).withId(valId)
        val builder = Types
          .buildGroup(field.getRepetition).as(LogicalTypeAnnotation.mapType())
          .addField(
            Types
              .repeatedGroup()
              .addField(convert(keyField, parentField, sparkSchema,
                absolutePath :+ PARQUET_MAP_KEY_FIELD_NAME, relKeyFieldPath))
              .addField(convert(valueField, parentField, sparkSchema,
                absolutePath :+ PARQUET_MAP_VALUE_FIELD_NAME, relValFieldPath))
              .named("key_value"))
        if (field.getId != null) {
          builder.id(field.getId.intValue())
        }
        builder.named(field.getName)
      case _ if field.isPrimitive => field
      case _ =>
        val builder = Types.buildGroup(field.getRepetition)
        field.asGroupType().getFields.forEach { field =>
          val absPath = absolutePath :+ field.getName
          val parentField = findFieldInSparkSchema(sparkSchema, absPath)
          builder.addField(convert(field, parentField, sparkSchema, absPath, Seq(field.getName)))
        }
        if (field.getId != null) {
          builder.id(field.getId.intValue())
        }
        builder.named(field.getName)
    }
  }
}
