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

package org.apache.spark.sql.delta.icebergShaded

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.metering.DeltaLogging
import shadedForDelta.org.apache.iceberg.{Schema => IcebergSchema}
import shadedForDelta.org.apache.iceberg.types.{Type => IcebergType, Types => IcebergTypes}

import org.apache.spark.sql.types._

object IcebergSchemaUtils extends DeltaLogging {

  /////////////////
  // Public APIs //
  /////////////////

  // scalastyle:off line.size.limit
  /**
   * Delta types are defined here: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format
   *
   * Iceberg types are defined here: https://iceberg.apache.org/spec/#schemas-and-data-types
   */
  // scalastyle:on line.size.limit
  def convertDeltaSchemaToIcebergSchema(deltaSchema: StructType): IcebergSchema = {
    val icebergStruct = convertStruct(deltaSchema)
    new IcebergSchema(icebergStruct.fields())
  }

  private[delta] def getNestedFieldId(field: Option[StructField], path: Seq[String]): Int = {
    field.get.metadata
      .getMetadata(DeltaColumnMapping.COLUMN_MAPPING_METADATA_NESTED_IDS_KEY)
      .getLong(path.mkString("."))
      .toInt
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /** Visible for testing */
  private[delta] def convertStruct(deltaSchema: StructType): IcebergTypes.StructType = {
    /**
     * Recursively (i.e. for all nested elements) transforms the delta DataType `elem` into its
     * corresponding Iceberg type.
     *
     * - StructType -> IcebergTypes.StructType
     * - ArrayType -> IcebergTypes.ListType
     * - MapType -> IcebergTypes.MapType
     * - primitive -> IcebergType.PrimitiveType
     */
    def transform[E <: DataType](elem: E, field: Option[StructField], name: Seq[String])
        : IcebergType = elem match {
      case StructType(fields) =>
        IcebergTypes.StructType.of(fields.map { f =>
          if (!DeltaColumnMapping.hasColumnId(f)) {
            throw new UnsupportedOperationException("UniForm requires Column Mapping")
          }

          IcebergTypes.NestedField.of(
            DeltaColumnMapping.getColumnId(f),
            f.nullable,
            f.name,
            transform(f.dataType, Some(f), Seq(DeltaColumnMapping.getPhysicalName(f))),
            f.getComment().orNull
          )
        }.toList.asJava)

      case ArrayType(elementType, containsNull) =>
        val currName = name :+ DeltaColumnMapping.PARQUET_LIST_ELEMENT_FIELD_NAME
        val id = getNestedFieldId(field, currName)
        if (containsNull) {
          IcebergTypes.ListType.ofOptional(id, transform(elementType, field, currName))
        } else {
          IcebergTypes.ListType.ofRequired(id, transform(elementType, field, currName))
        }

      case MapType(keyType, valueType, valueContainsNull) =>
        val currKeyName = name :+ DeltaColumnMapping.PARQUET_MAP_KEY_FIELD_NAME
        val currValName = name :+ DeltaColumnMapping.PARQUET_MAP_VALUE_FIELD_NAME
        val keyId = getNestedFieldId(field, currKeyName)
        val valId = getNestedFieldId(field, currValName)
        if (valueContainsNull) {
          IcebergTypes.MapType.ofOptional(
            keyId,
            valId,
            transform(keyType, field, currKeyName),
            transform(valueType, field, currValName)
          )
        } else {
          IcebergTypes.MapType.ofRequired(
            keyId,
            valId,
            transform(keyType, field, currKeyName),
            transform(valueType, field, currValName)
          )
        }

      case atomicType: AtomicType => convertAtomic(atomicType)

      case other =>
        throw new UnsupportedOperationException(s"Cannot convert Delta type $other to Iceberg")
    }

    transform(deltaSchema, None, Seq.empty).asStructType()
  }

  /**
   * Converts delta atomic into an iceberg primitive.
   *
   * Visible for testing.
   *
   * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types
   */
  private[delta] def convertAtomic[E <: DataType](elem: E): IcebergType.PrimitiveType = elem match {
    case StringType => IcebergTypes.StringType.get()
    case LongType => IcebergTypes.LongType.get()
    case IntegerType | ShortType | ByteType => IcebergTypes.IntegerType.get()
    case FloatType => IcebergTypes.FloatType.get()
    case DoubleType => IcebergTypes.DoubleType.get()
    case d: DecimalType => IcebergTypes.DecimalType.of(d.precision, d.scale)
    case BooleanType => IcebergTypes.BooleanType.get()
    case BinaryType => IcebergTypes.BinaryType.get()
    case DateType => IcebergTypes.DateType.get()
    case TimestampType => IcebergTypes.TimestampType.withZone()
    case TimestampNTZType => IcebergTypes.TimestampType.withoutZone()
    case _ => throw new UnsupportedOperationException(s"Could not convert atomic type $elem")
  }
}
