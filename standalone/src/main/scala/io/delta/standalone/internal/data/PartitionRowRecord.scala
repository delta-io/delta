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

package io.delta.standalone.internal.data

import java.math.{BigDecimal => BigDecimalJ}
import java.sql.{Date, Timestamp}

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types._

import io.delta.standalone.internal.exception.DeltaErrors

/**
 * A RowRecord representing a Delta Lake partition of Map(partitionKey -> partitionValue)
 */
private[internal] class PartitionRowRecord(
    partitionSchema: StructType,
    partitionValues: Map[String, String]) extends RowRecordJ {

  require(partitionSchema.getFieldNames.toSet == partitionValues.keySet,
    s"""
      |Column mismatch between partitionSchema and partitionValues.
      |partitionSchema: ${partitionSchema.getFieldNames.mkString(", ")}
      |partitionValues: ${partitionValues.keySet.mkString(", ")}
      |""".stripMargin)

  private def getPrimitive(field: StructField): String = {
    val partitionValue = partitionValues(field.getName)
    if (partitionValue == null) throw DeltaErrors.nullValueFoundForPrimitiveTypes(field.getName)
    partitionValue
  }

  private def getNonPrimitive(field: StructField): Option[String] = {
    val partitionValue = partitionValues(field.getName)
    if (partitionValue == null) {
      if (!field.isNullable) {
        throw DeltaErrors.nullValueFoundForNonNullSchemaField(field.getName, partitionSchema)
      }
      None
    } else Some(partitionValue)
  }

  override def getSchema: StructType = partitionSchema

  override def getLength: Int = partitionSchema.getFieldNames.length

  override def isNullAt(fieldName: String): Boolean = {
    partitionSchema.get(fieldName) // check that the field exists
    partitionValues(fieldName) == null
  }

  override def getInt(fieldName: String): Int = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[IntegerType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "integer")
    }
    getPrimitive(field).toInt
  }

  override def getLong(fieldName: String): Long = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[LongType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "long")
    }
    getPrimitive(field).toLong
  }

  override def getByte(fieldName: String): Byte = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[ByteType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "byte")
    }
    getPrimitive(field).toByte
  }

  override def getShort(fieldName: String): Short = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[ShortType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "short")
    }
    getPrimitive(field).toShort
  }

  override def getBoolean(fieldName: String): Boolean = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[BooleanType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "boolean")
    }
    getPrimitive(field).toBoolean
  }

  override def getFloat(fieldName: String): Float = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[FloatType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "float")
    }
    getPrimitive(field).toFloat
  }

  override def getDouble(fieldName: String): Double = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[DoubleType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "double")
    }
    getPrimitive(field).toDouble
  }

  override def getString(fieldName: String): String = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[StringType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "string")
    }
    getNonPrimitive(field).orNull
  }

  override def getBinary(fieldName: String): Array[Byte] = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[BinaryType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "binary")
    }
    getNonPrimitive(field).map(_.map(_.toByte).toArray).orNull
  }

  override def getBigDecimal(fieldName: String): BigDecimalJ = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[DecimalType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "decimal")
    }
    getNonPrimitive(field).map(new BigDecimalJ(_)).orNull
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[TimestampType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "timestamp")
    }
    getNonPrimitive(field).map(Timestamp.valueOf).orNull
  }

  override def getDate(fieldName: String): Date = {
    val field = partitionSchema.get(fieldName)
    if (!field.getDataType.isInstanceOf[DateType]) {
      throw DeltaErrors.fieldTypeMismatch(fieldName, field.getDataType, "date")
    }
    getNonPrimitive(field).map(Date.valueOf).orNull
  }

  override def getRecord(fieldName: String): RowRecordJ = {
    throw new UnsupportedOperationException(
      "Struct is not a supported partition type.")
  }

  override def getList[T](fieldName: String): java.util.List[T] = {
    throw new UnsupportedOperationException(
      "Array is not a supported partition type.")
  }

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] = {
    throw new UnsupportedOperationException(
      "Map is not a supported partition type.")
  }
}
