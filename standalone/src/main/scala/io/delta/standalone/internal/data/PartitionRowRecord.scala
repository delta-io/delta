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

import java.sql.{Date, Timestamp}

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types.{BooleanType, IntegerType, StringType, StructType}

/**
 * A RowRecord representing a Delta Lake partition of Map(partitionKey -> partitionValue)
 */
private[internal] class PartitionRowRecord(
    partitionSchema: StructType,
    partitionValues: Map[String, String]) extends RowRecordJ {

  private val partitionFieldToType =
   partitionSchema.getFields.map { f => f.getName -> f.getDataType }.toMap

  require(partitionFieldToType.keySet == partitionValues.keySet,
    s"""
      |Column mismatch between partitionSchema and partitionValues.
      |partitionSchema: ${partitionFieldToType.keySet.mkString(", ")}
      |partitionValues: ${partitionValues.keySet.mkString(", ")}
      |""".stripMargin)

  private def requireFieldExists(fieldName: String): Unit = {
    // this is equivalent to checking both partitionValues and partitionFieldToType maps
    // due to `require` statement above
    require(partitionValues.contains(fieldName))
  }

  override def getSchema: StructType = partitionSchema

  override def getLength: Int = partitionSchema.getFieldNames.length

  override def isNullAt(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    null == partitionValues(fieldName)
  }

  override def getInt(fieldName: String): Int = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[IntegerType])
    partitionValues(fieldName).toInt
  }

  override def getLong(fieldName: String): Long = 0 // TODO

  override def getByte(fieldName: String): Byte = 0 // TODO

  override def getShort(fieldName: String): Short = 0 // TODO

  override def getBoolean(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[BooleanType])
    partitionValues(fieldName).toBoolean
  }

  override def getFloat(fieldName: String): Float = 0 // TODO

  override def getDouble(fieldName: String): Double = 0 // TODO

  override def getString(fieldName: String): String = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[StringType])
    partitionValues(fieldName)
  }

  override def getBinary(fieldName: String): Array[Byte] = null // TODO

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = null // TODO

  override def getTimestamp(fieldName: String): Timestamp = null // TODO

  override def getDate(fieldName: String): Date = null // TODO

  override def getRecord(fieldName: String): RowRecordJ = null // TODO

  override def getList[T](fieldName: String): java.util.List[T] = null // TODO

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] = null // TODO
}
