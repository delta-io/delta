package io.delta.standalone.internal.data

import java.sql.{Date, Timestamp}

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types.{BooleanType, IntegerType, StructType}

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

  override def getLong(fieldName: String): Long = ???

  override def getByte(fieldName: String): Byte = ???

  override def getShort(fieldName: String): Short = ???

  override def getBoolean(fieldName: String): Boolean = {
    requireFieldExists(fieldName)
    require(partitionFieldToType(fieldName).isInstanceOf[BooleanType])
    partitionValues(fieldName).toBoolean
  }

  override def getFloat(fieldName: String): Float = ???

  override def getDouble(fieldName: String): Double = ???

  override def getString(fieldName: String): String = ???

  override def getBinary(fieldName: String): Array[Byte] = ???

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = ???

  override def getTimestamp(fieldName: String): Timestamp = ???

  override def getDate(fieldName: String): Date = ???

  override def getRecord(fieldName: String): RowRecordJ = ???

  override def getList[T](fieldName: String): java.util.List[T] = ???

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] = ???
}
