package io.delta.dsv2.read

import io.delta.kernel.data.Row

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Wrapper for a Delta Kernel Row to be used as a Spark InternalRow.
 */
class KernelRowToSparkRowWrapper(row: Row) extends InternalRow {

  override def anyNull: Boolean = (0 until numFields).exists(isNullAt)

  override def copy(): InternalRow = {
    new KernelRowToSparkRowWrapper(row)
  }

  override def numFields: Int = row.getSchema.length()

  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def getInt(ordinal: Int): Int = row.getInt(ordinal)

  override def getLong(ordinal: Int): Long = row.getLong(ordinal)

  override def getString(ordinal: Int): String = row.getString(ordinal)

  override def getUTF8String(ordinal: Int): UTF8String = {
    UTF8String.fromString(row.getString(ordinal))
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    dataType match {
      case org.apache.spark.sql.types.BooleanType => Boolean.box(getBoolean(ordinal))
      case org.apache.spark.sql.types.IntegerType => Int.box(getInt(ordinal))
      case org.apache.spark.sql.types.LongType => Long.box(getLong(ordinal))
      case org.apache.spark.sql.types.StringType => getUTF8String(ordinal)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }

  /////////////////////////
  // Unsupported methods //
  /////////////////////////

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException(
      "getInterval is not supported for KernelRowToSparkRowWrapper")

  override def getByte(ordinal: Int): Byte =
    throw new UnsupportedOperationException(
      "getByte is not supported for KernelRowToSparkRowWrapper")

  override def getShort(ordinal: Int): Short =
    throw new UnsupportedOperationException(
      "getShort is not supported for KernelRowToSparkRowWrapper")

  override def getFloat(ordinal: Int): Float =
    throw new UnsupportedOperationException(
      "getFloat is not supported for KernelRowToSparkRowWrapper")

  override def getDouble(ordinal: Int): Double =
    throw new UnsupportedOperationException(
      "getDouble is not supported for KernelRowToSparkRowWrapper")

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException(
      "getDecimal is not supported for KernelRowToSparkRowWrapper")

  override def getBinary(ordinal: Int): Array[Byte] =
    throw new UnsupportedOperationException(
      "getBinary is not supported for KernelRowToSparkRowWrapper")

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException(
      "getStruct is not supported for KernelRowToSparkRowWrapper")

  override def getArray(ordinal: Int): ArrayData =
    throw new UnsupportedOperationException(
      "getArray is not supported for KernelRowToSparkRowWrapper")

  override def getMap(ordinal: Int): MapData =
    throw new UnsupportedOperationException(
      "getMap is not supported for KernelRowToSparkRowWrapper")

  override def setNullAt(i: Int): Unit =
    throw new UnsupportedOperationException("setNullAt is not supported in read-only rows")

  override def update(i: Int, value: Any): Unit =
    throw new UnsupportedOperationException("update is not supported in read-only rows")
}
