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
package io.delta.kernel.defaults.utils

import scala.collection.JavaConverters._
import scala.collection.mutable.{Seq => MutableSeq}
import org.apache.spark.sql.{types => sparktypes}
import org.apache.spark.sql.{Row => SparkRow}
import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.types._

import java.sql.Timestamp
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

/**
 * Corresponding Scala class for each Kernel data type:
 * - BooleanType --> boolean
 * - ByteType --> byte
 * - ShortType --> short
 * - IntegerType --> int
 * - LongType --> long
 * - FloatType --> float
 * - DoubleType --> double
 * - StringType --> String
 * - DateType --> int (number of days since the epoch)
 * - TimestampType --> long (number of microseconds since the unix epoch)
 * - TimestampNTZType --> long (number of microseconds in local time with no timezone)
 * - DecimalType --> java.math.BigDecimal
 * - BinaryType --> Array[Byte]
 * - ArrayType --> Seq[Any]
 * - MapType --> Map[Any, Any]
 * - StructType --> TestRow
 *
 * For complex types array and map, the inner elements types should align with this mapping.
 */
class TestRow(val values: Array[Any]) {

  def length: Int = values.length

  def get(i: Int): Any = values(i)

  def toSeq: Seq[Any] = values.clone()

  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }

  override def toString: String = this.mkString("[", ",", "]")
}

object TestRow {

  /**
   * Construct a [[TestRow]] with the given values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def apply(values: Any*): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the same values as a Kernel [[Row]].
   */
  def apply(row: Row): TestRow = {
    TestRow.fromSeq(row.getSchema.fields().asScala.zipWithIndex.map { case (field, i) =>
      field.getDataType match {
        case _ if row.isNullAt(i) => null
        case _: BooleanType => row.getBoolean(i)
        case _: ByteType => row.getByte(i)
        case _: IntegerType => row.getInt(i)
        case _: LongType => row.getLong(i)
        case _: ShortType => row.getShort(i)
        case _: DateType => row.getInt(i)
        case _: TimestampType => row.getLong(i)
        case _: TimestampNTZType => row.getLong(i)
        case _: FloatType => row.getFloat(i)
        case _: DoubleType => row.getDouble(i)
        case _: StringType => row.getString(i)
        case _: BinaryType => row.getBinary(i)
        case _: DecimalType => row.getDecimal(i)
        case _: ArrayType => arrayValueToScalaSeq(row.getArray(i))
        case _: MapType => mapValueToScalaMap(row.getMap(i))
        case _: StructType => TestRow(row.getStruct(i))
        case _ => throw new UnsupportedOperationException("unrecognized data type")
      }
    }.toSeq)
  }

  def apply(row: SparkRow): TestRow = {
    def decodeCellValue(dataType: sparktypes.DataType, obj: Any): Any = {
      dataType match {
        case _ if obj == null => null
        case _: sparktypes.BooleanType => obj.asInstanceOf[Boolean]
        case _: sparktypes.ByteType => obj.asInstanceOf[Byte]
        case _: sparktypes.IntegerType => obj.asInstanceOf[Int]
        case _: sparktypes.LongType => obj.asInstanceOf[Long]
        case _: sparktypes.ShortType => obj.asInstanceOf[Short]
        case _: sparktypes.DateType => LocalDate.ofEpochDay(obj.asInstanceOf[Int])
        case _: sparktypes.TimestampType =>
          ChronoUnit.MICROS.between(Instant.EPOCH, obj.asInstanceOf[Timestamp].toInstant)
        case _: sparktypes.TimestampNTZType =>
          ChronoUnit.MICROS.between(Instant.EPOCH, obj.asInstanceOf[LocalDateTime].toInstant(UTC))
        case _: sparktypes.FloatType => obj.asInstanceOf[Float]
        case _: sparktypes.DoubleType => obj.asInstanceOf[Double]
        case _: sparktypes.StringType => obj.asInstanceOf[String]
        case _: sparktypes.BinaryType => obj.asInstanceOf[Array[Byte]]
        case _: sparktypes.DecimalType => obj.asInstanceOf[java.math.BigDecimal]
        case arrayType: sparktypes.ArrayType =>
          obj.asInstanceOf[MutableSeq[Any]]
            .map(decodeCellValue(arrayType.elementType, _))
        case mapType: sparktypes.MapType => obj.asInstanceOf[Map[Any, Any]].map {
          case (k, v) =>
            decodeCellValue(mapType.keyType, k) -> decodeCellValue(mapType.valueType, v)
        }
        case _: sparktypes.StructType => TestRow(obj.asInstanceOf[SparkRow])
        case _ => throw new UnsupportedOperationException("unrecognized data type")
      }
    }

    TestRow.fromSeq(row.schema.fields.zipWithIndex.map { case (field, i) =>
      field.dataType match {
        case _ if row.isNullAt(i) => null
        case _: sparktypes.BooleanType => row.getBoolean(i)
        case _: sparktypes.ByteType => row.getByte(i)
        case _: sparktypes.IntegerType => row.getInt(i)
        case _: sparktypes.LongType => row.getLong(i)
        case _: sparktypes.ShortType => row.getShort(i)
        case _: sparktypes.DateType => row.getDate(i).toLocalDate.toEpochDay.toInt
        case _: sparktypes.TimestampType =>
          ChronoUnit.MICROS.between(Instant.EPOCH, row.getTimestamp(i).toInstant)
        case _: sparktypes.TimestampNTZType =>
          ChronoUnit.MICROS.between(Instant.EPOCH, row.getAs[LocalDateTime](i).toInstant(UTC))
        case _: sparktypes.FloatType => row.getFloat(i)
        case _: sparktypes.DoubleType => row.getDouble(i)
        case _: sparktypes.StringType => row.getString(i)
        case _: sparktypes.BinaryType => row(i) // return as byte[], there is no getBinary method
        case _: sparktypes.DecimalType => row.getDecimal(i)
        case arrayType: sparktypes.ArrayType =>
          val arrayValue = row.getSeq[Any](i)
          arrayValue.indices.map { i =>
            decodeCellValue(arrayType.elementType, arrayValue(i));
          }
        case mapType: sparktypes.MapType =>
          val mapValue = row.getMap[Any, Any](i)
          mapValue.map { case (k, v) =>
            decodeCellValue(mapType.keyType, k) -> decodeCellValue(mapType.valueType, v)
          }
        case _: sparktypes.StructType => TestRow(row.getStruct(i))
        case _ => throw new UnsupportedOperationException("unrecognized data type")
      }
    })
  }

  /**
   * Retrieves the value at `rowId` in the column vector as it's corresponding scala type.
   * See the [[TestRow]] docs for details.
   */
  private def getAsTestObject(vector: ColumnVector, rowId: Int): Any = {
    vector.getDataType match {
      case _ if vector.isNullAt(rowId) => null
      case _: BooleanType => vector.getBoolean(rowId)
      case _: ByteType => vector.getByte(rowId)
      case _: IntegerType => vector.getInt(rowId)
      case _: LongType => vector.getLong(rowId)
      case _: ShortType => vector.getShort(rowId)
      case _: DateType => vector.getInt(rowId)
      case _: TimestampType => vector.getLong(rowId)
      case _: TimestampNTZType => vector.getLong(rowId)
      case _: FloatType => vector.getFloat(rowId)
      case _: DoubleType => vector.getDouble(rowId)
      case _: StringType => vector.getString(rowId)
      case _: BinaryType => vector.getBinary(rowId)
      case _: DecimalType => vector.getDecimal(rowId)
      case _: ArrayType => arrayValueToScalaSeq(vector.getArray(rowId))
      case _: MapType => mapValueToScalaMap(vector.getMap(rowId))
      case dataType: StructType =>
        TestRow.fromSeq(Seq.range(0, dataType.length()).map { ordinal =>
          getAsTestObject(vector.getChild(ordinal), rowId)
        })
      case _ => throw new UnsupportedOperationException("unrecognized data type")
    }
  }

  private def arrayValueToScalaSeq(arrayValue: ArrayValue): Seq[Any] = {
    val elemVector = arrayValue.getElements
    (0 until arrayValue.getSize).map { i =>
      getAsTestObject(elemVector, i)
    }
  }

  private def mapValueToScalaMap(mapValue: MapValue): Map[Any, Any] = {
    val keyVector = mapValue.getKeys()
    val valueVector = mapValue.getValues()
    (0 until mapValue.getSize).map { i =>
      getAsTestObject(keyVector, i) -> getAsTestObject(valueVector, i)
    }.toMap
  }

  /**
   * Construct a [[TestRow]] from the given seq of values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def fromSeq(values: Seq[Any]): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the elements of the given tuple. See the docs for
   * [[TestRow]] for the scala type corresponding to each Kernel data type.
   */
  def fromTuple(tuple: Product): TestRow = fromSeq(tuple.productIterator.toSeq)
}
