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

package org.apache.spark.sql.interface.system.semistructured.hbase.dialect

import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.interface.system.structured.relational.dialect.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

import java.sql.ResultSet
import java.util.Locale

/**
 * specially, this Dialect supports data type of one-dimensional ARRAY
 */
case object HBaseDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:phoenix")

  override def getCatalystType(
                                sqlType: Int,
                                typeName: String,
                                size: Int,
                                md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case java.sql.Types.ARRAY =>
        typeName match {
          case "INTEGER ARRAY" => Some(ArrayType(IntegerType))
          case "VARCHAR ARRAY" => Some(ArrayType(StringType))
          case "BIGINT ARRAY" => Some(ArrayType(LongType))
          case "TINYINT ARRAY" => Some(ArrayType(ByteType))
          case "FLOAT ARRAY" => Some(ArrayType(FloatType))
          case "SMALLINT ARRAY" => Some(ArrayType(ShortType))
          case "DOUBLE ARRAY" => Some(ArrayType(DoubleType))
          case "DECIMAL ARRAY" => Some(ArrayType(DecimalType(10, 2)))
          case "CHAR ARRAY" => Some(ArrayType(StringType))
          case "BOOLEAN ARRAY" => Some(ArrayType(BooleanType))
        }
      case java.sql.Types.DATE => Some(TimestampType)
      case java.sql.Types.TINYINT => Some(ByteType)
      case _ => None
    }

  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case DoubleType => Option(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
    case FloatType => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case ByteType => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case StringType => Option(JdbcType("CHAR", java.sql.Types.CHAR))
    case BinaryType => Option(JdbcType("BINARY", java.sql.Types.BINARY))
    case ArrayType(StringType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(IntegerType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(LongType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(ByteType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(FloatType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(ShortType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(DoubleType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(DecimalType(), true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case ArrayType(BooleanType, true) => Option(JdbcType("ARRAY", java.sql.Types.ARRAY))
    case _ => None
  }


  override def resultSetToSparkInternalRows(
                                             resultSet: ResultSet,
                                             schema: StructType,
                                             inputMetrics: InputMetrics): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
      private[this] val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        if (rs.next()) {
          inputMetrics.incRecordsRead(1)
          var i = 0
          while (i < getters.length) {
            getters(i).apply(rs, mutableRow, i)
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  private type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  private def makeGetters(schema: StructType): Array[JDBCValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

    case DecimalType.Fixed(p, s) =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](
                decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case IntegerType =>
          (array: Object) =>
            array.asInstanceOf[Array[Int]]

        case LongType =>
          (array: Object) =>
            array.asInstanceOf[Array[Long]]

        case ByteType =>
          (array: Object) =>
            array.asInstanceOf[Array[Byte]]

        case FloatType =>
          (array: Object) =>
            array.asInstanceOf[Array[Float]]

        case ShortType =>
          (array: Object) =>
            array.asInstanceOf[Array[Short]]

        case DoubleType =>
          (array: Object) =>
            array.asInstanceOf[Array[Double]]

        case DecimalType() =>
          (array: Object) =>
            array.asInstanceOf[Array[Decimal]]

        case BooleanType =>
          (array: Object) =>
            array.asInstanceOf[Array[Boolean]]

        case ArrayType(_, _) =>
          throw new IllegalArgumentException(s" Nested arrays are not supported ")
        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray)))
        row.update(pos, array)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported types ${dt.catalogString}")
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }


}
