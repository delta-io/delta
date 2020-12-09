// scalastyle:off
/*
 * Copyright (c) 2018 Marcin Jakubowski
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * This file contains code from the parquet4s project (original license above).
 * It contains modifications, which are licensed as follows:
 */
// scalastyle:on

/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import java.util
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.compat.Factory
import scala.reflect.ClassTag

import com.github.mjakubowski84.parquet4s._

import io.delta.standalone.data.{RowRecord => RowParquetRecordJ}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.types._

/**
 * Scala implementation of Java interface [[RowParquetRecordJ]].
 *
 * @param record the internal parquet4s record
 * @param schema the intended schema for this record
 * @param timeZone the timeZone as which time-based data will be read
 */
private[internal] case class RowParquetRecordImpl(
    private val record: RowParquetRecord,
    private val schema: StructType,
    private val timeZone: TimeZone) extends RowParquetRecordJ {

  /**
   * Needed to decode values. Constructed with the `timeZone` to properly decode time-based data.
   */
  private val codecConf = ValueCodecConfiguration(timeZone)

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def getSchema: StructType = schema

  override def getLength: Int = record.length

  override def isNullAt(fieldName: String): Boolean = record.get(fieldName) == NullValue

  override def getInt(fieldName: String): Int = getAs[Int](fieldName)

  override def getLong(fieldName: String): Long = getAs[Long](fieldName)

  override def getByte(fieldName: String): Byte = getAs[Byte](fieldName)

  override def getShort(fieldName: String): Short = getAs[Short](fieldName)

  override def getBoolean(fieldName: String): Boolean = getAs[Boolean](fieldName)

  override def getFloat(fieldName: String): Float = getAs[Float](fieldName)

  override def getDouble(fieldName: String): Double = getAs[Double](fieldName)

  override def getString(fieldName: String): String = getAs[String](fieldName)

  override def getBinary(fieldName: String): Array[Byte] = getAs[Array[Byte]](fieldName)

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    getAs[java.math.BigDecimal](fieldName)

  override def getTimestamp(fieldName: String): Timestamp = getAs[Timestamp](fieldName)

  override def getDate(fieldName: String): Date = getAs[Date](fieldName)

  override def getRecord(fieldName: String): RowParquetRecordJ = getAs[RowParquetRecordJ](fieldName)

  override def getList[T](fieldName: String): util.List[T] = getAs[util.List[T]](fieldName)

  override def getMap[K, V](fieldName: String): util.Map[K, V] = getAs[util.Map[K, V]](fieldName)

  ///////////////////////////////////////////////////////////////////////////
  // Decoding Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Decodes the parquet data into the desired type [[T]]
   *
   * @param fieldName the field name to lookup
   * @return the data at column with name `fieldName` as type [[T]]
   * @throws IllegalArgumentException if `fieldName` not in this schema
   * @throws NullPointerException if field, of type [[StructField]], is not `nullable` and null data
   *                              value read
   * @throws RuntimeException if unable to decode the type [[T]]
   */
  private def getAs[T](fieldName: String): T = {
    val schemaField = schema.get(fieldName)
    val parquetVal = record.get(fieldName)

    if (parquetVal == NullValue && !schemaField.isNullable) {
      throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, schema)
    }

    if (primitiveDecodeMap.contains(schemaField.getDataType.getTypeName)
      && parquetVal == NullValue) {
      throw DeltaErrors.nullValueFoundForPrimitiveTypes(fieldName)
    }

    decode(schemaField.getDataType, parquetVal).asInstanceOf[T]
  }

  /**
   * Decode the parquet `parqetVal` into the corresponding Scala type for `elemType`
   */
  private def decode(elemType: DataType, parquetVal: Value): Any = {
    val elemTypeName = elemType.getTypeName
    if (primitiveDecodeMap.contains(elemTypeName)) {
      return primitiveDecodeMap(elemTypeName).decode(parquetVal, codecConf)
    }

    if (primitiveNullableDecodeMap.contains(elemTypeName)) {
      return primitiveNullableDecodeMap(elemTypeName).decode(parquetVal, codecConf)
    }

    (elemType, parquetVal) match {
      case (x: ArrayType, y: ListParquetRecord) => decodeList(x.getElementType, y)
      case (x: MapType, y: MapParquetRecord) => decodeMap(x.getKeyType, x.getValueType, y)
      case (x: StructType, y: RowParquetRecord) => RowParquetRecordImpl(y, x, timeZone)
      case _ =>
        throw new RuntimeException(s"Unknown non-primitive decode type $elemTypeName, $parquetVal")
    }
  }

  /**
   * Decode the parquet `listVal` into a [[java.util.List]], with all elements (recursive) decoded
   */
  private def decodeList(elemType: DataType, listVal: ListParquetRecord): Any = {
    val elemTypeName = elemType.getTypeName

    if (seqDecodeMap.contains(elemTypeName)) {
      // List of primitives
      return seqDecodeMap(elemTypeName).decode(listVal, codecConf).asJava
    }

    elemType match {
      case x: ArrayType =>
        // List of lists
        listVal.map { case y: ListParquetRecord =>
          y.map(z => decode(x.getElementType, z)).asJava
        }.asJava
      case x: MapType =>
        // List of maps
        listVal.map { case y: MapParquetRecord =>
          decodeMap(x.getKeyType, x.getValueType, y)
        }.asJava
      case x: StructType =>
        // List of records
        listVal.map { case y: RowParquetRecord => RowParquetRecordImpl(y, x, timeZone) }.asJava
      case _ => throw new RuntimeException(s"Unknown non-primitive list decode type $elemTypeName")
    }
  }

  /**
   * Decode the parquet `mapVal` into a [[java.util.Map]], with all entries (recursive) decoded
   */
  private def decodeMap(
      keyType: DataType,
      valueType: DataType,
      mapVal: MapParquetRecord): java.util.Map[Any, Any] = {
    mapVal.map { case (keyParquetVal, valParquetVal) =>
      decode(keyType, keyParquetVal) -> decode(valueType, valParquetVal)
    }.toMap.asJava
  }

  ///////////////////////////////////////////////////////////////////////////
  // Useful Custom Decoders and type -> decoder Maps
  ///////////////////////////////////////////////////////////////////////////

  /**
   * parquet4s.ValueCodec.decimalCodec doesn't match on IntValue, but it should.
   *
   * So, we create our own version that does.
   *
   * It should only ever be used to decode, not encode.
   */
  private val customDecimalCodec: ValueCodec[java.math.BigDecimal] =
    new OptionalValueCodec[java.math.BigDecimal] {

      override def decodeNonNull(
          value: Value,
          configuration: ValueCodecConfiguration): java.math.BigDecimal = {
        value match {
          case IntValue(int) => new java.math.BigDecimal(int)
          case DoubleValue(double) => BigDecimal.decimal(double).bigDecimal
          case FloatValue(float) => BigDecimal.decimal(float).bigDecimal
          case BinaryValue(binary) => Decimals.decimalFromBinary(binary).bigDecimal
          case _ => throw new RuntimeException(s"Unknown decimal decode type $value")
        }
      }

      /** should NEVER be called */
      override def encodeNonNull(
          data: java.math.BigDecimal,
          configuration: ValueCodecConfiguration): Value = {
        throw new UnsupportedOperationException("Shouldn't be encoding in the reader (decimal)")
      }
    }

  /**
   * Decode parquet array into a [[Seq]].
   *
   * parquet4s decodes all list records into [[Array]]s, but we cannot implement the Java method
   * `<T> T[] getArray(String field)` in Scala due to type erasure.
   *
   * If we convert the parquet arrays, instead, into [[Seq]]s, then we can implement the Java method
   * `<T> List<T> getList(String fieldName)` in Scala.
   *
   * This should only ever be used to decode, not encode.
   */
  private def customSeqCodec[T](elementCodec: ValueCodec[T])(implicit
      classTag: ClassTag[T],
      factory: Factory[T, Seq[T]]
  ): ValueCodec[Seq[T]] = new OptionalValueCodec[Seq[T]] {

    override def decodeNonNull(
        value: Value,
        configuration: ValueCodecConfiguration): Seq[T] = {
      value match {
        case listRecord: ListParquetRecord =>
          listRecord.map(elementCodec.decode(_, codecConf))
        case binaryValue: BinaryValue if classTag.runtimeClass == classOf[Byte] =>
          binaryValue.value.getBytes.asInstanceOf[Seq[T]]
        case _ => throw new RuntimeException(s"Unknown list decode type $value")
      }
    }

    /** should NEVER be called */
    override def encodeNonNull(
        data: Seq[T],
        configuration: ValueCodecConfiguration): Value = {
      throw new UnsupportedOperationException("Shouldn't be encoding in the reader (seq)")
    }
  }

  private val primitiveDecodeMap = Map(
    new IntegerType().getTypeName -> ValueCodec.intCodec,
    new LongType().getTypeName -> ValueCodec.longCodec,
    new ByteType().getTypeName -> ValueCodec.byteCodec,
    new ShortType().getTypeName -> ValueCodec.shortCodec,
    new BooleanType().getTypeName -> ValueCodec.booleanCodec,
    new FloatType().getTypeName -> ValueCodec.floatCodec,
    new DoubleType().getTypeName -> ValueCodec.doubleCodec
  )

  private val primitiveNullableDecodeMap = Map(
    new StringType().getTypeName -> ValueCodec.stringCodec,
    new BinaryType().getTypeName -> ValueCodec.arrayCodec[Byte, Array],
    new DecimalType(1, 1).getTypeName -> customDecimalCodec,
    new TimestampType().getTypeName -> ValueCodec.sqlTimestampCodec,
    new DateType().getTypeName -> ValueCodec.sqlDateCodec
  )

  private val seqDecodeMap = Map(
    new IntegerType().getTypeName -> customSeqCodec[Int](ValueCodec.intCodec),
    new LongType().getTypeName -> customSeqCodec[Long](ValueCodec.longCodec),
    new ByteType().getTypeName -> customSeqCodec[Byte](ValueCodec.byteCodec),
    new ShortType().getTypeName -> customSeqCodec[Short](ValueCodec.shortCodec),
    new BooleanType().getTypeName -> customSeqCodec[Boolean](ValueCodec.booleanCodec),
    new FloatType().getTypeName -> customSeqCodec[Float](ValueCodec.floatCodec),
    new DoubleType().getTypeName -> customSeqCodec[Double](ValueCodec.doubleCodec),
    new StringType().getTypeName -> customSeqCodec[String](ValueCodec.stringCodec),
    new BinaryType().getTypeName -> customSeqCodec[Array[Byte]](ValueCodec.arrayCodec[Byte, Array]),
    new DecimalType(1, 1).getTypeName ->
      customSeqCodec[java.math.BigDecimal](customDecimalCodec),
    new TimestampType().getTypeName -> customSeqCodec[Timestamp](ValueCodec.sqlTimestampCodec),
    new DateType().getTypeName -> customSeqCodec[Date](ValueCodec.sqlDateCodec)
  )
}
