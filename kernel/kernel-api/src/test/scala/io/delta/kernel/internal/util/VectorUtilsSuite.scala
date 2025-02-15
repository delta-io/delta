/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.util

import java.sql.{Date, Timestamp}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  TimestampType
}

import java.lang.{
  Boolean => BooleanJ,
  Byte => ByteJ,
  Double => DoubleJ,
  Float => FloatJ,
  Integer => IntegerJ,
  Long => LongJ,
  Short => ShortJ
}
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Tables.Table

import java.math.BigDecimal

class VectorUtilsSuite extends AnyFunSuite with VectorTestUtils {

  Table(
    ("values", "dataType"),
    (List[ByteJ](1.toByte, 2.toByte, 3.toByte, null), ByteType.BYTE),
    (List[ShortJ](1.toShort, 2.toShort, 3.toShort, null), ShortType.SHORT),
    (List[IntegerJ](1, 2, 3, null), IntegerType.INTEGER),
    (List[LongJ](1L, 2L, 3L, null), LongType.LONG),
    (List[FloatJ](1.0f, 2.0f, 3.0f, null), FloatType.FLOAT),
    (List[DoubleJ](1.0, 2.0, 3.0, null), DoubleType.DOUBLE),
    (List[Array[Byte]]("one".getBytes, "two".getBytes, "three".getBytes, null), BinaryType.BINARY),
    (List[BooleanJ](true, false, false, null), BooleanType.BOOLEAN),
    (
      List[BigDecimal](new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("3"), null),
      new DecimalType(10, 2)
    ),
    (List[String]("one", "two", "three", null), StringType.STRING),
    (
      List[IntegerJ](10, 20, 30, null),
      DateType.DATE
    ),
    (
      List[LongJ](
        Timestamp.valueOf("2023-01-01 00:00:00").getTime,
        Timestamp.valueOf("2023-01-02 00:00:00").getTime,
        Timestamp.valueOf("2023-01-03 00:00:00").getTime,
        null
      ),
      TimestampType.TIMESTAMP
    )
  ).foreach(
    testCase =>
      test(s"handle ${testCase._2} array correctly") {
        val values = testCase._1
        val dataType = testCase._2
        val columnVector = VectorUtils.buildColumnVector(values.asJava, dataType)
        assert(columnVector.getSize == 4)

        dataType match {
          case ByteType.BYTE =>
            assert(columnVector.getByte(0) == 1.toByte)
            assert(columnVector.getByte(1) == 2.toByte)
            assert(columnVector.getByte(2) == 3.toByte)
          case ShortType.SHORT =>
            assert(columnVector.getShort(0) == 1.toShort)
            assert(columnVector.getShort(1) == 2.toShort)
            assert(columnVector.getShort(2) == 3.toShort)
          case IntegerType.INTEGER =>
            assert(columnVector.getInt(0) == 1)
            assert(columnVector.getInt(1) == 2)
            assert(columnVector.getInt(2) == 3)
          case LongType.LONG =>
            assert(columnVector.getLong(0) == 1L)
            assert(columnVector.getLong(1) == 2L)
            assert(columnVector.getLong(2) == 3L)
          case FloatType.FLOAT =>
            assert(columnVector.getFloat(0) == 1.0f)
            assert(columnVector.getFloat(1) == 2.0f)
            assert(columnVector.getFloat(2) == 3.0f)
          case DoubleType.DOUBLE =>
            assert(columnVector.getDouble(0) == 1.0)
            assert(columnVector.getDouble(1) == 2.0)
            assert(columnVector.getDouble(2) == 3.0)
          case BooleanType.BOOLEAN =>
            assert(columnVector.getBoolean(0))
            assert(!columnVector.getBoolean(1))
            assert(!columnVector.getBoolean(2))
          case _: DecimalType =>
            assert(columnVector.getDecimal(0) == new BigDecimal("1"))
            assert(columnVector.getDecimal(1) == new BigDecimal("2"))
            assert(columnVector.getDecimal(2) == new BigDecimal("3"))
          case BinaryType.BINARY =>
            assert(columnVector.getBinary(0) sameElements "one".getBytes)
            assert(columnVector.getBinary(1) sameElements "two".getBytes)
            assert(columnVector.getBinary(2) sameElements "three".getBytes)
          case StringType.STRING =>
            assert(columnVector.getString(0) == "one")
            assert(columnVector.getString(1) == "two")
            assert(columnVector.getString(2) == "three")
          case DateType.DATE =>
            assert(columnVector.getInt(0) == 10)
            assert(columnVector.getInt(1) == 20)
            assert(columnVector.getInt(2) == 30)
          case TimestampType.TIMESTAMP =>
            assert(
              columnVector.getLong(0) == Timestamp.valueOf("2023-01-01 00:00:00").getTime
            )
            assert(
              columnVector.getLong(1) == Timestamp.valueOf("2023-01-02 00:00:00").getTime
            )
            assert(
              columnVector.getLong(2) == Timestamp.valueOf("2023-01-03 00:00:00").getTime
            )
        }
        assert(columnVector.isNullAt(3))
      }
  )
}
