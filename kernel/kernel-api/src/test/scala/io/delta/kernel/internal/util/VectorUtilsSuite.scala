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

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.internal.data.GenericRow

import java.sql.{Date, Timestamp}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructType,
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
import java.util

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

  test(s"handle array of struct correctly") {
    val structType =
      new StructType().add("name", StringType.STRING).add("value", IntegerType.INTEGER)

    val arrayType = new ArrayType(structType, true)

    def row(name: String, value: Integer): Row = {
      val map = new util.HashMap[Integer, AnyRef]
      map.put(0, name)
      map.put(1, value)
      new GenericRow(structType, map)
    }

    val values = List[ArrayValue](
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[Row](
            row("a1", 1),
            row("a2", 2)
          ).asJava,
          structType
        )
      },
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[Row](
            row("b1", 3),
            row("b2", 4)
          ).asJava,
          structType
        )
      },
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[Row](
            row("c1", 5),
            row("c2", 6)
          ).asJava,
          structType
        )
      },
      null
    )

    val columnVector = VectorUtils.buildColumnVector(values.asJava, arrayType)

    // Test size
    assert(columnVector.getSize == 4)

    // Test first array
    val array0 = columnVector.getArray(0)
    val struct0 = array0.getElements
    assert(struct0.getSize == 2)

    val nameVector0 = struct0.getChild(0)
    val valueVector0 = struct0.getChild(1)
    assert(nameVector0.getString(0) == "a1")
    assert(valueVector0.getInt(0) == 1)
    assert(nameVector0.getString(1) == "a2")
    assert(valueVector0.getInt(1) == 2)

    // Test second array
    val array1 = columnVector.getArray(1)
    val struct1 = array1.getElements
    assert(struct1.getSize == 2)

    val nameVector1 = struct1.getChild(0)
    val valueVector1 = struct1.getChild(1)
    assert(nameVector1.getString(0) == "b1")
    assert(valueVector1.getInt(0) == 3)
    assert(nameVector1.getString(1) == "b2")
    assert(valueVector1.getInt(1) == 4)

    // Test third array
    val array2 = columnVector.getArray(2)
    val struct2 = array2.getElements
    assert(struct2.getSize == 2)

    val nameVector2 = struct2.getChild(0)
    val valueVector2 = struct2.getChild(1)
    assert(nameVector2.getString(0) == "c1")
    assert(valueVector2.getInt(0) == 5)
    assert(nameVector2.getString(1) == "c2")
    assert(valueVector2.getInt(1) == 6)

    // Test null value
    assert(columnVector.isNullAt(3))
  }

  test(s"handle array of map correctly") {
    val mapType = new MapType(StringType.STRING, IntegerType.INTEGER, true)
    val arrayType = new ArrayType(mapType, true)

    val values = List[ArrayValue](
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[MapValue](
            new MapValue {
              override def getSize: Int = 2
              override def getKeys: ColumnVector =
                VectorUtils.buildColumnVector(List("a1", "a2").asJava, StringType.STRING)
              override def getValues: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](1, 2).asJava, IntegerType.INTEGER)
            },
            new MapValue {
              override def getSize: Int = 2
              override def getKeys: ColumnVector =
                VectorUtils.buildColumnVector(List("a3", "a4").asJava, StringType.STRING)
              override def getValues: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](3, 4).asJava, IntegerType.INTEGER)
            }
          ).asJava,
          mapType
        )
      },
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[MapValue](
            new MapValue {
              override def getSize: Int = 2
              override def getKeys: ColumnVector =
                VectorUtils.buildColumnVector(List("b1", "b2").asJava, StringType.STRING)
              override def getValues: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](5, 6).asJava, IntegerType.INTEGER)
            },
            new MapValue {
              override def getSize: Int = 2
              override def getKeys: ColumnVector =
                VectorUtils.buildColumnVector(List("b3", "b4").asJava, StringType.STRING)
              override def getValues: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](7, 8).asJava, IntegerType.INTEGER)
            }
          ).asJava,
          mapType
        )
      },
      null
    )

    val columnVector = VectorUtils.buildColumnVector(values.asJava, arrayType)

    // Test size
    assert(columnVector.getSize == 3)

    // Test first array
    val firstArray = columnVector.getArray(0)
    val firstArrayMaps = firstArray.getElements
    assert(firstArrayMaps.getSize == 2)

    val firstArrayFirstMap = firstArrayMaps.getMap(0)
    assert(firstArrayFirstMap.getKeys.getString(0) == "a1")
    assert(firstArrayFirstMap.getKeys.getString(1) == "a2")
    assert(firstArrayFirstMap.getValues.getInt(0) == 1)
    assert(firstArrayFirstMap.getValues.getInt(1) == 2)

    val firstArraySecondMap = firstArrayMaps.getMap(1)
    assert(firstArraySecondMap.getKeys.getString(0) == "a3")
    assert(firstArraySecondMap.getKeys.getString(1) == "a4")
    assert(firstArraySecondMap.getValues.getInt(0) == 3)
    assert(firstArraySecondMap.getValues.getInt(1) == 4)

    // Test second array
    val secondArray = columnVector.getArray(1)
    val secondArrayMaps = secondArray.getElements
    assert(secondArrayMaps.getSize == 2)

    val secondArrayFirstMap = secondArrayMaps.getMap(0)
    assert(secondArrayFirstMap.getKeys.getString(0) == "b1")
    assert(secondArrayFirstMap.getKeys.getString(1) == "b2")
    assert(secondArrayFirstMap.getValues.getInt(0) == 5)
    assert(secondArrayFirstMap.getValues.getInt(1) == 6)

    val secondArraySecondMap = secondArrayMaps.getMap(1)
    assert(secondArraySecondMap.getKeys.getString(0) == "b3")
    assert(secondArraySecondMap.getKeys.getString(1) == "b4")
    assert(secondArraySecondMap.getValues.getInt(0) == 7)
    assert(secondArraySecondMap.getValues.getInt(1) == 8)

    // Test null value
    assert(columnVector.isNullAt(2))
  }

  test(s"handle array of array correctly") {
    val innerArrayType = new ArrayType(IntegerType.INTEGER, true)
    val outerArrayType = new ArrayType(innerArrayType, true)

    val values = List[ArrayValue](
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[ArrayValue](
            new ArrayValue {
              override def getSize: Int = 2
              override def getElements: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](1, 2).asJava, IntegerType.INTEGER)
            },
            new ArrayValue {
              override def getSize: Int = 2
              override def getElements: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](3, 4).asJava, IntegerType.INTEGER)
            }
          ).asJava,
          innerArrayType
        )
      },
      new ArrayValue {
        override def getSize: Int = 2
        override def getElements: ColumnVector = VectorUtils.buildColumnVector(
          List[ArrayValue](
            new ArrayValue {
              override def getSize: Int = 2
              override def getElements: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](5, 6).asJava, IntegerType.INTEGER)
            },
            new ArrayValue {
              override def getSize: Int = 2
              override def getElements: ColumnVector =
                VectorUtils.buildColumnVector(List[IntegerJ](7, 8).asJava, IntegerType.INTEGER)
            }
          ).asJava,
          innerArrayType
        )
      },
      null
    )

    val columnVector = VectorUtils.buildColumnVector(values.asJava, outerArrayType)

    // Test size
    assert(columnVector.getSize == 3)

    // Test first outer array
    val firstOuterArray = columnVector.getArray(0)
    val firstOuterArrayElements = firstOuterArray.getElements
    assert(firstOuterArrayElements.getSize == 2)

    val firstOuterArrayFirstInner = firstOuterArrayElements.getArray(0)
    val firstOuterArrayFirstInnerElements = firstOuterArrayFirstInner.getElements
    assert(firstOuterArrayFirstInnerElements.getInt(0) == 1)
    assert(firstOuterArrayFirstInnerElements.getInt(1) == 2)

    val firstOuterArraySecondInner = firstOuterArrayElements.getArray(1)
    val firstOuterArraySecondInnerElements = firstOuterArraySecondInner.getElements
    assert(firstOuterArraySecondInnerElements.getInt(0) == 3)
    assert(firstOuterArraySecondInnerElements.getInt(1) == 4)

    // Test second outer array
    val secondOuterArray = columnVector.getArray(1)
    val secondOuterArrayElements = secondOuterArray.getElements
    assert(secondOuterArrayElements.getSize == 2)

    val secondOuterArrayFirstInner = secondOuterArrayElements.getArray(0)
    val secondOuterArrayFirstInnerElements = secondOuterArrayFirstInner.getElements
    assert(secondOuterArrayFirstInnerElements.getInt(0) == 5)
    assert(secondOuterArrayFirstInnerElements.getInt(1) == 6)

    val secondOuterArraySecondInner = secondOuterArrayElements.getArray(1)
    val secondOuterArraySecondInnerElements = secondOuterArraySecondInner.getElements
    assert(secondOuterArraySecondInnerElements.getInt(0) == 7)
    assert(secondOuterArraySecondInnerElements.getInt(1) == 8)

    // Test null value
    assert(columnVector.isNullAt(2))
  }
}
