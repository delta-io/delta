/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.expressions

import java.math.RoundingMode

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import CastExpressionEvaluator.canCastTo
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._

/**
 * Test suite that covers applying cast expressions to [[ColumnVector]]s
 */
class CastExpressionEvaluatorSuite extends AnyFunSuite with TestUtils {

  private val allowedDecimalCasts: Set[(DataType, DataType)] = Set(
    (new DecimalType(12, 0), new DecimalType(14, 0)),
    (new DecimalType(5, 2), new DecimalType(7, 4)),
    (new DecimalType(35, 5), new DecimalType(38, 6)),

    (ByteType.BYTE, new DecimalType(4, 0)),
    (ByteType.BYTE, new DecimalType(10, 5)),
    (ShortType.SHORT, new DecimalType(6, 0)),
    (ShortType.SHORT, new DecimalType(10, 5)),
    (IntegerType.INTEGER, new DecimalType(11, 0)),
    (IntegerType.INTEGER, new DecimalType(20, 5)),
    (LongType.LONG, new DecimalType(21, 0)),
    (LongType.LONG, new DecimalType(30, 5))
  )

  private val allowedPrimitiveCasts: Set[(DataType, DataType)] = allowedDecimalCasts ++ Set(
    (ByteType.BYTE, ShortType.SHORT),
    (ByteType.BYTE, IntegerType.INTEGER),
    (ByteType.BYTE, LongType.LONG),
    (ByteType.BYTE, FloatType.FLOAT),
    (ByteType.BYTE, DoubleType.DOUBLE),

    (ShortType.SHORT, IntegerType.INTEGER),
    (ShortType.SHORT, LongType.LONG),
    (ShortType.SHORT, FloatType.FLOAT),
    (ShortType.SHORT, DoubleType.DOUBLE),

    (IntegerType.INTEGER, LongType.LONG),
    (IntegerType.INTEGER, FloatType.FLOAT),
    (IntegerType.INTEGER, DoubleType.DOUBLE),

    (LongType.LONG, FloatType.FLOAT),
    (LongType.LONG, DoubleType.DOUBLE),
    (FloatType.FLOAT, DoubleType.DOUBLE)
  )

  private val allowedNestedCasts: Set[(DataType, DataType)] = Set(
    // 1-level nesting: cast struct, map and array.
    (new StructType()
      .add("a", ByteType.BYTE)
      .add("b", ShortType.SHORT),
      new StructType()
        .add("a", IntegerType.INTEGER)
        .add("b", LongType.LONG)),
    (new MapType(LongType.LONG, ByteType.BYTE, false),
      new MapType(DoubleType.DOUBLE, IntegerType.INTEGER, false)),
    (new ArrayType(FloatType.FLOAT, false),
      new ArrayType(DoubleType.DOUBLE, false)),

    // 2-level nesting: cast struct, map and array nested into each others.
    (new StructType()
      .add("map", new MapType(IntegerType.INTEGER, ByteType.BYTE, false))
      .add("array", new ArrayType(IntegerType.INTEGER, false)),
      new StructType()
      .add("map", new MapType(DoubleType.DOUBLE, FloatType.FLOAT, false))
      .add("array", new ArrayType(DoubleType.DOUBLE, false))),
    (new MapType(
      new StructType().add("a", FloatType.FLOAT),
      new ArrayType(ByteType.BYTE, false),
      false),
    new MapType(
      new StructType().add("a", DoubleType.DOUBLE),
      new ArrayType(DoubleType.DOUBLE, false),
      false)),
    (new ArrayType(new StructType().add("a", ShortType.SHORT), false),
      new ArrayType(new StructType().add("a", FloatType.FLOAT), false)),
    (new ArrayType(new MapType(ByteType.BYTE, ByteType.BYTE, false), false),
      new ArrayType(new MapType(LongType. LONG, ShortType.SHORT, false), false))
  )

  private val allowedCasts: Set[(DataType, DataType)] =
    allowedPrimitiveCasts ++ allowedNestedCasts ++ Seq(
      // date -> timestamp_ntz requires converting days to microseconds to compare results and is
      // tested separately.
      (DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)
    )

  test("can cast to") {
    Seq.range(0, ALL_TYPES.length).foreach { fromTypeIdx =>
      val fromType: DataType = ALL_TYPES(fromTypeIdx)
      Seq.range(0, ALL_TYPES.length).foreach { toTypeIdx =>
        val toType: DataType = ALL_TYPES(toTypeIdx)
        assert(canCastTo(fromType, toType) ===
          fromType.equals(toType) || allowedCasts.contains((fromType, toType)))
      }
    }
  }

  test("can cast to nested and decimal") {
    (allowedDecimalCasts ++ allowedNestedCasts).foreach { case (from, to) =>
      assert(canCastTo(from, to), s"Casting $from to $to should be allowed")
      assert(!canCastTo(to, from), s"Casting $to to $from should not be allowed")
    }
  }

  (allowedPrimitiveCasts ++ allowedNestedCasts).foreach { castPair =>
    test(s"eval cast expression: ${castPair._1} -> ${castPair._2}") {
      val fromType = castPair._1
      val toType = castPair._2
      val inputVector = testData(87, fromType, (rowId) => rowId % 7 == 0)
      val outputVector = CastExpressionEvaluator.eval(inputVector, toType)
      checkCastOutput(inputVector, toType, outputVector)
    }
  }

  test(s"eval cast expression: date -> timestamp_ntz") {
    val inputVector = testData(87, DateType.DATE, (rowId) => rowId % 7 == 0)
    val outputVector = CastExpressionEvaluator.eval(inputVector, TimestampNTZType.TIMESTAMP_NTZ)
    val expectedVector = new ColumnVector {
      override def getDataType: DataType = TimestampNTZType.TIMESTAMP_NTZ
      override def getSize: Int = inputVector.getSize
      override def close(): Unit = inputVector.close()
      override def isNullAt(rowId: Int): Boolean = inputVector.isNullAt(rowId)
      override def getLong(rowId: Int): Long = {
        val expectedDays = inputVector.getInt(rowId)
        expectedDays * 24 * 60 * 60 * 1000000L
      }
    }
    checkCastOutput(expectedVector, TimestampNTZType.TIMESTAMP_NTZ, outputVector)
  }

  def testData(size: Int, dataType: DataType, nullability: (Int) => Boolean): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = dataType
      override def getSize: Int = size
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = nullability(rowId)

      override def getByte(rowId: Int): Byte = {
        assert(dataType === ByteType.BYTE)
        generateValue(rowId).toByte
      }

      override def getShort(rowId: Int): Short = {
        assert(dataType === ShortType.SHORT)
        generateValue(rowId).toShort
      }

      override def getInt(rowId: Int): Int = {
        assert(dataType === IntegerType.INTEGER || dataType === DateType.DATE)
        generateValue(rowId).toInt
      }

      override def getLong(rowId: Int): Long = {
        assert(dataType === LongType.LONG)
        generateValue(rowId).toLong
      }

      override def getFloat(rowId: Int): Float = {
        assert(dataType === FloatType.FLOAT)
        generateValue(rowId).toFloat
      }

      override def getDouble(rowId: Int): Double = {
        assert(dataType === DoubleType.DOUBLE)
        generateValue(rowId)
      }

      override def getDecimal(rowId: Int): java.math.BigDecimal = {
        dataType match {
          case d: DecimalType =>
            new java.math.BigDecimal(generateValue(rowId))
              .setScale(d.getScale, RoundingMode.HALF_UP)
          case _ =>
            fail(s"Wrong type, expected decimal, got $dataType")
        }
      }

      override def getChild(ordinal: Int): ColumnVector = {
        dataType match {
          case s: StructType =>
            testData(size, s.at(ordinal).getDataType, nullability = _ => false)
          case _ =>
            fail(s"Wrong type, expected struct, got $dataType")
        }
      }

      override def getMap(ordinal: Int): MapValue = {
        dataType match {
          case s: MapType =>
            // Use a fixed size of 3 key-value pairs for every map.
            val size = 3
            new MapValue() {
              override def getSize: Int = size

              override def getKeys: ColumnVector =
                testData(size, s.getKeyType, nullability = _ => false)

              override def getValues: ColumnVector =
                testData(size, s.getValueType, nullability = _ => false)
            }
          case _ =>
            fail(s"Wrong type, expected map, got $dataType")
        }
      }

      override def getArray(ordinal: Int): ArrayValue = {
        dataType match {
          case s: ArrayType =>
            // Use a fixed size of 3 elements for every array.
            val size = 3
            new ArrayValue() {
              override def getSize: Int = size

              override def getElements: ColumnVector =
                testData(size, s.getElementType, nullability = _ => false)
            }
          case _ =>
            fail(s"Wrong type, expected array, got $dataType")
        }
      }
    }
  }

  // Utility method to generate a value based on the rowId. Returned value is a double
  // which the callers can cast to appropriate numerical type.
  private def generateValue(rowId: Int): Double = rowId * 2.76 + 7623

  private def checkCastOutput(input: ColumnVector, toType: DataType, output: ColumnVector): Unit = {
    assert(input.getSize === output.getSize)
    assert(toType === output.getDataType)
    Seq.range(0, input.getSize).foreach { rowId =>
      assert(input.isNullAt(rowId) === output.isNullAt(rowId))
      assert(asScala(getValueAsObject(input, rowId)) === asScala(getValueAsObject(output, rowId)))
    }
  }

  // Transforms java types to scala types to allow comparing values that have different data types:
  // scala only considers the actual values and accepts that types may differ, but java will e.g.
  // consider that a List[Int] is always different from a List[Long], even if both contain the same
  // values.
  private def asScala(value: Any): Any = value match {
    case null => null
    case m: java.util.Map[_, _] =>
      m.entrySet().asScala.map { kv => asScala(kv.getKey) -> asScala(kv.getValue) }.toMap
    case l: java.util.List[_] =>
      l.asScala.map(asScala)
    case d: java.math.BigDecimal => BigDecimal(d)
    case other => other
  }
}
