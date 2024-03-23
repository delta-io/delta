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

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.{TestUtils, VectorTestUtils}
import io.delta.kernel.expressions._
import io.delta.kernel.types._
import java.lang.{
  Byte => ByteJ,
  Short => ShortJ, Integer => IntegerJ, Long => LongJ, Double => DoubleJ, Float => FloatJ, String => StringJ}

trait ExpressionSuiteBase extends TestUtils with VectorTestUtils {
  /** create a columnar batch of given `size` with zero columns in it. */
  protected def zeroColumnBatch(rowCount: Int): ColumnarBatch = {
    new DefaultColumnarBatch(rowCount, new StructType(), new Array[ColumnVector](0))
  }

  protected def and(left: Predicate, right: Predicate): And = {
    new And(left, right)
  }

  protected def or(left: Predicate, right: Predicate): Or = {
    new Or(left, right)
  }

  protected def comparator(symbol: String, left: Expression, right: Expression): Predicate = {
    new Predicate(symbol, left, right)
  }
  protected def checkVectors(actual: ColumnVector,
                             expected: ColumnVector,
                             dataType: DataType): Unit = {
    assert(actual.getDataType === expected.getDataType)
    assert(actual.getSize === expected.getSize)
    Seq.range(0, actual.getSize).foreach { rowId =>
      assert(actual.isNullAt(rowId) === expected.isNullAt(rowId))
      if (!actual.isNullAt(rowId)) {
        dataType match {
          case BooleanType.BOOLEAN => assert(
            actual.getBoolean(rowId) === expected.getBoolean(rowId),
            s"unexpected value at $rowId")
          case ByteType.BYTE => assert(
            actual.getByte(rowId) === expected.getByte(rowId),
            s"unexpected value at $rowId")
          case ShortType.SHORT => assert(
            actual.getShort(rowId) === expected.getShort(rowId),
            s"unexpected value at $rowId")
          case IntegerType.INTEGER => assert(
            actual.getInt(rowId) === expected.getInt(rowId),
            s"unexpected value at $rowId")
          case LongType.LONG => assert(
            actual.getLong(rowId) === expected.getLong(rowId),
            s"unexpected value at $rowId")
          case FloatType.FLOAT => assert(
            actual.getFloat(rowId) === expected.getFloat(rowId),
            s"unexpected value at $rowId")
          case DoubleType.DOUBLE => assert(
            actual.getDouble(rowId) === expected.getDouble(rowId),
            s"unexpected value at $rowId")
          case StringType.STRING => assert(
            actual.getString(rowId) === expected.getString(rowId),
            s"unexpected value at $rowId")
          case _ => new UnsupportedOperationException("date type is not supported")
        }
      }
    }
  }

  protected def checkBooleanVectors(actual: ColumnVector, expected: ColumnVector): Unit = {
    assert(actual.getDataType === expected.getDataType)
    assert(actual.getSize === expected.getSize)
    Seq.range(0, actual.getSize).foreach { rowId =>
      assert(actual.isNullAt(rowId) === expected.isNullAt(rowId))
      if (!actual.isNullAt(rowId)) {
        assert(
          actual.getBoolean(rowId) === expected.getBoolean(rowId),
          s"unexpected value at $rowId"
        )
      }
    }
  }

  /**
   * Create column vector from seq of number for each number type
   */
  protected def seqToColumnVector
  (kernelType: DataType, data: Seq[Any]): ColumnVector = kernelType match {
    case ByteType.BYTE =>
      val copy: Seq[ByteJ] = data.map {
        case i: Int => new ByteJ(i.toByte)
        case i: Byte => new ByteJ(i)
        case _ => null
      }
      byteVector(copy)
    case ShortType.SHORT =>
      val copy: Seq[ShortJ] = data.map {
        case i: Int => new ShortJ(i.toShort)
        case i: Short => new ShortJ(i)
        case _ => null
      }
      shortVector(copy)
    case IntegerType.INTEGER =>
      val copy: Seq[IntegerJ] = data.map {
        case i: Int => IntegerJ.valueOf(i)
        case _ => null
      }
      integerVector(copy)
    case LongType.LONG =>
      val copy: Seq[LongJ] = data.map {
        case i: Int => LongJ.valueOf(i)
        case i: Long => LongJ.valueOf(i)
        case _ => null
      }
      longVector(copy)
    case FloatType.FLOAT =>
      val copy: Seq[FloatJ] = data.map {
        case i: Int => FloatJ.valueOf(i)
        case i: Float => FloatJ.valueOf(i)
        case _ => null
      }
      floatVector(copy)
    case DoubleType.DOUBLE =>
      val copy: Seq[DoubleJ] = data.map {
        case i: Int => DoubleJ.valueOf(i)
        case i: Double => DoubleJ.valueOf(i)
        case _ => null
      }
      doubleVector(copy)
    case StringType.STRING =>
      val copy: Seq[StringJ] = data.map {
        case i: String => i
        case _ => null
      }
      stringVector(copy)
    case null => null
  }

  /**
   * Create test column vector of size: size and value: value at every n-1 index
   *     where n = valueAtEveryNth
   * for example:
   *  size = 5, valueAtEveryNth = 2, value = 1
   *  will return Seq[null,1,null,1,null]
   */
  protected def getTestColumnVectors
  (kernelType: DataType,
   size: Int,
   valueAtEveryNth: Int,
   value: Int): ColumnVector = kernelType match {
    case ByteType.BYTE =>
      val data: Seq[ByteJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else new ByteJ(value.toByte)
      }
      byteVector(data)
    case ShortType.SHORT =>
      val data: Seq[ShortJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else new ShortJ(value.toShort)
      }
      shortVector(data)
    case IntegerType.INTEGER =>
      val data: Seq[IntegerJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else IntegerJ.valueOf(value)
      }
      integerVector(data)
    case LongType.LONG =>
      val data: Seq[LongJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else LongJ.valueOf(value)
      }
      longVector(data)
    case FloatType.FLOAT =>
      val data: Seq[FloatJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else FloatJ.valueOf(value)
      }
      floatVector(data)
    case DoubleType.DOUBLE =>
      val data: Seq[DoubleJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else DoubleJ.valueOf(value)
      }
      doubleVector(data)
    case StringType.STRING =>
      val data: Seq[StringJ] = (1 to size).map {
        index => if (index % valueAtEveryNth != 0) null else StringJ.valueOf(value)
      }
      stringVector(data)
    case null => null
  }
}
