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
package io.delta.kernel.test

import io.delta.kernel.data.{ColumnVector, MapValue}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._

import java.lang.{Boolean => BooleanJ, Double => DoubleJ, Float => FloatJ, Byte => ByteJ}
import scala.collection.JavaConverters._
import org.scalatest.Assertions.convertToEqualizer

trait VectorTestUtils {

  protected def booleanVector(values: Seq[BooleanJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = BooleanType.BOOLEAN

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getBoolean(rowId: Int): Boolean = values(rowId)
    }
  }

  protected def byteVector(values: Seq[ByteJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = ByteType.BYTE

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getByte(rowId: Int): Byte = values(rowId)
    }
  }

  protected def timestampVector(values: Seq[Long]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = TimestampType.TIMESTAMP

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == -1

      // Values are stored as Longs representing milliseconds since epoch
      override def getLong(rowId: Int): Long = values(rowId)
    }
  }

  protected def stringVector(values: Seq[String]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = StringType.STRING

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getString(rowId: Int): String = values(rowId)
    }
  }

  protected def mapTypeVector(values: Seq[Map[String, String]]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = new MapType(StringType.STRING, StringType.STRING, true)

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getMap(rowId: Int): MapValue =
        VectorUtils.stringStringMapValue(values(rowId).asJava)
    }
  }

  protected def floatVector(values: Seq[FloatJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = FloatType.FLOAT

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = (values(rowId) == null)

      override def getFloat(rowId: Int): Float = values(rowId)
    }
  }

  protected def doubleVector(values: Seq[DoubleJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = DoubleType.DOUBLE

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = (values(rowId) == null)

      override def getDouble(rowId: Int): Double = values(rowId)
    }
  }

  def longVector(values: Long*): ColumnVector = new ColumnVector {
    override def getDataType: DataType = LongType.LONG

    override def getSize: Int = values.size

    override def close(): Unit = {}

    override def isNullAt(rowId: Int): Boolean = false

    override def getLong(rowId: Int): Long = values(rowId)
  }

  def selectSingleElement(size: Int, selectRowId: Int): ColumnVector = new ColumnVector {
    override def getDataType: DataType = BooleanType.BOOLEAN

    override def getSize: Int = size

    override def close(): Unit = {}

    override def isNullAt(rowId: Int): Boolean = false

    override def getBoolean(rowId: Int): Boolean = rowId == selectRowId
  }

  protected def checkVectors[T](
      actual: ColumnVector,
      expected: ColumnVector,
      expectedType: DataType,
      getValue: (ColumnVector, Int) => T,
      errorMessageFn: (Int, T, T) => String = (rowId: Int, exp: T, act: T) =>
        s"unexpected value at $rowId"
  ): Unit = {

    assert(actual.getDataType === expectedType)
    assert(actual.getDataType === expected.getDataType)
    assert(actual.getSize === expected.getSize)

    Seq.range(0, actual.getSize).foreach { rowId =>
      assert(actual.isNullAt(rowId) === expected.isNullAt(rowId))
      if (!actual.isNullAt(rowId)) {
        val actualValue = getValue(actual, rowId)
        val expectedValue = getValue(expected, rowId)
        assert(
          actualValue === expectedValue,
          errorMessageFn(rowId, expectedValue, actualValue)
        )
      }
    }
  }
}
