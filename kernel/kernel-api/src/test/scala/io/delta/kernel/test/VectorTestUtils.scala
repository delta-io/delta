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

import java.lang.{Boolean => BooleanJ, Double => DoubleJ, Float => FloatJ, Integer => IntegerJ, Long => LongJ}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, MapValue, Row}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator

trait VectorTestUtils {

  protected def emptyActionsIterator = new CloseableIterator[Row] {
    override def hasNext: Boolean = false
    override def next(): Row = throw new NoSuchElementException("No more elements")
    override def close(): Unit = {}
  }

  protected def emptyColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = null
    override def getColumnVector(ordinal: Int): ColumnVector = null
    override def getSize: Int = 0
  }

  protected def booleanVector(values: Seq[BooleanJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = BooleanType.BOOLEAN

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getBoolean(rowId: Int): Boolean = values(rowId)
    }
  }

  protected def timestampVector(values: Seq[LongJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = TimestampType.TIMESTAMP

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null || values(rowId) == -1

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

  protected def byteVector(values: Seq[java.lang.Byte]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = ByteType.BYTE

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getByte(rowId: Int): Byte = values(rowId)
    }
  }

  protected def intVector(values: Seq[IntegerJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = IntegerType.INTEGER

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getInt(rowId: Int): Int = values(rowId)
    }
  }

  protected def floatVector(values: Seq[FloatJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = FloatType.FLOAT

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getFloat(rowId: Int): Float = values(rowId)
    }
  }

  protected def doubleVector(values: Seq[DoubleJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = DoubleType.DOUBLE

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getDouble(rowId: Int): Double = values(rowId)
    }
  }

  def longVector(values: Seq[LongJ]): ColumnVector = new ColumnVector {
    override def getDataType: DataType = LongType.LONG

    override def getSize: Int = values.length

    override def close(): Unit = {}

    override def isNullAt(rowId: Int): Boolean = values(rowId) == null

    override def getLong(rowId: Int): Long = values(rowId)
  }

  def selectSingleElement(size: Int, selectRowId: Int): ColumnVector = new ColumnVector {
    override def getDataType: DataType = BooleanType.BOOLEAN

    override def getSize: Int = size

    override def close(): Unit = {}

    override def isNullAt(rowId: Int): Boolean = false

    override def getBoolean(rowId: Int): Boolean = rowId == selectRowId
  }
}
