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

import java.lang.{Boolean => BooleanJ, Double => DoubleJ, Float => FloatJ}
import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.types._

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

  protected def stringVector(values: Seq[String]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = StringType.STRING

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getString(rowId: Int): String = values(rowId)
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

  /**
   * Returns a [[ColumnarBatch]] with each given vector is a top-level column col_i where i is
   * the index of the vector in the input list.
   */
  protected def columnarBatch(vectors: ColumnVector*): ColumnarBatch = {
    val numRows = vectors.head.getSize
    vectors.tail.foreach(
      v => require(v.getSize == numRows, "All vectors should have the same size"))

    val schema = (0 until vectors.length)
      .foldLeft(new StructType())((s, i) => s.add(s"col_$i", vectors(i).getDataType))

    new DefaultColumnarBatch(numRows, schema, vectors.toArray)
  }
}
