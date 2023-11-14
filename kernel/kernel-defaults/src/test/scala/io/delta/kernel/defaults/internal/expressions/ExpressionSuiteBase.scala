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

import java.lang.{Boolean => BooleanJ}
import java.util

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.expressions._
import io.delta.kernel.types._

trait ExpressionSuiteBase extends TestUtils {
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
    new Predicate(symbol, util.Arrays.asList(left, right))
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

  protected def booleanVector(values: Seq[BooleanJ]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = BooleanType.BOOLEAN

      override def getSize: Int = values.length

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = values(rowId) == null

      override def getBoolean(rowId: Int): Boolean = values(rowId)
    }
  }
}
