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

import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.defaults.utils.{DefaultVectorTestUtils, TestUtils}
import io.delta.kernel.expressions._
import io.delta.kernel.types._

import scala.collection.JavaConverters._

trait ExpressionSuiteBase extends TestUtils with DefaultVectorTestUtils {
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

  protected def like(
      left: Expression, right: Expression, escape: Option[Character] = None): Predicate = {
    if (escape.isDefined && escape.get!=null) {
      like(List(left, right, Literal.ofString(escape.get.toString, "UTF8_BINARY")))
    } else like(List(left, right))
  }

  protected def like(children: List[Expression]): Predicate = {
    new Predicate("like", children.asJava)
  }

  protected def comparator(symbol: String, left: Expression, right: Expression): Predicate = {
    new Predicate(symbol, left, right)
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

  protected def checkTimestampVectors(actual: ColumnVector, expected: ColumnVector): Unit = {
    assert(actual.getSize === expected.getSize)
    for (rowId <- 0 until actual.getSize) {
      if (expected.isNullAt(rowId)) {
        assert(actual.isNullAt(rowId), s"Expected null at row $rowId")
      } else {
        val expectedValue = getValueAsObject(expected, rowId).asInstanceOf[Long]
        val actualValue = getValueAsObject(actual, rowId).asInstanceOf[Long]
        assert(actualValue === expectedValue, s"Unexpected value at row $rowId")
      }
    }
  }
}
