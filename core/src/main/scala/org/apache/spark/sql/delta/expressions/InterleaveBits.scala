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

package org.apache.spark.sql.delta.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType}


/**
 * Interleaves the bits of its input data in a round-robin fashion.
 *
 * If the input data is seen as a series of multidimensional points, this function computes the
 * corresponding Z-values, in a way that's preserving data locality: input points that are close
 * in the multidimensional space will be mapped to points that are close on the Z-order curve.
 *
 * The returned value is a byte array where the size of the array is 4 * num of input columns.
 *
 * @see https://en.wikipedia.org/wiki/Z-order_curve
 *
 * @note Only supports input expressions of type Int for now.
 */
case class InterleaveBits(children: Seq[Expression])
  extends Expression with ExpectsInputTypes
    with CodegenFallback /* TODO: implement doGenCode() */ {

  private val n: Int = children.size

  override def inputTypes: Seq[DataType] = Seq.fill(n)(IntegerType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /** Nulls in the input will be treated like this value */
  val nullValue: Int = 0

  private val childrenArray: Array[Expression] = children.toArray

  private val ints = new Array[Int](n)

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < n) {
      val int = childrenArray(i).eval(input) match {
        case null => nullValue
        case int: Int => int
        case any => throw new IllegalArgumentException(
          s"${this.getClass.getSimpleName} expects only inputs of type Int, but got: " +
            s"$any of type${any.getClass.getSimpleName}")
      }
      ints.update(i, int)
      i += 1
    }

    val ret = new Array[Byte](n * 4)
    var ret_idx: Int = 0
    var ret_bit: Int = 7
    var ret_byte: Byte = 0

    var bit = 31 /* going from most to least significant bit */
    while (bit >= 0) {
      var idx = 0
      while (idx < n) {
        ret_byte = (ret_byte | (((ints(idx) >> bit) & 1) << ret_bit)).toByte
        ret_bit -= 1
        if (ret_bit == -1) {
          // finished processing a byte
          ret.update(ret_idx, ret_byte)
          ret_byte = 0
          ret_idx += 1
          ret_bit = 7
        }
        idx += 1
      }
      bit -= 1
    }
    assert(ret_idx == n * 4)
    assert(ret_bit == 7)
    ret
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): InterleaveBits = copy(children = newChildren)
}
