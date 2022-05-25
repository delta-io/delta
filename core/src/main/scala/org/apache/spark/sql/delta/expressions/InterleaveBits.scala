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
    interleaveBits(ints)
  }

  def defaultInterleaveBits(): Array[Byte] = {
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

  def interleaveBits(inputs: Array[Int]): Array[Byte] = {
    inputs.length match {
      // it's a more fast approach
      // can see http://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
      case 0 => Array.empty
      case 1 => intToByte(inputs(0))
      case 2 => interleave2Ints(inputs(1), inputs(0))
      case 3 => interleave3Ints(inputs(2), inputs(1), inputs(0))
      case 4 => interleave4Ints(inputs(3), inputs(2), inputs(1), inputs(0))
      case 5 => interleave5Ints(inputs(4), inputs(3), inputs(2), inputs(1), inputs(0))
      case 6 => interleave6Ints(inputs(5), inputs(4), inputs(3), inputs(2), inputs(1), inputs(0))
      case 7 => interleave7Ints(inputs(6), inputs(5), inputs(4), inputs(3), inputs(2), inputs(1),
        inputs(0))
      case 8 => interleave8Ints(inputs(7), inputs(6), inputs(5), inputs(4), inputs(3), inputs(2),
        inputs(1), inputs(0))
      case _ => defaultInterleaveBits()
    }
  }

  private def interleave2Ints(i1: Int, i2: Int): Array[Byte] = {
    val result = new Array[Byte](8)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte

      var z = 0
      var j = 0
      while (j < 8) {
        val x_masked = tmp1 & (1 << j)
        val y_masked = tmp2 & (1 << j)
        z |= (x_masked << j)
        z |= (y_masked << (j + 1))
        j = j + 1
      }
      result((3 - i) * 2 + 1) = (z & 0xFF).toByte
      result((3 - i) * 2) = ((z >> 8) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  def intToByte(input: Int): Array[Byte] = {
    val result = new Array[Byte](4)
    var i = 0
    while (i <= 3) {
      val offset = i * 8
      result(3 - i) = ((input >> offset) & 0xFF).toByte
      i += 1
    }
    result
  }

  private def interleave3Ints(i1: Int, i2: Int, i3: Int): Array[Byte] = {
    val result = new Array[Byte](12)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte

      var z = 0
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        z |= (r1_mask << (2 * j)) | (r2_mask << (2 * j + 1)) | (r3_mask << (2 * j + 2))
        j = j + 1
      }
      result((3 - i) * 3 + 2) = (z & 0xFF).toByte
      result((3 - i) * 3 + 1) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 3) = ((z >> 16) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  private def interleave4Ints(i1: Int, i2: Int, i3: Int, i4: Int): Array[Byte] = {
    val result = new Array[Byte](16)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte
      val tmp4 = ((i4 >> (i * 8)) & 0xFF).toByte

      var z = 0
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        z |= (r1_mask << (3 * j)) | (r2_mask << (3 * j + 1)) | (r3_mask << (3 * j + 2)) |
          (r4_mask << (3 * j + 3))
        j = j + 1
      }
      result((3 - i) * 4 + 3) = (z & 0xFF).toByte
      result((3 - i) * 4 + 2) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 4 + 1) = ((z >> 16) & 0xFF).toByte
      result((3 - i) * 4) = ((z >> 24) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  private def interleave5Ints(
      i1: Int,
      i2: Int,
      i3: Int,
      i4: Int,
      i5: Int): Array[Byte] = {
    val result = new Array[Byte](20)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte
      val tmp4 = ((i4 >> (i * 8)) & 0xFF).toByte
      val tmp5 = ((i5 >> (i * 8)) & 0xFF).toByte

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        z |= (r1_mask << (4 * j)) | (r2_mask << (4 * j + 1)) | (r3_mask << (4 * j + 2)) |
          (r4_mask << (4 * j + 3)) | (r5_mask << (4 * j + 4))
        j = j + 1
      }
      result((3 - i) * 5 + 4) = (z & 0xFF).toByte
      result((3 - i) * 5 + 3) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 5 + 2) = ((z >> 16) & 0xFF).toByte
      result((3 - i) * 5 + 1) = ((z >> 24) & 0xFF).toByte
      result((3 - i) * 5) = ((z >> 32) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  private def interleave6Ints(
      i1: Int,
      i2: Int,
      i3: Int,
      i4: Int,
      i5: Int,
      i6: Int): Array[Byte] = {
    val result = new Array[Byte](24)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte
      val tmp4 = ((i4 >> (i * 8)) & 0xFF).toByte
      val tmp5 = ((i5 >> (i * 8)) & 0xFF).toByte
      val tmp6 = ((i6 >> (i * 8)) & 0xFF).toByte

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        z |= (r1_mask << (5 * j)) | (r2_mask << (5 * j + 1)) | (r3_mask << (5 * j + 2)) |
          (r4_mask << (5 * j + 3)) | (r5_mask << (5 * j + 4)) | (r6_mask << (5 * j + 5))
        j = j + 1
      }
      result((3 - i) * 6 + 5) = (z & 0xFF).toByte
      result((3 - i) * 6 + 4) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 6 + 3) = ((z >> 16) & 0xFF).toByte
      result((3 - i) * 6 + 2) = ((z >> 24) & 0xFF).toByte
      result((3 - i) * 6 + 1) = ((z >> 32) & 0xFF).toByte
      result((3 - i) * 6) = ((z >> 40) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  private def interleave7Ints(
      i1: Int,
      i2: Int,
      i3: Int,
      i4: Int,
      i5: Int,
      i6: Int,
      i7: Int): Array[Byte] = {
    val result = new Array[Byte](28)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte
      val tmp4 = ((i4 >> (i * 8)) & 0xFF).toByte
      val tmp5 = ((i5 >> (i * 8)) & 0xFF).toByte
      val tmp6 = ((i6 >> (i * 8)) & 0xFF).toByte
      val tmp7 = ((i7 >> (i * 8)) & 0xFF).toByte

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        val r7_mask = tmp7 & (1 << j)
        z |= (r1_mask << (6 * j)) | (r2_mask << (6 * j + 1)) | (r3_mask << (6 * j + 2)) |
          (r4_mask << (6 * j + 3)) | (r5_mask << (6 * j + 4)) | (r6_mask << (6 * j + 5)) |
          (r7_mask << (6 * j + 6))
        j = j + 1
      }
      result((3 - i) * 7 + 6) = (z & 0xFF).toByte
      result((3 - i) * 7 + 5) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 7 + 4) = ((z >> 16) & 0xFF).toByte
      result((3 - i) * 7 + 3) = ((z >> 24) & 0xFF).toByte
      result((3 - i) * 7 + 2) = ((z >> 32) & 0xFF).toByte
      result((3 - i) * 7 + 1) = ((z >> 40) & 0xFF).toByte
      result((3 - i) * 7) = ((z >> 48) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  private def interleave8Ints(
      i1: Int,
      i2: Int,
      i3: Int,
      i4: Int,
      i5: Int,
      i6: Int,
      i7: Int,
      i8: Int): Array[Byte] = {
    val result = new Array[Byte](32)
    var i = 0
    while (i < 4) {
      val tmp1 = ((i1 >> (i * 8)) & 0xFF).toByte
      val tmp2 = ((i2 >> (i * 8)) & 0xFF).toByte
      val tmp3 = ((i3 >> (i * 8)) & 0xFF).toByte
      val tmp4 = ((i4 >> (i * 8)) & 0xFF).toByte
      val tmp5 = ((i5 >> (i * 8)) & 0xFF).toByte
      val tmp6 = ((i6 >> (i * 8)) & 0xFF).toByte
      val tmp7 = ((i7 >> (i * 8)) & 0xFF).toByte
      val tmp8 = ((i8 >> (i * 8)) & 0xFF).toByte

      var z = 0L
      var j = 0
      while (j < 8) {
        val r1_mask = tmp1 & (1 << j)
        val r2_mask = tmp2 & (1 << j)
        val r3_mask = tmp3 & (1 << j)
        val r4_mask = tmp4 & (1 << j)
        val r5_mask = tmp5 & (1 << j)
        val r6_mask = tmp6 & (1 << j)
        val r7_mask = tmp7 & (1 << j)
        val r8_mask = tmp8 & (1 << j)
        z |= (r1_mask << (7 * j)) | (r2_mask << (7 * j + 1)) | (r3_mask << (7 * j + 2)) |
          (r4_mask << (7 * j + 3)) | (r5_mask << (7 * j + 4)) | (r6_mask << (7 * j + 5)) |
          (r7_mask << (7 * j + 6)) | (r8_mask << (7 * j + 7))
        j = j + 1
      }
      result((3 - i) * 8 + 7) = (z & 0xFF).toByte
      result((3 - i) * 8 + 6) = ((z >> 8) & 0xFF).toByte
      result((3 - i) * 8 + 5) = ((z >> 16) & 0xFF).toByte
      result((3 - i) * 8 + 4) = ((z >> 24) & 0xFF).toByte
      result((3 - i) * 8 + 3) = ((z >> 32) & 0xFF).toByte
      result((3 - i) * 8 + 2) = ((z >> 40) & 0xFF).toByte
      result((3 - i) * 8 + 1) = ((z >> 48) & 0xFF).toByte
      result((3 - i) * 8) = ((z >> 56) & 0xFF).toByte
      i = i + 1
    }
    result
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): InterleaveBits = copy(children = newChildren)
}
