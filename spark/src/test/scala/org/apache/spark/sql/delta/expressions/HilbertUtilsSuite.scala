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

import java.util

import org.apache.spark.sql.delta.expressions.HilbertUtils.HilbertMatrix

import org.apache.spark.SparkFunSuite

class HilbertUtilsSuite extends SparkFunSuite {

  test("circularLeftShift") {
    assert(
      (0 until (1 << 10) by 7).forall(i => HilbertUtils.circularLeftShift(10, i, 0) == i),
      "Shift by 0 should be a no op"
    )
    assert(
      (0 until (1 << 10) by 7).forall(i => HilbertUtils.circularLeftShift(10, i, 10) == i),
      "Shift by n should be a no op"
    )
    // 0111 (<< 2) => 1101
    assert(
      HilbertUtils.circularLeftShift(4, 7, 2) == 13,
      "handle wrapping"
    )
    assert(
      (0 until (1 << 5)).forall(HilbertUtils.circularLeftShift(5, _, 5) <= (1 << 5)),
      "always mask values based on n"
    )
  }

  test("circularRightShift") {
    assert(
      (0 until (1 << 10) by 7).forall(i => HilbertUtils.circularRightShift(10, i, 0) == i),
      "Shift by 0 should be a no op"
    )
    assert(
      (0 until (1 << 10) by 7).forall(i => HilbertUtils.circularRightShift(10, i, 10) == i),
      "Shift by n should be a no op"
    )
    // 0111 (>> 2) => 1101
    assert(
      HilbertUtils.circularRightShift(4, 7, 2) == 13,
      "handle wrapping"
    )
    assert(
      (0 until (1 << 5)).forall(HilbertUtils.circularRightShift(5, _, 5) <= (1 << 5)),
      "always mask values based on n"
    )
  }

  test("getSetColumn should return the column that is set") {
    (0 until 16) foreach { i =>
      assert(HilbertUtils.getSetColumn(16, 1 << i) ==  16 - 1 - i)
    }
  }

  test("HilbertMatrix makes sense") {
    val identityMatrix = HilbertMatrix.identity(10)
    (0 until (1 << 10) by 7) foreach { i =>
      assert(identityMatrix.transform(i) == i, s"$i transformed by the identity should be $i")
    }

    identityMatrix.multiply(HilbertMatrix.identity(10)) == identityMatrix

    val shift5 = HilbertMatrix(10, 0, 5)
    assert(shift5.multiply(shift5) == identityMatrix, "shift by 5 twice should equal identity")
  }

  test("HilbertUtils.getBits") {
    assert(HilbertUtils.getBits(Array(0, 0, 1), 22, 2) == 1)
    val array = Array[Byte](0, 0, -1, 0)
    assert(HilbertUtils.getBits(array, 16, 4) == 15)
    assert(HilbertUtils.getBits(array, 18, 3) == 7)
    assert(HilbertUtils.getBits(array, 23, 1) == 1)
    assert(HilbertUtils.getBits(array, 23, 2) == 2)
    assert(HilbertUtils.getBits(array, 23, 8) == 128)
    assert(HilbertUtils.getBits(array, 16, 3) == 7)
    assert(HilbertUtils.getBits(array, 16, 2) == 3)
    assert(HilbertUtils.getBits(array, 16, 1) == 1)
    assert(HilbertUtils.getBits(array, 15, 2) == 1)
    assert(HilbertUtils.getBits(array, 15, 1) == 0)
    assert(HilbertUtils.getBits(array, 12, 8) == 15)
    assert(HilbertUtils.getBits(array, 12, 12) == 255)
    assert(HilbertUtils.getBits(array, 12, 13) == (255 << 1))

    assert(HilbertUtils.getBits(Array(0, 1, 0), 6, 6) == 0)
    assert(HilbertUtils.getBits(Array(0, 1, 0), 12, 6) == 4)
    assert(HilbertUtils.getBits(Array(0, 1, 0), 18, 6) == 0)
  }

  def check(received: Array[Byte], expected: Array[Byte]): Unit = {
    assert(util.Arrays.equals(expected, received),
      s"${expected.toSeq.map(_.toBinaryString.takeRight(8))} " +
      s"${received.toSeq.map(_.toBinaryString.takeRight(8))}")
  }

  test("HilbertUtils.setBits") {
    check(HilbertUtils.setBits(Array(0, 0, 0), 7, 8, 4), Array(1, 0, 0))
    check(HilbertUtils.setBits(Array(0, 0, 0), 7, 12, 4), Array(1, (1.toByte << 7).toByte, 0))
    check(HilbertUtils.setBits(Array(8, 0, 5), 7, 12, 4), Array(9, (1.toByte << 7).toByte, 5))
    check(HilbertUtils.setBits(Array(8, 0, 2), 7, -1, 12),
      Array(9, -1, ((7.toByte << 5).toByte | 2).toByte))
    check(HilbertUtils.setBits(Array(8, 14, 2), 15, 1, 1), Array(8, 15, 2))
  }

  test("addOne") {
    check(HilbertUtils.addOne(Array(0, 0, 0)), Array(0, 0, 1))
    check(HilbertUtils.addOne(Array(0, 0, -1)), Array(0, 1, 0))
    check(HilbertUtils.addOne(Array(0, 0, -2)), Array(0, 0, -1))
    check(HilbertUtils.addOne(Array(0, -1, -1)), Array(1, 0, 0))
    check(HilbertUtils.addOne(Array(-1, -1, -1)), Array(0, 0, 0))
  }
}
