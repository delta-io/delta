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

object HilbertUtils {

  /**
   * Returns the column number that is set. We assume that a bit is set.
   */
  @inline def getSetColumn(n: Int, i: Int): Int = {
    n - 1 - Integer.numberOfTrailingZeros(i)
  }

  @inline def circularLeftShift(n: Int, i: Int, shift: Int): Int = {
    ((i << shift) | (i >>> (n - shift))) & ((1 << n) - 1)
  }

  @inline def circularRightShift(n: Int, i: Int, shift: Int): Int = {
    ((i >>> shift) | (i << (n - shift))) & ((1 << n) - 1)
  }

  @inline
  private[expressions] def getBits(key: Array[Byte], offset: Int, n: Int): Int = {
    // [    ][    ][    ][    ][    ]
    // <---offset---> [  n-bits  ]      <- this is the result
    var result = 0

    var remainingBits = n
    var keyIndex = offset / 8
    // initial key offset
    var keyOffset = offset - (keyIndex * 8)
    while (remainingBits > 0) {
      val bitsFromIdx = math.min(remainingBits, 8 - keyOffset)
      val newInt = if (remainingBits >= 8) {
        java.lang.Byte.toUnsignedInt(key(keyIndex))
      } else {
        java.lang.Byte.toUnsignedInt(key(keyIndex)) >>> (8 - keyOffset - bitsFromIdx)
      }
      result = (result << bitsFromIdx) | (newInt & ((1 << bitsFromIdx) - 1))

      remainingBits -= (8 - keyOffset)
      keyOffset = 0
      keyIndex += 1
    }

    result
  }

  @inline
  private[expressions] def setBits(
      key: Array[Byte],
      offset: Int,
      newBits: Int,
      n: Int): Array[Byte] = {
    // bits: [   meaningless bits   ][  n meaningful bits  ]
    //
    // [    ][    ][    ][    ][    ]
    // <---offset---> [  n-bits  ]

    // move meaningful bits to the far left
    var bits = newBits << (32 - n)
    var remainingBits = n

    // initial key index
    var keyIndex = offset / 8
    // initial key offset
    var keyOffset = offset - (keyIndex * 8)
    while (remainingBits > 0) {
      key(keyIndex) = (key(keyIndex) | (bits >>> (24 + keyOffset))).toByte
      remainingBits -= (8 - keyOffset)
      bits = bits << (8 - keyOffset)
      keyOffset = 0
      keyIndex += 1
    }
    key
  }

  /**
   * treats `key` as an Integer and adds 1
   */
  @inline def addOne(key: Array[Byte]): Array[Byte] = {
    var idx = key.length - 1
    var overflow = true
    while (overflow && idx >= 0) {
      key(idx) = (key(idx) + 1.toByte).toByte
      overflow = key(idx) == 0
      idx -= 1
    }
    key
  }

  def manhattanDist(p1: Array[Int], p2: Array[Int]): Int = {
    assert(p1.length == p2.length)
    p1.zip(p2).map { case (a, b) => math.abs(a - b) }.sum
  }


  /**
   * This is not really a matrix, but a representation of one. Due to the constraints of this
   * system the necessary matrices can be defined by two values: dY and X2. DY is the amount
   * of right shifting of the identity matrix, and X2 is a bitmask for which column values are
   * negative. The [[toString]] method is overridden to construct and print the matrix to aid
   * in debugging.
   * Instead of constructing the matrix directly we store and manipulate these values.
   */
  case class HilbertMatrix(n: Int, x2: Int, dy: Int) {
    override def toString(): String = {
      val sb = new StringBuilder()

      val base = 1 << (n - 1 - dy)
      (0 until n).foreach { i =>
        sb.append('\n')
        val row = circularRightShift(n, base, i)
        (0 until n).foreach { j =>
          if (isColumnSet(row, j)) {
            if (isColumnSet(x2, j)) {
              sb.append('-')
            } else {
              sb.append(' ')
            }
            sb.append('1')
          } else {
            sb.append(" 0")
          }
        }
      }
      sb.append('\n')
      sb.toString
    }

    // columns count from the left: 0, 1, 2 ... , n
    @inline def isColumnSet(i: Int, column: Int): Boolean = {
      val mask = 1 << (n - 1 - column)
      (i & mask) > 0
    }

    def transform(e: Int): Int = {
      circularLeftShift(n, e ^ x2, dy)
    }

    def multiply(other: HilbertMatrix): HilbertMatrix = {
      HilbertMatrix(n, circularRightShift(n, x2, other.dy) ^ other.x2, (dy + other.dy) % n)
    }
  }

  object HilbertMatrix {
    def identity(n: Int): HilbertMatrix = {
      HilbertMatrix(n, 0, 0)
    }
  }
}
