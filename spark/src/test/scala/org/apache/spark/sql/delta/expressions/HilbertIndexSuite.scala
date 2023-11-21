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

import org.scalatest.Tag
import org.apache.spark.SparkFunSuite

class HilbertIndexSuite extends SparkFunSuite {

  /**
   * Represents a test case. Each n-k pair will verify the continuity of the mapping,
   * and the reversibility of it.
   * @param n The number of dimensions
   * @param k The number of bits in each dimension
   */
  case class TestCase(n: Int, k: Int)
  val testCases = Seq(
    TestCase(2, 10),
    TestCase(3, 6),
    TestCase(4, 5),
    TestCase(5, 4),
    TestCase(6, 3)
  )

  def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  gridTest("HilbertStates caches states")(2 to 9) { n =>
    val start = System.nanoTime()
    HilbertStates.getStateList(n)
    val end = System.nanoTime()

    HilbertStates.getStateList(n)
    val end2 = System.nanoTime()
    assert(end2 - end < end - start)
  }

  gridTest("Hilbert Mapping is continuous (long keys)")(testCases) { case TestCase(n, k) =>
    val generator = HilbertIndex.getStateGenerator(n)

    val stateList = generator.generateStateList()

    val states = stateList.getDKeyToNPointStateMap

    val maxDKeys = 1L << (k * n)
    var d = 0
    var lastPoint = new Array[Int](n)
    while (d < maxDKeys) {
      val point = states.translateDKeyToNPoint(d, k)
      if (d != 0) {
        assert(HilbertUtils.manhattanDist(lastPoint, point) == 1)
      }

      lastPoint = point
      d += 1
    }

  }

  gridTest("Hilbert Mapping is 1 to 1 (long keys)")(testCases) { case TestCase(n, k) =>
    val generator = HilbertIndex.getStateGenerator(n)
    val stateList = generator.generateStateList()

    val d2p = stateList.getDKeyToNPointStateMap
    val p2d = stateList.getNPointToDKeyStateMap

    val maxDKeys = 1L << (k * n)
    var d = 0
    while (d < maxDKeys) {
      val point = d2p.translateDKeyToNPoint(d, k)
      val d2 = p2d.translateNPointToDKey(point, k)
      assert(d == d2)
      d += 1
    }
  }

  gridTest("Hilbert Mapping is continuous (array keys)")(testCases) { case TestCase(n, k) =>
    val generator = HilbertIndex.getStateGenerator(n)

    val stateList = generator.generateStateList()

    val states = stateList.getDKeyToNPointStateMap

    val maxDKeys = 1L << (k * n)
    val d = new Array[Byte](((k * n) / 8) + 1)
    var lastPoint = new Array[Int](n)
    var i = 0
    while (i < maxDKeys) {
      val point = states.translateDKeyArrayToNPoint(d, k)
      if (i != 0) {
        assert(HilbertUtils.manhattanDist(lastPoint, point) == 1,
          s"$i ${d.toSeq.map(_.toBinaryString.takeRight(8))} ${lastPoint.toSeq} to ${point.toSeq}")
      }

      lastPoint = point
      i += 1
      HilbertUtils.addOne(d)
    }

  }

  gridTest("Hilbert Mapping is 1 to 1 (array keys)")(testCases) { case TestCase(n, k) =>
    val generator = HilbertIndex.getStateGenerator(n)
    val stateList = generator.generateStateList()

    val d2p = stateList.getDKeyToNPointStateMap
    val p2d = stateList.getNPointToDKeyStateMap

    val maxDKeys = 1L << (k * n)
    val d = new Array[Byte](((k * n) / 8) + 1)
    var i = 0
    while (i < maxDKeys) {
      val point = d2p.translateDKeyArrayToNPoint(d, k)
      val d2 = p2d.translateNPointToDKeyArray(point, k)
      assert(util.Arrays.equals(d, d2), s"$i ${d.toSeq}, ${d2.toSeq}")
      i += 1
      HilbertUtils.addOne(d)
    }
  }

  gridTest("continuous and 1 to 1 for all spaces")((2 to 9).map(n => TestCase(n, 15 - n))) {
      case TestCase(n, k) =>
    val generator = HilbertIndex.getStateGenerator(n)
    val stateList = generator.generateStateList()

    val d2p = stateList.getDKeyToNPointStateMap
    val p2d = stateList.getNPointToDKeyStateMap

    val numBits = k * n
    val numBytes = (numBits + 7) / 8

    // test 1000 contiguous 1000 point blocks to make sure the mapping is continuous and one to one

    val maxDKeys = 1L << (k * n)
    val step = maxDKeys / 1000
    var x = 0L
    for (_ <- 0 until 1000) {
      var dLong = x
      val bigIntArray = BigInt(dLong).toByteArray
      val dArray = new Array[Byte](numBytes)

      System.arraycopy(
        bigIntArray,
        math.max(0, bigIntArray.length - dArray.length),
        dArray,
        math.max(0, dArray.length - bigIntArray.length),
        math.min(bigIntArray.length, dArray.length)
      )

      var lastPoint: Array[Int] = null

      for (_ <- 0 until 1000) {
        val pArray = d2p.translateDKeyArrayToNPoint(dArray, k)
        val pLong = d2p.translateDKeyToNPoint(dLong, k)
        assert(util.Arrays.equals(pArray, pLong), s"points should be the same at $dLong")

        if (lastPoint != null) {
          assert(HilbertUtils.manhattanDist(lastPoint, pLong) == 1,
            s"distance between point and last point should be the same at $dLong")
        }

        val dArray2 = p2d.translateNPointToDKeyArray(pArray, k)
        val dLong2 = p2d.translateNPointToDKey(pLong, k)

        assert(dLong == dLong2, s"reversing the points should map correctly at $dLong != $dLong2")

        assert(util.Arrays.equals(dArray, dArray2),
          s"reversing the points should map correctly at $dLong")

        lastPoint = pLong

        dLong += 1
        HilbertUtils.addOne(dArray)
      }

      x += step
    }

  }
}
