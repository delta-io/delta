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

package org.apache.spark.sql.delta.util

import scala.collection.generic.Sizing


import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

case class IntArrayImplementingSizing(array: Seq[Int]) extends Sizing {
  override def size: Int = array.size
}

case class TestArbitrarySizing(size: Int) extends Sizing

class BinPackingIteratorSuite extends QueryTest
  with SharedSparkSession {

  test("Bin packing works") {
    val targetSize = 4
    val testInput = Seq(
      IntArrayImplementingSizing(Seq(1, 2, 3)),
      IntArrayImplementingSizing(Seq(1, 2, 3)))

    val binPackingIterator = new BinPackingIterator(testInput.iterator, targetSize)

    assert(binPackingIterator.hasNext)
    var count = 0
    for (bin <- binPackingIterator) {
      assert(bin.size === 1)
      assert(bin.toSeq === Seq(IntArrayImplementingSizing(Seq(1, 2, 3))))
      count += 1
    }
    assert(count === 2)
  }

  test("Bin packing can handle overflows to internal size tracking") {
    val targetSize = Int.MaxValue / 2
    val testInput = Seq(
      // 1st bin
      TestArbitrarySizing(1),
      TestArbitrarySizing(1),
      TestArbitrarySizing(2),
      // 2nd bin
      TestArbitrarySizing(Int.MaxValue - 14),
      // 3rd bin
      TestArbitrarySizing(Int.MaxValue / 2),
      // 4th bin
      TestArbitrarySizing(Int.MaxValue / 2),
    )
    val binPackingIterator = new BinPackingIterator(testInput.iterator, targetSize)
    assert(binPackingIterator.hasNext)
    val firstBin = binPackingIterator.next()
    assert(firstBin.size === 3)
    assert(firstBin.toSeq === Seq(
      TestArbitrarySizing(1),
      TestArbitrarySizing(1),
      TestArbitrarySizing(2)))
    assert(binPackingIterator.hasNext)
    val secondBin = binPackingIterator.next()
    assert(secondBin.size === 1)
    assert(secondBin.toSeq === Seq(TestArbitrarySizing(Int.MaxValue - 14)))
    assert(binPackingIterator.hasNext)
    val thirdBin = binPackingIterator.next()
    assert(thirdBin.size === 1)
    assert(thirdBin.toSeq === Seq(TestArbitrarySizing(Int.MaxValue / 2)))
    assert(binPackingIterator.hasNext)
    val fourthBin = binPackingIterator.next()
    assert(fourthBin.size === 1)
    assert(fourthBin.toSeq === Seq(TestArbitrarySizing(Int.MaxValue / 2)))
    assert(!binPackingIterator.hasNext)
  }
}
