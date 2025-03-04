/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.stats

import scala.collection.JavaConverters._
import scala.collection.Searching.search

import org.scalatest.funsuite.AnyFunSuite

class FileSizeHistogramSuite extends AnyFunSuite {
  implicit class FileSizeHistogramOps(histogram: FileSizeHistogram) {
    def getBoundaries: List[Long] = histogram.sortedBinBoundaries.asScala.map(_.longValue()).toList
    def getCounts: Array[Long] = histogram.fileCounts.map(_.longValue)
    def getBytes: Array[Long] = histogram.totalBytes.map(_.longValue)
  }

  test("init should create histogram with zero counts and bytes") {
    val histogram = FileSizeHistogram.init()
    assert(histogram.getCounts.forall(_ == 0L))
    assert(histogram.getBytes.forall(_ == 0L))
  }

  test("insert should handle file sizes correctly") {
    val histogram = FileSizeHistogram.init()
    val testSizes = List(
      512L, // Small file
      8L * 1024, // 8KB
      1024L * 1024, // 1MB
      128L * 1024 * 1024 // 128MB
    )

    // Test single insertions with different bins
    testSizes.foreach { size =>
      histogram.insert(size)
      val index = getBinIndexForTesting(histogram.getBoundaries, size)
      assert(histogram.getCounts(index) == 1L)
      assert(histogram.getBytes(index) == size)
    }

    // Test multiple insertions in same bin
    val sizeForMultiple = 1024L * 1024 // 1MB
    val numFiles = 5
    val index = getBinIndexForTesting(histogram.getBoundaries, sizeForMultiple)
    val initialCount = histogram.getCounts(index)
    val initialBytes = histogram.getBytes(index)

    (1 to numFiles).foreach(i => {
      histogram.insert(sizeForMultiple)
      assert(histogram.getCounts(index) == i + initialCount)
      assert(histogram.getBytes(index) == i * sizeForMultiple + initialBytes)
    })

    intercept[IllegalArgumentException] {
      histogram.insert(-1)
    }
  }

  test("remove should handle file sizes correctly") {
    val histogram = FileSizeHistogram.init()
    val fileSize = 1024L * 1024 // 1MB
    val index = getBinIndexForTesting(histogram.getBoundaries, fileSize)

    // Test multiple inserts and removes
    val numFiles = 3
    (1 to numFiles).foreach { _ =>
      histogram.insert(fileSize)
    }

    (1 to numFiles).foreach { i =>
      histogram.remove(fileSize)
      val remainingFiles = numFiles - i
      assert(histogram.getCounts(index) == remainingFiles)
      assert(histogram.getBytes(index) == fileSize * remainingFiles)
    }

    // Test error cases
    intercept[IllegalArgumentException] {
      histogram.remove(fileSize) // Try to remove from empty bin
    }

    histogram.insert(fileSize)
    intercept[IllegalArgumentException] {
      histogram.remove(fileSize * 2) // Try to remove more bytes than available
    }

    intercept[IllegalArgumentException] {
      histogram.remove(-1)
    }
  }

  private def getBinIndexForTesting(boundaries: List[Long], fileSize: Long): Int = {
    import scala.collection.Searching.{Found, InsertionPoint}
    boundaries.search(fileSize) match {
      case Found(index) => index
      case InsertionPoint(index) => index - 1
    }
  }
}
