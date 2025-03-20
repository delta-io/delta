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

import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.toJavaList

import org.scalatest.funsuite.AnyFunSuite

class FileSizeHistogramSuite extends AnyFunSuite {
  implicit class HistogramOps(histogram: FileSizeHistogram) {
    def getSortedBinBoundaries: List[Long] = {
      toJavaList(histogram.toRow().getArray(
        FileSizeHistogram.FULL_SCHEMA.indexOf("sortedBinBoundaries"))).asScala.toList
    }

    def getFileCounts: List[Long] = {
      toJavaList(histogram.toRow().getArray(
        FileSizeHistogram.FULL_SCHEMA.indexOf("fileCounts"))).asScala.toList
    }

    def getTotalBytes: List[Long] = {
      toJavaList(histogram.toRow().getArray(
        FileSizeHistogram.FULL_SCHEMA.indexOf("totalBytes"))).asScala.toList
    }
  }

  private val KB = 1024L
  private val MB = KB * 1024
  private val GB = MB * 1024

  test("createDefaultHistogram should create histogram with zero counts and bytes") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    assert(histogram.getFileCounts.forall(_ == 0L))
    assert(histogram.getTotalBytes.forall(_ == 0L))
    assert(histogram.getSortedBinBoundaries == List(
      0L,
      // Power of 2 till 4 MB
      8 * KB,
      16 * KB,
      32 * KB,
      64 * KB,
      128 * KB,
      256 * KB,
      512 * KB,
      1 * MB,
      2 * MB,
      4 * MB,
      // 4 MB jumps till 40 MB
      8 * MB,
      12 * MB,
      16 * MB,
      20 * MB,
      24 * MB,
      28 * MB,
      32 * MB,
      36 * MB,
      40 * MB,
      // 8 MB jumps till 120 MB
      48 * MB,
      56 * MB,
      64 * MB,
      72 * MB,
      80 * MB,
      88 * MB,
      96 * MB,
      104 * MB,
      112 * MB,
      120 * MB,
      // 4 MB jumps till 144 MB
      124 * MB,
      128 * MB,
      132 * MB,
      136 * MB,
      140 * MB,
      144 * MB,
      // 16 MB jumps till 576 MB
      160 * MB,
      176 * MB,
      192 * MB,
      208 * MB,
      224 * MB,
      240 * MB,
      256 * MB,
      272 * MB,
      288 * MB,
      304 * MB,
      320 * MB,
      336 * MB,
      352 * MB,
      368 * MB,
      384 * MB,
      400 * MB,
      416 * MB,
      432 * MB,
      448 * MB,
      464 * MB,
      480 * MB,
      496 * MB,
      512 * MB,
      528 * MB,
      544 * MB,
      560 * MB,
      576 * MB,
      // 64 MB jumps till 1408 MB
      640 * MB,
      704 * MB,
      768 * MB,
      832 * MB,
      896 * MB,
      960 * MB,
      1024 * MB,
      1088 * MB,
      1152 * MB,
      1216 * MB,
      1280 * MB,
      1344 * MB,
      1408 * MB,
      // 128 MB jumps till 2 GB
      1536 * MB,
      1664 * MB,
      1792 * MB,
      1920 * MB,
      2048 * MB,
      // 256 MB jumps till 4 GB
      2304 * MB,
      2560 * MB,
      2816 * MB,
      3072 * MB,
      3328 * MB,
      3584 * MB,
      3840 * MB,
      4 * GB,
      // power of 2 till 256 GB
      8 * GB,
      16 * GB,
      32 * GB,
      64 * GB,
      128 * GB,
      256 * GB))
  }

  test("basic insert") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    val testSizes = List(
      512L, // Small file
      8L * KB, // 8KB
      1L * MB, // 1MB
      128L * MB, // 128MB
      1L * GB, // 1GB
      10L * GB // 10GB
    )

    // Test single insertions with different bins
    testSizes.foreach { size =>
      histogram.insert(size)
      val index = getBinIndexForTesting(histogram.getSortedBinBoundaries, size)
      assert(histogram.getFileCounts(index) >= 1L)
      assert(histogram.getTotalBytes(index) >= size)
    }

    // Test multiple insertions in same bin
    val sizeForMultiple = 1L * MB
    val numFiles = 5
    val index = getBinIndexForTesting(histogram.getSortedBinBoundaries, sizeForMultiple)
    val initialCount = histogram.getFileCounts(index)
    val initialBytes = histogram.getTotalBytes(index)

    (1 to numFiles).foreach { i =>
      histogram.insert(sizeForMultiple)
      assert(histogram.getFileCounts(index) == initialCount + i)
      assert(histogram.getTotalBytes(index) == initialBytes + (i * sizeForMultiple))
    }

    // Test negative file size
    intercept[IllegalArgumentException] {
      histogram.insert(-1)
    }
  }

  test("insert the boundary") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    val boundaries = histogram.getSortedBinBoundaries
    boundaries.foreach { boundary =>
      histogram.insert(boundary)
      val index = getBinIndexForTesting(boundaries, boundary)
      assert(histogram.getFileCounts(index) == 1)
    }
  }

  test("insert very large file") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    val veryLargeSize = 256L * GB
    histogram.insert(veryLargeSize)
    val lastIndex = histogram.getSortedBinBoundaries.size - 1

    assert(histogram.getFileCounts(lastIndex) == 1)
  }

  test("remove should handle file sizes correctly") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    val fileSize = 1L * MB
    val index = getBinIndexForTesting(histogram.getSortedBinBoundaries, fileSize)

    // Test multiple inserts and removes
    val numFiles = 3
    (1 to numFiles).foreach(_ => histogram.insert(fileSize))

    (1 to numFiles).foreach { i =>
      histogram.remove(fileSize)
      val remainingFiles = numFiles - i
      assert(histogram.getFileCounts(index) == remainingFiles)
      assert(histogram.getTotalBytes(index) == fileSize * remainingFiles)
    }

    assert(histogram.getTotalBytes(
      getBinIndexForTesting(histogram.getSortedBinBoundaries, fileSize)) === 0)
    // Test error cases
    intercept[IllegalArgumentException] {
      histogram.remove(fileSize) // Try to remove from empty bin
    }

    histogram.insert(fileSize)
    assert(
      getBinIndexForTesting(histogram.getSortedBinBoundaries, fileSize)
        === getBinIndexForTesting(histogram.getSortedBinBoundaries, fileSize + 1))
    intercept[IllegalArgumentException] {
      histogram.remove(fileSize + 1) // Try to remove more bytes than available
    }

    intercept[IllegalArgumentException] {
      histogram.remove(-1) // Try to remove negative size
    }
  }

  test("histogram should be identical after serialization round trip") {
    val histogram = FileSizeHistogram.createDefaultHistogram()
    List(
      (512L, 5), // 5 files of 512B
      (1 * MB, 3), // 3 files of 1MB
      (10 * MB, 2) // 2 files of 10MB
    ).foreach { case (size, count) =>
      (1 to count).foreach(_ => histogram.insert(size))
    }

    val reconstructedHistogram = FileSizeHistogram.fromColumnVector(
      VectorUtils.buildColumnVector(
        Seq(histogram.toRow).toList.asJava,
        FileSizeHistogram.FULL_SCHEMA),
      0)

    assert(reconstructedHistogram.isPresent)
    assert(histogram === reconstructedHistogram.get())
  }

  /**
   * Determines the bin index for a given file size using binary search.
   * Returns the index of the largest bin boundary that is less than or equal to the file size.
   */
  private def getBinIndexForTesting(boundaries: List[Long], fileSize: Long): Int = {
    import scala.collection.Searching.{Found, InsertionPoint}
    boundaries.search(fileSize) match {
      case Found(index) => index
      case InsertionPoint(index) => index - 1
    }
  }
}
