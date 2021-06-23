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

package org.apache.spark.sql.delta.stats

import java.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

/**
 * A Histogram class tracking the file counts and total bytes in different size ranges
 * @param sortedBinBoundaries - a sorted list of bin boundaries where each element represents the
 *                              start of the bin (included) and the next element represents the end
 *                              of the bin (excluded)
 * @param fileCounts - an array of Int representing total number of files in different bins
 * @param totalBytes - an array of Long representing total number of bytes in different bins
 */
private[delta] case class FileSizeHistogram(
    sortedBinBoundaries: Array[Long],
    fileCounts: Array[Int],
    totalBytes: Array[Long]) {

  require(sortedBinBoundaries.nonEmpty)
  require(sortedBinBoundaries.head == 0, "The first bin should start from 0")
  require(sortedBinBoundaries.length == fileCounts.length, "number of binBoundaries should be" +
    " same as size of fileCounts")
  require(sortedBinBoundaries.length == totalBytes.length, "number of binBoundaries should be" +
    " same as size of totalBytes")

  /**
   * Insert a given value into the appropriate histogram bin
   */
  def insert(fileSize: Long): Unit = {
    var index = util.Arrays.binarySearch(sortedBinBoundaries, fileSize)
    // If the element is present in the array, it returns a non-negative result and that
    // represents the bucket in which fileSize belongs.
    // If the element is not present in the array, then it returns (-1 * insertion_point) - 1
    // where insertion_point is the index of the first element greater than the key.
    if (index < 0) {
      index = ((index + 1) * -1) - 1
    }

    // If fileSize is lesser than min bucket of histogram, then no need to update the histogram.
    if (index >= 0) {
      fileCounts(index) += 1
      totalBytes(index) += fileSize
    }
  }
}

private[delta] object FileSizeHistogram {

  def apply(sortedBinBoundaries: Seq[Long]): FileSizeHistogram = {
    new FileSizeHistogram(
      sortedBinBoundaries.toArray,
      Array.fill(sortedBinBoundaries.size)(0),
      Array.fill(sortedBinBoundaries.size)(0)
    )
  }

  val schema: StructType = ExpressionEncoder[FileSizeHistogram]().schema
}
