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

import java.util.Arrays

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
case class FileSizeHistogram(
    sortedBinBoundaries: IndexedSeq[Long],
    fileCounts: Array[Long],
    totalBytes: Array[Long]) extends FileStatsHistogram {

  /**
   * Not intended to be used for [[Map]] structure keys. Implemented for the sole purpose of having
   * an equals method, which requires overriding hashCode as well, so an incomplete hash is okay.
   * We only require a == b implies a.hashCode == b.hashCode
   */
  override def hashCode(): Int = Arrays.hashCode(totalBytes)

  override def equals(that: Any): Boolean = that match {
    case h: FileSizeHistogram => equalsHistogram(h)
    case _ => false
  }

  /**
   * Insert a given value into the appropriate histogram bin
   */
  def insert(fileSize: Long): Unit = {
    val index = FileSizeHistogram.getBinIndex(fileSize, sortedBinBoundaries)
    if (index >= 0) {
      fileCounts(index) += 1
      totalBytes(index) += fileSize
    }
  }

  /**
   * Remove a given value from the appropriate histogram bin
   * @param fileSize to remove
   */
  def remove(fileSize: Long): Unit = {
    val index = FileSizeHistogram.getBinIndex(fileSize, sortedBinBoundaries)
    if (index >= 0) {
      fileCounts(index) -= 1
      totalBytes(index) -= fileSize
    }
  }

  /**
   * Number of files whose size is strictly less than `threshold`.
   *
   * A bin contributes fully when its upper boundary is `<= threshold`, i.e. every file in the bin
   * is guaranteed to be smaller than `threshold`. A bin whose lower boundary is below `threshold`
   * but whose upper boundary exceeds it is conservatively excluded, because individual file sizes
   * within the bin are unknown and may straddle the threshold. The last bin's upper boundary is
   * `Long.MaxValue`.
   */
  def smallFileCount(threshold: Long): Long = {
    var total = 0L
    var i = 0
    val n = sortedBinBoundaries.length
    while (i < n) {
      val binUpperBoundary = if (i + 1 < n) sortedBinBoundaries(i + 1) else Long.MaxValue
      if (binUpperBoundary <= threshold) {
        total += fileCounts(i)
      }
      i += 1
    }
    total
  }
}

private[delta] object FileSizeHistogram {

  /**
   * Returns the index of the bin to which given fileSize belongs OR -1 if given fileSize doesn't
   * belongs to any bin
   */
  def getBinIndex(fileSize: Long, sortedBinBoundaries: IndexedSeq[Long]): Int = {
    FileStatsHistogram.getBinIndex(fileSize, sortedBinBoundaries)
  }

  def apply(sortedBinBoundaries: IndexedSeq[Long]): FileSizeHistogram = {
    new FileSizeHistogram(
      sortedBinBoundaries,
      Array.fill(sortedBinBoundaries.size)(0),
      Array.fill(sortedBinBoundaries.size)(0)
    )
  }

  lazy val schema: StructType = ExpressionEncoder[FileSizeHistogram]().schema
}
