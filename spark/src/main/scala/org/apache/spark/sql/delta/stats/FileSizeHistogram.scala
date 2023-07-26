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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

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
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    sortedBinBoundaries: IndexedSeq[Long],
    fileCounts: Array[Long],
    totalBytes: Array[Long]) {

  require(sortedBinBoundaries.nonEmpty)
  require(sortedBinBoundaries.head == 0, "The first bin should start from 0")
  require(sortedBinBoundaries.length == fileCounts.length, "number of binBoundaries should be" +
    " same as size of fileCounts")
  require(sortedBinBoundaries.length == totalBytes.length, "number of binBoundaries should be" +
    " same as size of totalBytes")

  /**
   * Not intended to be used for [[Map]] structure keys. Implemented for the sole purpose of having
   * an equals method, which requires overriding hashCode as well, so an incomplete hash is okay.
   * We only require a == b implies a.hashCode == b.hashCode
   */
  override def hashCode(): Int = Arrays.hashCode(totalBytes)

  override def equals(that: Any): Boolean = that match {
    case FileSizeHistogram(thatSB, thatFC, thatTB) =>
      sortedBinBoundaries == thatSB &&
      java.util.Arrays.equals(fileCounts, thatFC) &&
      java.util.Arrays.equals(totalBytes, thatTB)
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
}

private[delta] object FileSizeHistogram {

  /**
   * Returns the index of the bin to which given fileSize belongs OR -1 if given fileSize doesn't
   * belongs to any bin
   */
  def getBinIndex(fileSize: Long, sortedBinBoundaries: IndexedSeq[Long]): Int = {
    import scala.collection.Searching._
    // The search function on IndexedSeq uses binary search.
    val searchResult = sortedBinBoundaries.search(fileSize)
    searchResult match {
      case Found(index) =>
        index
      case InsertionPoint(insertionPoint) =>
        // insertionPoint=0 means that fileSize is lesser than min bucket of histogram
        // return -1 in that case
        insertionPoint - 1
    }
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
