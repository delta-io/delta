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

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}
import org.apache.spark.unsafe.Platform

/**
 * Base trait for histogram implementations tracking file counts and total bytes across bins.
 */
trait FileStatsHistogram {
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  def sortedBinBoundaries: IndexedSeq[Long]
  def fileCounts: Array[Long]
  def totalBytes: Array[Long]

  require(sortedBinBoundaries.nonEmpty)
  require(sortedBinBoundaries.head == 0, "The first bin should start from 0")
  require(sortedBinBoundaries.length == fileCounts.length,
    "number of binBoundaries should be same as size of fileCounts")
  require(sortedBinBoundaries.length == totalBytes.length,
    "number of binBoundaries should be same as size of totalBytes")

  /**
   * Helper method for subclass equals implementations. Subclasses should override both
   * equals and hashCode together.
   */
  protected def equalsHistogram(that: FileStatsHistogram): Boolean = {
    sortedBinBoundaries == that.sortedBinBoundaries &&
      java.util.Arrays.equals(fileCounts, that.fileCounts) &&
      java.util.Arrays.equals(totalBytes, that.totalBytes)
  }
}

/**
 * Companion object with utility functions for histograms.
 */
object FileStatsHistogram {

  /**
   * Returns the index of the bin to which given value belongs OR -1 if value doesn't belong
   * to any bin
   */
  def getBinIndex(value: Long, sortedBinBoundaries: IndexedSeq[Long]): Int = {
    import scala.collection.Searching._
    val searchResult = sortedBinBoundaries.search(value)
    searchResult match {
      case Found(index) =>
        index
      case InsertionPoint(insertionPoint) =>
        // insertionPoint=0 means that fileSize is lesser than min bucket of histogram
        insertionPoint - 1
    }
  }

  /**
   * Returns a compacted version of a histogram where empty bins are merged together.
   */
  def compress[H <: FileStatsHistogram](
      h: H,
      constructor: (IndexedSeq[Long], Array[Long], Array[Long]) => H): H = {
    val newSortedBinBoundaries = ArrayBuffer.empty[Long]
    val newFileCounts = ArrayBuffer.empty[Long]
    val newTotalBytes = ArrayBuffer.empty[Long]
    if (h.sortedBinBoundaries.nonEmpty) {
      newSortedBinBoundaries.append(h.sortedBinBoundaries(0))
      newFileCounts.append(h.fileCounts(0))
      newTotalBytes.append(h.totalBytes(0))
      for (index <- 1 until h.sortedBinBoundaries.length) {
        if (h.fileCounts(index) != 0 || h.fileCounts(index - 1) != 0) {
          newSortedBinBoundaries.append(h.sortedBinBoundaries(index))
          newFileCounts.append(h.fileCounts(index))
          newTotalBytes.append(h.totalBytes(index))
        }
      }
    }
    constructor(newSortedBinBoundaries.toIndexedSeq, newFileCounts.toArray, newTotalBytes.toArray)
  }

  /**
   * Base class for imperative aggregate implementations of file statistics histograms.
   * This provides common functionality for both FileSizeHistogram and FileAgeHistogram aggregates.
   *
   * The return type of this Imperative Aggregate is of ArrayType(LongType). The array
   * represents a flattened histogram with following structure:
   *
   * --------------------------------------------------------------------------------
   * |  PART-1: sortedBinBoundaries  |  PART-2: fileCounts   | PART-3: totalBytes   |
   * --------------------------------------------------------------------------------
   *
   * This Aggregate returns the flattened histogram and not the histogram object due to
   * the limitation that Imperative aggregates can only return primitive/Array/Map types.
   *
   * The intermediate aggregation buffer consists of only PART-2 + PART-3 and doesn't contain
   * the sortedBinBoundaries. sortedBinBoundaries are added only at the end when the aggregate
   * is finalized.
   */
  abstract class FileStatsHistogramAggBase
    extends org.apache.spark.sql.catalyst.expressions.aggregate
      .TypedImperativeAggregate[Array[Long]]
    with org.apache.spark.sql.catalyst.trees.UnaryLike[
      org.apache.spark.sql.catalyst.expressions.Expression] {

    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.util.GenericArrayData
    import org.apache.spark.sql.types.{ArrayType, DataType, LongType}
    import org.apache.spark.unsafe.Platform

    def sortedBinBoundaries: IndexedSeq[Long]

    // Size of underlying buffer for the aggregate. We make buffer of 2 * totalBins to store
    // fileCount as well as totalBytes.
    lazy val underlyingBufferSize: Int = sortedBinBoundaries.size * 2
    lazy val secondHalfStartIndex: Int = sortedBinBoundaries.size

    /**
     * The aggregation buffer of this aggregate is an Array of Longs of size - 2 * NumBins
     * 1. First half of the array represents fileCounts
     * 2. Second half of the array represents totalBytes
     *
     * --------------------------------------------------------------------------------
     * |     fileCounts related indices       |          totalBytes related indices   |
     * --------------------------------------------------------------------------------
     */
    override def createAggregationBuffer(): Array[Long] = Array.fill(underlyingBufferSize)(0)

    override def dataType: DataType = ArrayType(LongType)

    override def nullable: Boolean = {
      // This Aggregate doesn't return null
      false
    }

    override def update(aggBuffer: Array[Long], input: InternalRow): Array[Long] = {
      val value = child.eval(input)
      if (value != null) {
        val metricValue = value.asInstanceOf[Long]
        val index = FileStatsHistogram.getBinIndex(metricValue, sortedBinBoundaries)
        if (index >= 0) {
          aggBuffer(index) += 1
          aggBuffer(secondHalfStartIndex + index) += metricValue
        }
      }
      aggBuffer
    }

    override def merge(buffer: Array[Long], input: Array[Long]): Array[Long] = {
      buffer.indices.foreach { index =>
        buffer(index) += input(index)
      }
      buffer
    }

    override def eval(buffer: Array[Long]): Any = {
      new GenericArrayData(sortedBinBoundaries ++ buffer)
    }

    /** Serializes the aggregation buffer to Array[Byte] */
    override def serialize(buffer: Array[Long]): Array[Byte] = {
      val byteArray = new Array[Byte](buffer.length * 8)
      buffer.indices.foreach { index =>
        Platform.putLong(byteArray, Platform.BYTE_ARRAY_OFFSET + index * 8, buffer(index))
      }
      byteArray
    }

    /** De-serializes the serialized format Array[Byte], and produces aggregation buffer */
    override def deserialize(bytes: Array[Byte]): Array[Long] = {
      val aggBuffer = new Array[Long](bytes.length / 8)
      aggBuffer.indices.foreach { index =>
        aggBuffer(index) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + index * 8)
      }
      aggBuffer
    }
  }
}
