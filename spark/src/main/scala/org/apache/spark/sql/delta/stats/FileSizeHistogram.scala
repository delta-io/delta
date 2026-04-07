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
}

private[delta] object FileSizeHistogram {

  private val KB = 1024L
  private val MB = 1024L * KB
  private val GB = 1024L * MB

  // Exponentially spaced bins with finer resolution around the 128MB region.
  val DEFAULT_BINS: IndexedSeq[Long] = IndexedSeq(
    0L,
    8*KB, 16*KB, 32*KB, 64*KB, 128*KB, 256*KB,
    512*KB, 1*MB, 2*MB, 4*MB,
    8*MB, 12*MB, 16*MB, 20*MB, 24*MB, 28*MB,
    32*MB, 36*MB, 40*MB,
    48*MB, 56*MB, 64*MB, 72*MB, 80*MB,
    88*MB, 96*MB, 104*MB, 112*MB, 120*MB,
    124*MB, 128*MB, 132*MB, 136*MB, 140*MB, 144*MB,
    160*MB, 176*MB, 192*MB, 208*MB, 224*MB, 240*MB,
    256*MB, 272*MB, 288*MB, 304*MB, 320*MB, 336*MB,
    352*MB, 368*MB, 384*MB, 400*MB, 416*MB, 432*MB,
    448*MB, 464*MB, 480*MB, 496*MB, 512*MB, 528*MB,
    544*MB, 560*MB, 576*MB,
    640*MB, 704*MB, 768*MB, 832*MB, 896*MB, 960*MB,
    1024*MB, 1088*MB, 1152*MB, 1216*MB, 1280*MB,
    1344*MB, 1408*MB,
    1536*MB, 1664*MB, 1792*MB, 1920*MB, 2048*MB,
    2304*MB, 2560*MB, 2816*MB, 3072*MB,
    3328*MB, 3584*MB, 3840*MB, 4*GB,
    8*GB, 16*GB, 32*GB, 64*GB, 128*GB, 256*GB
  )

  def emptyHistogram: FileSizeHistogram = FileSizeHistogram(DEFAULT_BINS)

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
