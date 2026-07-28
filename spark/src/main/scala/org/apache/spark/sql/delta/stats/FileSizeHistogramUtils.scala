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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.udf

/**
 * This class contains all the functionality related to [[FileSizeHistogram]]
 */
object FileSizeHistogramUtils {

  private val KB: Long = 1024
  private val MB: Long = 1024 * 1024
  private val GB: Long = 1024 * 1024 * 1024

  val DEFAULT_BINS: IndexedSeq[Long] = IndexedSeq(
    0,
    // Power of 2 till 4 MB
    8 * KB, 16 * KB, 32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB, 1 * MB, 2 * MB, 4 * MB,
    // 4 MB jumps till 40 MB
    8 * MB, 12 * MB, 16 * MB, 20 * MB, 24 * MB, 28 * MB, 32 * MB, 36 * MB, 40 * MB,
    // 8 MB jumps till 120 MB
    48 * MB, 56 * MB, 64 * MB, 72 * MB, 80 * MB, 88 * MB, 96 * MB, 104 * MB, 112 * MB, 120 * MB,
    // 4 MB jumps till 144 MB (since we want more detail around the 128 MB mark)
    124 * MB, 128 * MB, 132 * MB, 136 * MB, 140 * MB, 144 * MB,
    // 16 MB jumps till 576 MB (reasonable detail until past the 512 MB mark)
    160 * MB, 176 * MB, 192 * MB, 208 * MB, 224 * MB, 240 * MB, 256 * MB, 272 * MB, 288 * MB,
    304 * MB, 320 * MB, 336 * MB, 352 * MB, 368 * MB, 384 * MB, 400 * MB, 416 * MB, 432 * MB,
    448 * MB, 464 * MB, 480 * MB, 496 * MB, 512 * MB, 528 * MB, 544 * MB, 560 * MB, 576 * MB,
    // 64 MB jumps till 1408 MB (detail around the 1024MB mark, allowing for overshoot)
    640 * MB, 704 * MB, 768 * MB, 832 * MB, 896 * MB, 960 * MB, 1024 * MB, 1088 * MB, 1152 * MB,
    1216 * MB, 1280 * MB, 1344 * MB, 1408 * MB,
    // 128 MB jumps till 2 GB
    1536 * MB, 1664 * MB, 1792 * MB, 1920 * MB, 2048 * MB,
    // 256 MB jumps till 4 GB
    2304 * MB, 2560 * MB, 2816 * MB, 3072 * MB, 3328 * MB, 3584 * MB, 3840 * MB, 4 * GB,
    // power of 2 till 256 GB
    8 * GB, 16 * GB, 32 * GB, 64 * GB, 128 * GB, 256 * GB
  )

  /**
   * Returns an empty histogram with [[DEFAULT_BINS]]
   */
  def emptyHistogram: FileSizeHistogram = FileSizeHistogram.apply(DEFAULT_BINS)

  /**
   * Returns a compacted version of this FileSizeHistogram where empty bins are merged together.
   */
  def compress(h: FileSizeHistogram): FileSizeHistogram = {
    FileStatsHistogram.compress(h, FileSizeHistogram.apply)
  }


  /**
   * An imperative aggregate implementation of FileSizeHistogram.
   * Extends the base FileStatsHistogramAggBase class with file size specific bins.
   */
  case class FileSizeHistogramAgg(
    child: Expression,
    sortedBinBoundaries: IndexedSeq[Long] = DEFAULT_BINS,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
  ) extends FileStatsHistogram.FileStatsHistogramAggBase {

    override protected def withNewChildInternal(newChild: Expression): FileSizeHistogramAgg =
      copy(child = newChild)

    override def withNewMutableAggBufferOffset(offset: Int): FileSizeHistogramAgg =
      copy(mutableAggBufferOffset = offset)

    override def withNewInputAggBufferOffset(offset: Int): FileSizeHistogramAgg =
      copy(inputAggBufferOffset = offset)
  }

  /**
   * A UDF to convert flattenedHistogram (returned by [[FileSizeHistogramAgg]]) to
   * [[FileSizeHistogram]]
   */
  private lazy val flattenedHistogramToHistogramUDF = udf((flattenedHistogram: Array[Long]) => {
    val splits = flattenedHistogram.grouped(flattenedHistogram.length / 3).toSeq
    new FileSizeHistogram(splits(0), splits(1), splits(2))
  })

  def histogramAggregate(child: Expression): Column = {
    val aggregate = Column(FileSizeHistogramAgg(child).toAggregateExpression())
    FileSizeHistogramUtils.flattenedHistogramToHistogramUDF(aggregate)
  }
}
