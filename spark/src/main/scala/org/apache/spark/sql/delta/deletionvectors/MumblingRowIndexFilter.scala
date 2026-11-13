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

package org.apache.spark.sql.delta.deletionvectors

import java.nio.ByteBuffer

import scala.collection.Seq

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.deletionvectors.mumbling.{MumblingBitmap, MumblingBitmapWriter}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * A [[RowIndexFilter]] over one or more AMT manifest bitmaps. By default a leaf row is dropped when
 * its row index is set in any bitmap, and kept otherwise. With `inverse`, only rows set in at least
 * one bitmap are kept. This is the AMT-local analog of
 * [[org.apache.spark.sql.delta.util.BitmapFilter.IfContained]] or
 * [[org.apache.spark.sql.delta.util.BitmapFilter.IfNotContained]], and probes each
 * bitmap per row instead of materializing their union.
 */
private[deletionvectors] final class MumblingRowIndexFilter(
    bitmaps: Seq[MumblingBitmap],
    inverse: Boolean = false)
  extends RowIndexFilter {

  require(bitmaps.nonEmpty, "At least one Mumbling bitmap is required")

  private def valueFor(rowIndex: Long): Byte = {
    require(
      rowIndex < MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE,
      s"Row index $rowIndex exceeds the Mumbling bitmap limit " +
        s"${MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE - 1}")
    val isSet = rowIndex >= 0 && bitmaps.exists(_.isSet(rowIndex.toInt))
    if (inverse) {
      if (isSet) RowIndexFilter.KEEP_ROW_VALUE else RowIndexFilter.DROP_ROW_VALUE
    } else {
      if (isSet) RowIndexFilter.DROP_ROW_VALUE else RowIndexFilter.KEEP_ROW_VALUE
    }
  }

  override def materializeIntoVector(
      start: Long, end: Long, batch: WritableColumnVector): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      batch.putByte(rowId, valueFor(start + rowId.toLong))
      rowId += 1
    }
  }

  override def materializeIntoVectorWithRowIndex(
      batchSize: Int, rowIndexColumn: ColumnVector, batch: WritableColumnVector): Unit = {
    var rowId = 0
    while (rowId < batchSize) {
      batch.putByte(rowId, valueFor(rowIndexColumn.getLong(rowId)))
      rowId += 1
    }
  }

  override def materializeSingleRowWithRowIndex(
      rowIndex: Long, batch: WritableColumnVector): Unit =
    batch.putByte(0, valueFor(rowIndex))
}

/** A [[RowIndexFilterProvider]] for one or more inline AMT manifest bitmaps. */
private[deletionvectors] final class MumblingRowIndexFilterProvider(
    inputSerializedBitmaps: Seq[Array[Byte]],
    inputComponentCardinalities: Seq[Long],
    inverse: Boolean = false)
  extends RowIndexFilterProvider with Serializable {

  require(inputSerializedBitmaps.nonEmpty, "At least one serialized Mumbling bitmap is required")
  require(inputSerializedBitmaps.size == inputComponentCardinalities.size,
    "Each serialized Mumbling bitmap must have one component cardinality")
  require(inputComponentCardinalities.forall(_ >= 0),
    s"Component cardinalities must be non-negative: $inputComponentCardinalities")

  private[deletionvectors] val serializedBitmaps = inputSerializedBitmaps.map(_.clone()).toArray
  private[deletionvectors] val componentCardinalities = inputComponentCardinalities.toArray

  private def loadBitmaps(): Seq[MumblingBitmap] =
    serializedBitmaps.map(bytes => new MumblingBitmap(ByteBuffer.wrap(bytes))).toSeq

  override def retrieve(hadoopConf: Configuration): RowIndexFilter =
    new MumblingRowIndexFilter(loadBitmaps(), inverse)

}
