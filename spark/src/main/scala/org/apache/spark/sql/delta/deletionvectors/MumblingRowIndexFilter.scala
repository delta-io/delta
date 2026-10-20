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

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.deletionvectors.mumbling.{MumblingBitmap, MumblingBitmapWriter}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * A [[RowIndexFilter]] over an AMT manifest deletion vector (MDV): a leaf row is dropped when its
 * row index is masked (contained) by the MDV, and kept otherwise. This is the AMT-local analog of
 * [[org.apache.spark.sql.delta.util.BitmapFilter.IfContained]] and probes the bitmap per
 * row (row indices within a leaf are dense and small), which matches the point-membership shape of
 * the Mumbling bitmap.
 */
private[deletionvectors] final class MumblingRowIndexFilter(bitmap: MumblingBitmap)
  extends RowIndexFilter {

  private def valueFor(rowIndex: Long): Byte = {
    require(
      rowIndex < MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE,
      s"Row index $rowIndex exceeds the Mumbling bitmap limit " +
        s"${MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE - 1}")
    if (rowIndex >= 0 && bitmap.isSet(rowIndex.toInt)) {
      RowIndexFilter.DROP_ROW_VALUE
    } else {
      RowIndexFilter.KEEP_ROW_VALUE
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

/**
 * A [[RowIndexFilterProvider]] for a manifest deletion vector carried inline in `manifest_info.dv`.
 */
private[deletionvectors] final class MumblingRowIndexFilterProvider(
    inputSerializedBytes: Array[Byte], cardinality: Long)
  extends RowIndexFilterProvider with Serializable {

  private val serializedBytes = inputSerializedBytes.clone()

  private def loadBitmap(): MumblingBitmap =
    new MumblingBitmap(ByteBuffer.wrap(serializedBytes))

  override def retrieve(hadoopConf: Configuration): RowIndexFilter =
    new MumblingRowIndexFilter(loadBitmap())

}
