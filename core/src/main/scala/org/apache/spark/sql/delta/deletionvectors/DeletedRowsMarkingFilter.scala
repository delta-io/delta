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

import org.apache.spark.sql.delta.RowIndexFilter
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.vectorized.WritableColumnVector

/**
 * Implementation of [[RowIndexFilter]] which checks, for a given row index and deletion vector,
 * whether the row index is present in the deletion vector.If present, the row is marked for
 * skipping.
 * @param bitmap Represents the deletion vector
 */
final class DeletedRowsMarkingFilter(bitmap: RoaringBitmapArray) extends RowIndexFilter {

  override def materializeIntoVector(start: Long, end: Long, batch: WritableColumnVector): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      val isContained = bitmap.contains(start + rowId.toLong)
      val filterOutput = if (isContained) {
        RowIndexFilter.DROP_ROW_VALUE
      } else {
        RowIndexFilter.KEEP_ROW_VALUE
      }
      batch.putByte(rowId, filterOutput)
      rowId += 1
    }
  }
}

object DeletedRowsMarkingFilter {
  /**
   * Utility method that creates [[RowIndexFilter]] to filter out row indices that
   * are present in the given deletion vector.
   */
  def createInstance(
      deletionVector: DeletionVectorDescriptor,
      hadoopConf: Configuration,
      tablePath: Option[Path]): RowIndexFilter = {
    if (deletionVector.cardinality == 0) {
      // no rows are deleted according to the deletion vector, create a constant row index filter
      // that keeps all rows
      new KeepAllRowsFilter
    } else {
      require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
      val dvStore = DeletionVectorStore.createInstance(hadoopConf)
      val storedBitmap = StoredBitmap.create(deletionVector, tablePath.get)
      val bitmap = storedBitmap.load(dvStore)
      new DeletedRowsMarkingFilter(bitmap)
    }
  }

  private class KeepAllRowsFilter extends RowIndexFilter {
    override def materializeIntoVector(
        start: Long, end: Long, batch: WritableColumnVector): Unit = {
      val batchSize = (end - start).toInt
      var rowId = 0
      while (rowId < batchSize) {
        batch.putByte(rowId, RowIndexFilter.KEEP_ROW_VALUE)
        rowId += 1
      }
    }
  }
}
