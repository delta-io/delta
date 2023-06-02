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
 * Base class for row index filters.
 * @param bitmap Represents the deletion vector
 */
abstract sealed class RowIndexMarkingFilters(bitmap: RoaringBitmapArray) extends RowIndexFilter {
  val valueWhenContained: Byte
  val valueWhenNotContained: Byte

  override def materializeIntoVector(start: Long, end: Long, batch: WritableColumnVector): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      val isContained = bitmap.contains(start + rowId.toLong)
      val filterOutput = if (isContained) {
        valueWhenContained
      } else {
        valueWhenNotContained
      }
      batch.putByte(rowId, filterOutput)
      rowId += 1
    }
  }
}

sealed trait RowIndexMarkingFiltersBuilder {
  def getFilterForEmptyDeletionVector(): RowIndexFilter
  def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter

  def createInstance(
      deletionVector: DeletionVectorDescriptor,
      hadoopConf: Configuration,
      tablePath: Option[Path]): RowIndexFilter = {
    if (deletionVector.cardinality == 0) {
      getFilterForEmptyDeletionVector()
    } else {
      require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
      val dvStore = DeletionVectorStore.createInstance(hadoopConf)
      val storedBitmap = StoredBitmap.create(deletionVector, tablePath.get)
      val bitmap = storedBitmap.load(dvStore)
      getFilterForNonEmptyDeletionVector(bitmap)
    }
  }
}

/**
 * Implementation of [[RowIndexFilter]] which checks, for a given row index and deletion vector,
 * whether the row index is present in the deletion vector. If present, the row is marked for
 * skipping.
 * @param bitmap Represents the deletion vector
 */
final class DropMarkedRowsFilter(bitmap: RoaringBitmapArray)
  extends RowIndexMarkingFilters(bitmap) {
  override val valueWhenContained: Byte = RowIndexFilter.DROP_ROW_VALUE
  override val valueWhenNotContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
}

/**
 * Utility methods that creates [[DropMarkedRowsFilter]] to filter out row indices that are present
 * in the given deletion vector.
 */
object DropMarkedRowsFilter extends RowIndexMarkingFiltersBuilder {
  override def getFilterForEmptyDeletionVector(): RowIndexFilter = KeepAllRowsFilter

  override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter =
    new DropMarkedRowsFilter(bitmap)
}

/**
 * Implementation of [[RowIndexFilter]] which checks, for a given row index and deletion vector,
 * whether the row index is present in the deletion vector. If not present, the row is marked for
 * skipping.
 * @param bitmap Represents the deletion vector
 */
final class KeepMarkedRowsFilter(bitmap: RoaringBitmapArray)
  extends RowIndexMarkingFilters(bitmap) {
  override val valueWhenContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
  override val valueWhenNotContained: Byte = RowIndexFilter.DROP_ROW_VALUE
}

/**
 * Utility methods that creates [[KeepMarkedRowsFilter]] to filter out row indices that are present
 * in the given deletion vector.
 */
object KeepMarkedRowsFilter extends RowIndexMarkingFiltersBuilder {
  override def getFilterForEmptyDeletionVector(): RowIndexFilter = DropAllRowsFilter

  override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter =
    new KeepMarkedRowsFilter(bitmap)
}

case object DropAllRowsFilter extends RowIndexFilter {
  override def materializeIntoVector(start: Long, end: Long, batch: WritableColumnVector): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      batch.putByte(rowId, RowIndexFilter.DROP_ROW_VALUE)
      rowId += 1
    }
  }
}

case object KeepAllRowsFilter extends RowIndexFilter {
  override def materializeIntoVector(start: Long, end: Long, batch: WritableColumnVector): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      batch.putByte(rowId, RowIndexFilter.KEEP_ROW_VALUE)
      rowId += 1
    }
  }
}
