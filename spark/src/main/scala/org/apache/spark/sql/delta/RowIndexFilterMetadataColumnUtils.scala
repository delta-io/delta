/*
 * Copyright (2026) The Delta Lake Project Authors.
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


package org.apache.spark.sql.delta

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.RowIndexFilter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{ByteType, StructField}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchRow, ColumnVector}

/** Materializes row-index-filter and row-index metadata columns in Parquet reader output. */
private[delta] object RowIndexFilterMetadataColumnUtils {

  /**
   * Populates the row-index-filter marker and optional generated row-index column after the caller
   * has resolved the file's [[RowIndexFilter]].
   */
  private[delta] def iteratorWithAdditionalMetadataColumns(
      iterator: Iterator[Object],
      rowIndexFilterOpt: Option[RowIndexFilter],
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      rowIndexColumnOpt: Option[ColumnMetadata],
      useOffHeapBuffers: Boolean,
      useMetadataRowIndex: Boolean): Iterator[Object] = {
    // We only generate the row index column when predicate pushdown is not enabled.
    val rowIndexColumnToWriteOpt = if (useMetadataRowIndex) None else rowIndexColumnOpt
    val metadataColumnsToWrite =
      Seq(isRowDeletedColumnOpt, rowIndexColumnToWriteOpt).filter(_.nonEmpty).map(_.get)

    // When metadata.row_index is not used there is no way to verify the Parquet index is
    // starting from 0. We disable the splits, so the assumption is ParquetFileFormat respects
    // that.
    var rowIndex: Long = 0

    // Used only when non-column row batches are received from the Parquet reader
    val tempVector = new OnHeapColumnVector(1, ByteType)

    iterator.map { row =>
      row match {
        case batch: ColumnarBatch => // When vectorized Parquet reader is enabled.
          val size = batch.numRows()
          // Create vectors for all needed metadata columns.
          // We can't use the one from Parquet reader as it set the
          // [[WritableColumnVector.isAllNulls]] to true and it can't be reset with using any
          // public APIs.
          trySafely(
            useOffHeapBuffers, size, metadataColumnsToWrite) { writableVectors =>
            val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]

            // When predicate pushdown is enabled we use _metadata.row_index. Therefore,
            // we only need to construct the isRowDeleted column.
            var index = 0
            isRowDeletedColumnOpt.foreach { columnMetadata =>
              val isRowDeletedVector = writableVectors(index)
              if (useMetadataRowIndex) {
                rowIndexFilterOpt.get.materializeIntoVectorWithRowIndex(
                  size, batch.column(rowIndexColumnOpt.get.index), isRowDeletedVector)
              } else {
                rowIndexFilterOpt.get
                  .materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
              }
              indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
              index += 1
            }

            rowIndexColumnToWriteOpt.foreach { columnMetadata =>
              val rowIndexVector = writableVectors(index)
              // populate the row index column value.
              for (i <- 0 until size) {
                rowIndexVector.putLong(i, rowIndex + i)
              }

              indexVectorTuples += (columnMetadata.index -> rowIndexVector)
              index += 1
            }

            val newBatch = replaceVectors(batch, indexVectorTuples.toSeq: _*)
            rowIndex += size
            newBatch
          }

        case columnarRow: ColumnarBatchRow =>
          // When vectorized reader is enabled but returns immutable rows instead of
          // columnar batches [[ColumnarBatchRow]]. So we have to copy the row as a
          // mutable [[InternalRow]] and set the `row_index` and `is_row_deleted`
          // column values. This is not efficient. It should affect only the wide
          // tables. https://github.com/delta-io/delta/issues/2246
          val newRow = columnarRow.copy();
          isRowDeletedColumnOpt.foreach { columnMetadata =>
            val rowIndexForFiltering = if (useMetadataRowIndex) {
              columnarRow.getLong(rowIndexColumnOpt.get.index)
            } else {
              rowIndex
            }
            rowIndexFilterOpt.get.materializeSingleRowWithRowIndex(rowIndexForFiltering, tempVector)
            newRow.setByte(columnMetadata.index, tempVector.getByte(0))
          }

          rowIndexColumnToWriteOpt
            .foreach(columnMetadata => newRow.setLong(columnMetadata.index, rowIndex))
          rowIndex += 1

          newRow
        case rest: InternalRow => // When vectorized Parquet reader is disabled
          // Temporary vector variable used to get DV values from RowIndexFilter
          // Currently the RowIndexFilter only supports writing into a columnar vector
          // and doesn't have methods to get DV value for a specific row index.
          // TODO: This is not efficient, but it is ok given the default reader is vectorized
          isRowDeletedColumnOpt.foreach { columnMetadata =>
            val rowIndexForFiltering = if (useMetadataRowIndex) {
              rest.getLong(rowIndexColumnOpt.get.index)
            } else {
              rowIndex
            }
            rowIndexFilterOpt.get.materializeSingleRowWithRowIndex(rowIndexForFiltering, tempVector)
            rest.setByte(columnMetadata.index, tempVector.getByte(0))
          }

          rowIndexColumnToWriteOpt
            .foreach(columnMetadata => rest.setLong(columnMetadata.index, rowIndex))
          rowIndex += 1
          rest
        case others =>
          throw new RuntimeException(
            s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
      }
    }
  }

  /** Utility method to create a new writable vector */
  private[delta] def newVector(
      useOffHeapBuffers: Boolean, size: Int, dataType: StructField): WritableColumnVector = {
    if (useOffHeapBuffers) {
      OffHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    } else {
      OnHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    }
  }

  /** Try the operation, if the operation fails release the created resource */
  private[delta] def trySafely[R <: WritableColumnVector, T](
      useOffHeapBuffers: Boolean,
      size: Int,
      columns: Seq[ColumnMetadata])(f: Seq[WritableColumnVector] => T): T = {
    val resources = new ArrayBuffer[WritableColumnVector](columns.size)
    try {
      columns.foreach(col => resources.append(newVector(useOffHeapBuffers, size, col.structField)))
      f(resources.toSeq)
    } catch {
      case NonFatal(e) =>
        resources.foreach(closeQuietly(_))
        throw e
    }
  }

  /** Utility method to quietly close an [[AutoCloseable]] */
  private[delta] def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try {
        closeable.close()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  /**
   * Helper method to replace the vectors in given [[ColumnarBatch]].
   * New vectors and its index in the batch are given as tuples.
   */
  private[delta] def replaceVectors(
      batch: ColumnarBatch,
      indexVectorTuples: (Int, ColumnVector) *): ColumnarBatch = {
    val vectors = ArrayBuffer[ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced: Boolean = false
      for (indexVectorTuple <- indexVectorTuples) {
        val index = indexVectorTuple._1
        val vector = indexVectorTuple._2
        if (indexVectorTuple._1 == i) {
          vectors += indexVectorTuple._2
          // Make sure to close the existing vector allocated in the Parquet
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }

  /** Helper class to encapsulate column info */
  case class ColumnMetadata(index: Int, structField: StructField)
}
