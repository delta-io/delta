/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.data

import java.util.TimeZone

import com.github.mjakubowski84.parquet4s._
import com.github.mjakubowski84.parquet4s.ParquetReader.Options

import io.delta.standalone.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.standalone.types.StructType

/**
 * A [[CloseableIterator]] over [[RowParquetRecordJ]]s.
 * Iterates file by file, row by row.
 *
 * @param dataFilePaths paths of files to iterate over, not null
 * @param schema data schema, not null. Used to read and verify the parquet data
 * @param timeZoneId time zone ID for data, can be null. Used to ensure proper Date and Timestamp
 *                   decoding
 */
private[internal] case class CloseableParquetDataIterator(
    dataFilePaths: Seq[String],
    schema: StructType,
    timeZoneId: String) extends CloseableIterator[RowParquetRecordJ] {

  /** Convert the timeZoneId to an actual timeZone that can be used for decoding. */
  private val readTimeZone =
    if (null == timeZoneId) TimeZone.getDefault else TimeZone.getTimeZone(timeZoneId)

  /** Iterator over the `dataFilePaths`. */
  private val dataFilePathsIter = dataFilePaths.iterator

  /**
   * Iterable resource that allows for iteration over the parquet rows for a single file.
   * Must be closed.
   */
  private var parquetRows = if (dataFilePathsIter.hasNext) readNextFile else null

  /**
   * Actual iterator over the parquet rows.
   *
   * We want this as its own variable, instead of calling `parquetRows.iterator.hasNext` or
   * `parquetRows.iterator.next`, as that returns a new iterator instance each time, thus restarting
   * at the head.
   */
  private var parquetRowsIter = if (null != parquetRows) parquetRows.iterator else null

  /**
   * @return true if there is next row of data in the current `dataFilePaths` file OR a row of
   *         data in the next `dataFilePathsIter` file, else false
   */
  override def hasNext: Boolean = {
    // Base case when initialized to null
    if (null == parquetRows || null == parquetRowsIter) {
      close()
      return false
    }

    // More rows in current file
    if (parquetRowsIter.hasNext) return true

    // No more rows in current file and no more files
    if (!dataFilePathsIter.hasNext) {
      close()
      return false
    }

    // No more rows in this file, but there is a next file
    parquetRows.close()
    parquetRows = readNextFile
    parquetRowsIter = parquetRows.iterator
    parquetRowsIter.hasNext
  }

  /**
   * @return the next row of data the current `dataFilePathsIter` file OR the first row of data in
   *         the next `dataFilePathsIter` file
   * @throws NoSuchElementException if there is no next row of data
   */
  override def next(): RowParquetRecordJ = {
    if (!hasNext) throw new NoSuchElementException
    val row = parquetRowsIter.next()
    RowParquetRecordImpl(row, schema, readTimeZone)
  }

  /**
   * Closes the `parquetRows` iterable and sets fields to null, ensuring that all following calls
   * to `hasNext` return false
   */
  override def close(): Unit = {
    if (null != parquetRows) {
      parquetRows.close()
      parquetRows = null
      parquetRowsIter = null
    }
  }

  /**
   * Requires that `dataFilePathsIter.hasNext` is true.
   *
   * @return the iterable for the next data file in `dataFilePathsIter`, not null
   */
  private def readNextFile: ParquetIterable[RowParquetRecord] = {
    ParquetReader.read[RowParquetRecord](dataFilePathsIter.next(), Options(readTimeZone))
  }
}
