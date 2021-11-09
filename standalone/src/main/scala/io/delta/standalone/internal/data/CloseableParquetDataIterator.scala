/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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
import org.apache.hadoop.conf.Configuration

import io.delta.standalone.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.standalone.types._

/**
 * A [[CloseableIterator]] over [[RowParquetRecordJ]]s.
 *
 * Iterates file by file, row by row.
 *
 * @param dataFilePathsAndPartitions Seq of (file path, file partitions) tuples to iterate over,
 *                                   not null
 * @param schema for file data and partition values, not null. Used to read and verify the parquet
 *               data in file and partition data
 * @param readTimeZone time zone ID for data, not null. Used to ensure proper Date and Timestamp
 *                     decoding
 */
private[internal] case class CloseableParquetDataIterator(
    dataFilePathsAndPartitions: Seq[(String, Map[String, String])],
    schema: StructType,
    readTimeZone: TimeZone,
    hadoopConf: Configuration) extends CloseableIterator[RowParquetRecordJ] {

  private val dataFilePathsAndPartitionsIter = dataFilePathsAndPartitions.iterator

  /**
   * Iterable resource that allows for iteration over the parquet rows of a single file.
   * Must be closed.
   */
  private var parquetRows = if (dataFilePathsAndPartitionsIter.hasNext) readNextFile else null

  /**
   * Deserialized partition values. This variable gets updated every time `readNextFile` is called
   *
   * It makes more sense to deserialize partition values once per file than N times for each N row
   * in a file.
   */
  private var partitionValues: Map[String, Any] = _

  /**
   * Actual iterator over the parquet rows.
   *
   * We want this as its own variable, instead of calling `parquetRows.iterator.hasNext` or
   * `parquetRows.iterator.next`, as that returns a new iterator instance each time, thus restarting
   * at the head.
   */
  private var parquetRowsIter = if (null != parquetRows) parquetRows.iterator else null

  /**
   * @return true if there is next row of data in the current `dataFilePathsAndPartitions` file
   *         OR a row of data in the next `dataFilePathsAndPartitionsIter` file, else false
   */
  override def hasNext: Boolean = {
    // Base case when initialized to null
    if (null == parquetRows || null == parquetRowsIter) {
      close()
      return false
    }

    // We need to search for the next non-empty file
    while (true) {
      // More rows in current file
      if (parquetRowsIter.hasNext) return true

      // No more rows in current file and no more files
      if (!dataFilePathsAndPartitionsIter.hasNext) {
        close()
        return false
      }

      // No more rows in this file, but there is a next file
      parquetRows.close()

      // Repeat the search at the next file
      parquetRows = readNextFile
      parquetRowsIter = parquetRows.iterator
    }

    // Impossible
    throw new RuntimeException("Some bug in CloseableParquetDataIterator::hasNext")
  }

  /**
   * @return the next row of data the current `dataFilePathsAndPartitionsIter` file
   *         OR the first row of data in the next `dataFilePathsAndPartitionsIter` file
   * @throws NoSuchElementException if there is no next row of data
   */
  override def next(): RowParquetRecordJ = {
    if (!hasNext()) throw new NoSuchElementException
    val row = parquetRowsIter.next()
    RowParquetRecordImpl(row, schema, readTimeZone, partitionValues)
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
   * Requires that `dataFilePathsAndPartitionsIter.hasNext` is true.
   *
   * @return the iterable for the next data file in `dataFilePathsAndPartitionsIter`, not null
   */
  private def readNextFile: ParquetIterable[RowParquetRecord] = {
    val (nextDataFilePath, nextPartitionVals) = dataFilePathsAndPartitionsIter.next()

    partitionValues = Map()

    if (null != nextPartitionVals) {
      nextPartitionVals.foreach { case (fieldName, value) =>
        if (value == null) {
          partitionValues += (fieldName -> null)
        } else {
          val schemaField = schema.get(fieldName)
          if (schemaField != null) {
            val decodedFieldValue = decodePartition(schemaField.getDataType, value)
            partitionValues += (fieldName -> decodedFieldValue)
          } else {
            throw new IllegalStateException(s"StructField with name $schemaField was null.")
          }
        }
      }
    }

    ParquetReader.read[RowParquetRecord](
      nextDataFilePath, Options(timeZone = readTimeZone, hadoopConf = hadoopConf))
  }

  /**
   * Follows deserialization as specified here
   * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization
   */
  private def decodePartition(elemType: DataType, partitionVal: String): Any = {
    elemType match {
      case _: StringType => partitionVal
      case _: TimestampType => java.sql.Timestamp.valueOf(partitionVal)
      case _: DateType => java.sql.Date.valueOf(partitionVal)
      case _: IntegerType => partitionVal.toInt
      case _: LongType => partitionVal.toLong
      case _: ByteType => partitionVal.toByte
      case _: ShortType => partitionVal.toShort
      case _: BooleanType => partitionVal.toBoolean
      case _: FloatType => partitionVal.toFloat
      case _: DoubleType => partitionVal.toDouble
      case _: DecimalType => new java.math.BigDecimal(partitionVal)
      case _: BinaryType => partitionVal.getBytes("UTF-8")
      case _ =>
        throw new RuntimeException(s"Unknown decode type ${elemType.getTypeName}, $partitionVal")
    }
  }
}
