/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2.read

import java.io.{Closeable, IOException}
import java.util.Objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

/**
 * Package-private partition reader for Delta's Spark DataSource V2 read path.
 *
 * This class must remain package-private so callers outside `v2.read` depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
private[read] class DeltaV2PartitionReader[T](
    readFuncInput: PartitionedFile => Iterator[InternalRow],
    partitionInput: FilePartition)
    extends PartitionReader[T] {
  // Function that produces an Iterator for a given file.
  private[this] val readFunc = Objects.requireNonNull(readFuncInput, "readFunc")
  private[this] val partition = Objects.requireNonNull(partitionInput, "partition")

  // Index of the next file to read within the partition.
  private[this] var currentFileIndex = 0

  // Current iterator for the file being read.
  private[this] var currentIterator: Iterator[T] = null


  @throws[IOException]
  override def next(): Boolean = {
    // Advance to the next available record, opening readers as needed and closing exhausted ones.
    while (true) {
      if (currentIterator != null && currentIterator.hasNext) {
        return true
      }

      closeCurrentIterator()

      if (currentFileIndex >= partition.files.length) {
        return false
      }

      val files = partition.files
      val fileIndex = currentFileIndex
      currentFileIndex += 1
      val file = files(fileIndex)
      currentIterator = readFunc(file).asInstanceOf[Iterator[T]]
    }
    false
  }

  override def get(): T = {
    if (currentIterator == null) {
      throw new IllegalStateException("No current record. Call next() before get().")
    }
    currentIterator.next()
  }

  @throws[IOException]
  override def close(): Unit = {
    closeCurrentIterator()
  }

  @throws[IOException]
  private def closeCurrentIterator(): Unit = {
    if (currentIterator != null) {
      currentIterator match {
        case closeable: Closeable => closeable.close()
        case _ =>
      }
      currentIterator = null
    }
  }
}
