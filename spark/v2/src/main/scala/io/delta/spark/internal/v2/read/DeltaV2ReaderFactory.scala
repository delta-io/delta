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
package io.delta.spark.internal.v2.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Package-private reader factory for Delta's Spark DataSource V2 read path.
 *
 * This class must remain package-private so callers outside `v2.read` depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
private[read] class DeltaV2ReaderFactory(
    private[this] val readFunc: PartitionedFile => Iterator[InternalRow],
    private[this] val supportsColumnar: Boolean)
    extends PartitionReaderFactory {


  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val filePartition = partition.asInstanceOf[FilePartition]
    new DeltaV2PartitionReader[ColumnarBatch](readFunc, filePartition)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = supportsColumnar

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val filePartition = partition.asInstanceOf[FilePartition]
    new DeltaV2PartitionReader[InternalRow](readFunc, filePartition)
  }
}
