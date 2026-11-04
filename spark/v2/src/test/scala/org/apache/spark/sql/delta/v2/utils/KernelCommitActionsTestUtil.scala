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
package org.apache.spark.sql.delta.v2.utils

import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.data.{ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.data.{ColumnVector => KernelColumnVector}
import io.delta.kernel.types.{StructType => KernelStructType}
import io.delta.kernel.utils.{CloseableIterator => KernelCloseableIterator}

object KernelCommitActionsTestUtil {

  def createCommitActions(
      kernelColumns: Seq[(String, KernelColumnVector)],
      kernelAdditionalBatchColumns: Seq[Seq[(String, KernelColumnVector)]] = Seq.empty)
      : TestCommitActions = {
    val kernelBatches = (kernelColumns +: kernelAdditionalBatchColumns).map { kernelBatchColumns =>
      new KernelColumnarBatch {
        override def getSchema: KernelStructType =
          kernelBatchColumns.foldLeft(new KernelStructType()) {
            case (schema, (name, kernelColumn)) => schema.add(name, kernelColumn.getDataType)
          }
        override def getColumnVector(ordinal: Int): KernelColumnVector =
          kernelBatchColumns(ordinal)._2
        override def getSize: Int = kernelBatchColumns.head._2.getSize
      }
    }
    new TestCommitActions(kernelBatches)
  }

  class TestCommitActions(kernelBatches: Seq[KernelColumnarBatch]) extends KernelCommitActions {
    var closed = false
    override def getVersion: Long = 1L
    override def getTimestamp: Long = 0L
    override def getActions(): KernelCloseableIterator[KernelColumnarBatch] = {
      if (closed) {
        throw new IllegalStateException("Commit actions are closed")
      }
      new KernelCloseableIterator[KernelColumnarBatch] {
        private val batches = kernelBatches.iterator
        private var closed = false
        override def hasNext: Boolean = {
          if (closed) {
            throw new IllegalStateException("Action iterator is closed")
          }
          batches.hasNext
        }
        override def next(): KernelColumnarBatch = {
          if (!hasNext) {
            throw new NoSuchElementException("No more action batches")
          }
          batches.next()
        }
        override def close(): Unit = closed = true
      }
    }
    override def close(): Unit = closed = true
  }
}
