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

package org.apache.spark.sql.delta.commands.backfill

import java.io.Closeable

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta.FileMetadataMaterializationTracker
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.Dataset

/**
 * Construct a lazily evaluated iterator for getting BackfillBatchType.
 * Callers should close this iterator to avoid resource leak.
 */
class BackfillBatchIterator[BackfillBatchType](
    filesToBackfill: Dataset[AddFile],
    tracker: FileMetadataMaterializationTracker,
    maxNumFilesPerBin: Int,
    constructBatch: (Seq[AddFile]) => BackfillBatchType)
    extends Iterator[BackfillBatchType] with Closeable {
  private val collectedFiles = filesToBackfill.collectAsList()

  private val addFileItr = new Iterator[AddFile] {
    private val underlying = collectedFiles.iterator

    override def hasNext: Boolean = underlying.hasNext

    override def next(): AddFile = underlying.next()
  }

  private val sourcePeekableItr = addFileItr.buffered

  private val buffer: ListBuffer[AddFile] = ListBuffer.empty[AddFile]

  private var lastBin: Seq[AddFile] = Nil

  private def createBin(): Seq[AddFile] = {
    val taskLevelPermitAllocator = tracker.createTaskLevelPermitAllocator()

    while (sourcePeekableItr.hasNext) {
      // Acquire permit to fetch the next file from source.
      taskLevelPermitAllocator.acquirePermit()

      val nextFile = sourcePeekableItr.next()

      buffer.append(nextFile)

      if (buffer.size >= maxNumFilesPerBin || (tracker.executeBatchEarly() && buffer.nonEmpty)) {
        val fileBin = buffer.clone()
        buffer.clear()
        return fileBin.toSeq
      }
    }

    val fileBin = buffer.clone()
    buffer.clear()
    fileBin.toSeq
  }

  override def hasNext: Boolean = {
    if (lastBin.isEmpty) {
      lastBin = createBin()
    }
    lastBin.nonEmpty
  }

  override def next(): BackfillBatchType = {
    val nextBin = lastBin
    lastBin = Nil
    constructBatch(nextBin)
  }

  override def close(): Unit = {
  }
}
