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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * A handle to a single commit in the Delta log, standing in for the raw commit log file that
 * [[DeltaLog.getChangeLogFiles]] used to expose as a [[FileStatus]] -- now hidden so the log's
 * physical layout stays internal, a prerequisite for the Adaptive Metadata Tree (AMT).
 */
class SingleCommit private (
    private val deltaLog: DeltaLog,
    val version: Long,
    private val fileStatus: FileStatus
) {
  /**
   * Open a fresh iterator over all of this commit's actions.
   *
   * @param maxInMemoryFileSizeBytes commits whose log file is smaller than this are read into
   *        memory once and replayed on every `rewind()`; larger commits are re-read on each pass.
   *        Defaults to 0, i.e. always stream via `readAsIterator` -- callers that want the
   *        in-memory/rewind fast path opt in by passing a threshold.
   */
  def getActionsIterator(
      maxInMemoryFileSizeBytes: Long = 0): ClosableIterator[Action] with SupportsRewinding[Action] =
    SingleCommit.createRewindableActionIterator(deltaLog, fileStatus, maxInMemoryFileSizeBytes)

  /**
   * The modification time of this commit's log file. Delta internal only.
   */
  private[delta] def fileModificationTimestamp: Long = fileStatus.getModificationTime

  /**
   * The size of this commit's log file, in bytes. Delta internal only.
   */
  private[delta] def sizeInBytes: Long = fileStatus.getLen

  /**
   * The commit log file's path. Should used by delta internal only.
   */
  private[delta] def path: Path = fileStatus.getPath
}

object SingleCommit {
  /**
   * Wrap a commit's log file into a [[SingleCommit]] handle. The raw [[FileStatus]] stays hidden
   * inside the handle so the log's physical layout is not exposed to callers.
   */
  private[delta] def apply(
      deltaLog: DeltaLog,
      version: Long,
      fileStatus: FileStatus): SingleCommit =
    new SingleCommit(deltaLog, version, fileStatus)

  /**
   * Read a single commit's actions into a rewindable, closable iterator, respecting memory
   * constraints. If the commit's log file is smaller than `maxInMemoryFileSizeBytes`, its actions
   * are read into memory once and replayed on every `rewind()`; otherwise the file is re-read on
   * each pass.
   *
   * TODO(AMT): This needs AMT support for manifest commits, whose file actions live in the content
   * tree rather than the commit JSON.
   */
  private def createRewindableActionIterator(
      deltaLog: DeltaLog,
      fileStatus: FileStatus,
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] = {
    lazy val actions =
      deltaLog.store.read(fileStatus, deltaLog.newDeltaHadoopConf()).map(Action.fromJson)
    // If the commit is smaller than the threshold, read it into memory once and iterate over that
    // buffer on every pass; otherwise re-read the file each time.
    val shouldLoadIntoMemory = fileStatus.getLen < maxInMemoryFileSizeBytes
    def createClosableIterator(): ClosableIterator[Action] = if (shouldLoadIntoMemory) {
      actions.toIterator.toClosable
    } else {
      deltaLog.store.readAsIterator(fileStatus, deltaLog.newDeltaHadoopConf()).withClose {
        _.map(Action.fromJson)
      }
    }
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private var delegatedIterator: ClosableIterator[Action] = createClosableIterator()
      override def hasNext: Boolean = delegatedIterator.hasNext
      override def next(): Action = delegatedIterator.next()
      override def close(): Unit = delegatedIterator.close()
      override def rewind(): Unit = {
        // Close the outgoing delegate before replacing it.
        delegatedIterator.close()
        delegatedIterator = createClosableIterator()
      }
    }
  }
}
