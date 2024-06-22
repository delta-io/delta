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

package io.delta.standalone.internal

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * An iterator that helps select old log files for deletion. It takes the input iterator of log
 * files from the earliest file, and returns should-be-deleted files until the given maxTimestamp
 * or maxVersion to delete is reached. Note that this iterator may stop deleting files earlier
 * than maxTimestamp or maxVersion if it finds that files that need to be preserved for adjusting
 * the timestamps of subsequent files. Let's go through an example. Assume the following commit
 * history:
 *
 * +---------+-----------+--------------------+
 * | Version | Timestamp | Adjusted Timestamp |
 * +---------+-----------+--------------------+
 * |       0 |         0 |                  0 |
 * |       1 |         5 |                  5 |
 * |       2 |        10 |                 10 |
 * |       3 |         7 |                 11 |
 * |       4 |         8 |                 12 |
 * |       5 |        14 |                 14 |
 * +---------+-----------+--------------------+
 *
 * As you can see from the example, we require timestamps to be monotonically increasing with
 * respect to the version of the commit, and each commit to have a unique timestamp. If we have
 * a commit which doesn't obey one of these two requirements, we adjust the timestamp of that
 * commit to be one millisecond greater than the previous commit.
 *
 * Given the above commit history, the behavior of this iterator will be as follows:
 *  - For maxVersion = 1 and maxTimestamp = 9, we can delete versions 0 and 1
 *  - Until we receive maxVersion >= 4 and maxTimestamp >= 12, we can't delete versions 2 and 3.
 *    This is because version 2 is used to adjust the timestamps of commits up to version 4.
 *  - For maxVersion >= 5 and maxTimestamp >= 14 we can delete everything
 * The semantics of time travel guarantee that for a given timestamp, the user will ALWAYS get the
 * same version. Consider a user asks to get the version at timestamp 11. If all files are there,
 * we would return version 3 (timestamp 11) for this query. If we delete versions 0-2, the
 * original timestamp of version 3 (7) will not have an anchor to adjust on, and if the time
 * travel query is re-executed we would return version 4. This is the motivation behind this
 * iterator implementation.
 *
 * The implementation maintains an internal "maybeDelete" buffer of files that we are unsure of
 * deleting because they may be necessary to adjust time of future files. For each file we get
 * from the underlying iterator, we check whether it needs time adjustment or not. If it does need
 * time adjustment, then we cannot immediately decide whether it is safe to delete that file or
 * not and therefore we put it in each the buffer. Then we iteratively peek ahead at the future
 * files and accordingly decide whether to delete all the buffered files or retain them.
 *
 * @param underlying The iterator which gives the list of files in ascending version order
 * @param maxTimestamp The timestamp until which we can delete (inclusive).
 * @param maxVersion The version until which we can delete (inclusive).
 * @param versionGetter A method to get the commit version from the file path.
 */
class BufferingLogDeletionIterator(
    underlying: Iterator[FileStatus],
    maxTimestamp: Long,
    maxVersion: Long,
    versionGetter: Path => Long) extends Iterator[FileStatus] {/**
 * Our output iterator
 */
  private val filesToDelete = new mutable.Queue[FileStatus]()
  /**
   * Our intermediate buffer which will buffer files as long as the last file requires a timestamp
   * adjustment.
   */
  private val maybeDeleteFiles = new mutable.ArrayBuffer[FileStatus]()
  private var lastFile: FileStatus = _
  private var hasNextCalled: Boolean = false

  private def init(): Unit = {
    if (underlying.hasNext) {
      lastFile = underlying.next()
      maybeDeleteFiles.append(lastFile)
    }
  }

  init()

  /** Whether the given file can be deleted based on the version and retention timestamp input. */
  private def shouldDeleteFile(file: FileStatus): Boolean = {
    file.getModificationTime <= maxTimestamp && versionGetter(file.getPath) <= maxVersion
  }

  /**
   * Files need a time adjustment if their timestamp isn't later than the lastFile.
   */
  private def needsTimeAdjustment(file: FileStatus): Boolean = {
    versionGetter(lastFile.getPath) < versionGetter(file.getPath) &&
      lastFile.getModificationTime >= file.getModificationTime
  }

  /**
   * Enqueue the files in the buffer if the last file is safe to delete. Clears the buffer.
   */
  private def flushBuffer(): Unit = {
    if (maybeDeleteFiles.lastOption.exists(shouldDeleteFile)) {
      filesToDelete ++= maybeDeleteFiles
    }
    maybeDeleteFiles.clear()
  }

  /**
   * Peeks at the next file in the iterator. Based on the next file we can have three
   * possible outcomes:
   * - The underlying iterator returned a file, which doesn't require timestamp adjustment. If
   *   the file in the buffer has expired, flush the buffer to our output queue.
   * - The underlying iterator returned a file, which requires timestamp adjustment. In this case,
   *   we add this file to the buffer and fetch the next file
   * - The underlying iterator is empty. In this case, we check the last file in the buffer. If
   *   it has expired, then flush the buffer to the output queue.
   * Once this method returns, the buffer is expected to have 1 file (last file of the
   * underlying iterator) unless the underlying iterator is fully consumed.
   */
  private def queueFilesInBuffer(): Unit = {
    var continueBuffering = true
    while (continueBuffering) {
      if (!underlying.hasNext) {
        flushBuffer()
        return
      }

      var currentFile = underlying.next()
      require(currentFile != null, "FileStatus iterator returned null")
      if (needsTimeAdjustment(currentFile)) {
        currentFile = new FileStatus(
          currentFile.getLen, currentFile.isDirectory, currentFile.getReplication,
          currentFile.getBlockSize, lastFile.getModificationTime + 1, currentFile.getPath)
        maybeDeleteFiles.append(currentFile)
      } else {
        flushBuffer()
        maybeDeleteFiles.append(currentFile)
        continueBuffering = false
      }
      lastFile = currentFile
    }
  }

  override def hasNext: Boolean = {
    hasNextCalled = true
    if (filesToDelete.isEmpty) queueFilesInBuffer()
    filesToDelete.nonEmpty
  }

  override def next(): FileStatus = {
    if (!hasNextCalled) throw new NoSuchElementException()
    hasNextCalled = false
    filesToDelete.dequeue()
  }
}
