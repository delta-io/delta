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

package org.apache.spark.sql.delta.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.util.FileNames.{CheckpointFile, DeltaFile}
import org.apache.hadoop.fs.FileStatus

/**
 * An iterator that groups same types of files by version.
 * Note that this class could handle only Checkpoints and Delta files.
 * For example for an input iterator:
 * - 11.checkpoint.0.1.parquet
 * - 11.checkpoint.1.1.parquet
 * - 11.json
 * - 12.checkpoint.parquet
 * - 12.json
 * - 13.json
 * - 14.json
 * - 15.checkpoint.0.1.parquet
 * - 15.checkpoint.1.1.parquet
 * - 15.checkpoint.<uuid>.parquet
 * - 15.json
 *  This will return:
 *  - (11, Seq(11.checkpoint.0.1.parquet, 11.checkpoint.1.1.parquet, 11.json))
 *  - (12, Seq(12.checkpoint.parquet, 12.json))
 *  - (13, Seq(13.json))
 *  - (14, Seq(14.json))
 *  - (15, Seq(15.checkpoint.0.1.parquet, 15.checkpoint.1.1.parquet, 15.checkpoint.<uuid>.parquet,
 *             15.json))
 */
class DeltaLogGroupingIterator(
  checkpointAndDeltas: Iterator[FileStatus]) extends Iterator[(Long, ArrayBuffer[FileStatus])] {

  private val bufferedIterator = checkpointAndDeltas.buffered

  /**
   * Validates that the underlying file is a checkpoint/delta file and returns the corresponding
   * version.
   */
  private def getFileVersion(file: FileStatus): Long = {
    file match {
      case DeltaFile(_, version) => version
      case CheckpointFile(_, version) => version
      case _ =>
        throw new IllegalStateException(
          s"${file.getPath} is not a valid commit file / checkpoint file")
    }
  }

  override def hasNext: Boolean = bufferedIterator.hasNext

  override def next(): (Long, ArrayBuffer[FileStatus]) = {
    val first = bufferedIterator.next()
    val buffer = scala.collection.mutable.ArrayBuffer(first)
    val firstFileVersion = getFileVersion(first)
    while (bufferedIterator.headOption.exists(getFileVersion(_) == firstFileVersion)) {
      buffer += bufferedIterator.next()
    }
    firstFileVersion -> buffer
  }
}
