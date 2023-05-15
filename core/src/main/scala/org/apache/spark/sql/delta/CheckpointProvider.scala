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

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * A trait which provides information about a checkpoint to the Snapshot.
 */
trait CheckpointProvider {

  /** Checkpoint version */
  def version: Long

  /** files in the underlying checkpoint */
  def checkpointFiles: Seq[FileStatus]

  /** [[CheckpointMetaData]] representing the checkpoint */
  def checkpointMetadata: CheckpointMetaData

  /** Effective size of checkpoint across all files */
  def effectiveCheckpointSizeInBytes(): Long

  /**
   * List of different file indexes which could help derive full state-reconstruction
   * for the checkpoint.
   */
  def allActionsFileIndexes(): Seq[DeltaLogFileIndex]
}

/**
 * An implementation of [[CheckpointProvider]] where the information about checkpoint files
 * (i.e. Seq[FileStatus]) is already known in advance.
 *
 * @param checkpointFiles - file statuses for the checkpoint
 * @param checkpointMetadataOpt - optional checkpoint metadata for the checkpoint.
 *                              If this is passed, the provider will use it instead of deriving the
 *                              [[CheckpointMetaData]] from the file list.
 */
case class PreloadedCheckpointProvider(
  override val checkpointFiles: Seq[FileStatus],
  checkpointMetadataOpt: Option[CheckpointMetaData]
) extends CheckpointProvider with DeltaLogging {

  require(checkpointFiles.nonEmpty, "There should be atleast 1 checkpoint file")
  private lazy val fileIndex =
    DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, checkpointFiles).get

  override def version: Long = checkpointMetadata.version

  override def checkpointMetadata: CheckpointMetaData = {
    checkpointMetadataOpt.getOrElse(CheckpointMetaData.fromFiles(checkpointFiles))
  }

  override def effectiveCheckpointSizeInBytes(): Long = fileIndex.sizeInBytes

  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] = Seq(fileIndex)
}
