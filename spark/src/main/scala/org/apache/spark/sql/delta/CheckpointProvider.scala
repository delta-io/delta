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

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Represents basic information about a checkpoint.
 * This is the info we always can know about a checkpoint, without doing any additional I/O.
 */
trait UninitializedCheckpointProvider {

  /** True if the checkpoint provider is empty (does not refer to a valid checkpoint) */
  def isEmpty: Boolean = version < 0

  /** Checkpoint version */
  def version: Long

  /**
   * Top level files that represents this checkpoint.
   * These files could be reused again to initialize the [[CheckpointProvider]].
   */
  def topLevelFiles: Seq[FileStatus]
}

/**
 * A trait which provides information about a checkpoint to the Snapshot.
 */
trait CheckpointProvider extends UninitializedCheckpointProvider {

  /** Effective size of checkpoint across all files */
  def effectiveCheckpointSizeInBytes(): Long

  /**
   * List of different file indexes which could help derive full state-reconstruction
   * for the checkpoint.
   */
  def allActionsFileIndexes(): Seq[DeltaLogFileIndex]
}

object CheckpointProvider extends DeltaLogging {

  private[delta] def isV2CheckpointEnabled(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(V2CheckpointTableFeature)

  /**
   * Returns whether V2 Checkpoints are enabled or not.
   * This means an underlying checkpoint in this table could be a V2Checkpoint with sidecar files.
   */
  def isV2CheckpointEnabled(snapshotDescriptor: SnapshotDescriptor): Boolean =
    isV2CheckpointEnabled(snapshotDescriptor.protocol)
}

/**
 * An implementation of [[CheckpointProvider]] where the information about checkpoint files
 * (i.e. Seq[FileStatus]) is already known in advance.
 *
 * @param topLevelFiles - file statuses that describes the checkpoint
 * @param lastCheckpointInfoOpt - optional [[LastCheckpointInfo]] corresponding to this checkpoint.
 *                                This comes from _last_checkpoint file
 */
case class PreloadedCheckpointProvider(
    override val topLevelFiles: Seq[FileStatus],
    lastCheckpointInfoOpt: Option[LastCheckpointInfo])
  extends CheckpointProvider
  with DeltaLogging {

  require(topLevelFiles.nonEmpty, "There should be atleast 1 checkpoint file")
  private lazy val fileIndex =
    DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, topLevelFiles).get

  override def version: Long = checkpointVersion(topLevelFiles.head)

  override def effectiveCheckpointSizeInBytes(): Long = fileIndex.sizeInBytes

  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] = Seq(fileIndex)
}

/**
 * An implementation for [[CheckpointProvider]] which could be used to represent a scenario when
 * checkpoint doesn't exist. This helps us simplify the code by making
 * [[LogSegment.checkpointProvider]] as non-optional.
 *
 * The [[CheckpointProvider.isEmpty]] method returns true for [[EmptyCheckpointProvider]]. Also
 * version is returned as -1.
 * For a real checkpoint, this will be returned true and version will be >= 0.
 */
object EmptyCheckpointProvider extends CheckpointProvider {
  override def version: Long = -1
  override def topLevelFiles: Seq[FileStatus] = Nil
  override def effectiveCheckpointSizeInBytes(): Long = 0L
  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] = Nil
}
