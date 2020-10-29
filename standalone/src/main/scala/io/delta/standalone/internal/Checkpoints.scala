/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.io.FileNotFoundException

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import io.delta.standalone.internal.util.JsonUtils
import io.delta.standalone.internal.util.FileNames._

/**
 * Records information about a checkpoint.
 *
 * @param version the version of this checkpoint
 * @param size the number of actions in the checkpoint
 * @param parts the number of parts when the checkpoint has multiple parts. None if this is a
 *              singular checkpoint
 */
case class CheckpointMetaData(
    version: Long,
    size: Long,
    parts: Option[Int])

/**
 * A class to help with comparing checkpoints with each other, where we may have had concurrent
 * writers that checkpoint with different number of parts.
 */
case class CheckpointInstance(
    version: Long,
    numParts: Option[Int]) extends Ordered[CheckpointInstance] {

  /**
   * Due to lexicographic sorting, a version with more parts will appear after a version with
   * less parts during file listing. We use that logic here as well.
   */
  def isEarlierThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version < other.version ||
      (version == other.version && numParts.forall(_ < other.numParts.getOrElse(1)))
  }

  def isNotLaterThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version <= other.version
  }

  def getCorrespondingFiles(path: Path): Seq[Path] = {
    assert(this != CheckpointInstance.MaxValue, "Can't get files for CheckpointVersion.MaxValue.")
    numParts match {
      case None => checkpointFileSingular(path, version) :: Nil
      case Some(parts) => checkpointFileWithParts(path, version, parts)
    }
  }

  override def compare(that: CheckpointInstance): Int = {
    if (version == that.version) {
      numParts.getOrElse(1) - that.numParts.getOrElse(1)
    } else {
      // we need to guard against overflow. We just can't return (this - that).toInt
      if (version - that.version < 0) -1 else 1
    }
  }
}

object CheckpointInstance {
  def apply(path: Path): CheckpointInstance = {
    CheckpointInstance(checkpointVersion(path), numCheckpointParts(path))
  }

  def apply(metadata: CheckpointMetaData): CheckpointInstance = {
    CheckpointInstance(metadata.version, metadata.parts)
  }

  val MaxValue: CheckpointInstance = CheckpointInstance(-1, None)
}

private[internal] trait Checkpoints {
  self: DeltaLogImpl =>

  /** The path to the file that holds metadata about the most recent checkpoint. */
  val LAST_CHECKPOINT = new Path(logPath, "_last_checkpoint")

  /** Returns information about the most recent checkpoint. */
  def lastCheckpoint: Option[CheckpointMetaData] = {
    loadMetadataFromFile(0)
  }

  /** Loads the checkpoint metadata from the _last_checkpoint file. */
  private def loadMetadataFromFile(tries: Int): Option[CheckpointMetaData] = {
    try {
      val checkpointMetadataJson = store.read(LAST_CHECKPOINT)
      val checkpointMetadata =
        JsonUtils.mapper.readValue[CheckpointMetaData](checkpointMetadataJson.head)
      Some(checkpointMetadata)
    } catch {
      case _: FileNotFoundException =>
        None
      case NonFatal(e) if tries < 3 =>
        // scalastyle:off println
        println(s"Failed to parse $LAST_CHECKPOINT. This may happen if there was an error " +
          "during read operation, or a file appears to be partial. Sleeping and trying again.", e)
        // scalastyle:on println

        Thread.sleep(1000)
        loadMetadataFromFile(tries + 1)
      case NonFatal(e) =>
        // scalastyle:off println
        println(s"$LAST_CHECKPOINT is corrupted. Will search the checkpoint files directly", e)
        // scalastyle:on println
        // Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
        // not atomic. We will try to list all files to find the latest checkpoint and restore
        // CheckpointMetaData from it.
        val verifiedCheckpoint = findLastCompleteCheckpoint(CheckpointInstance(-1L, None))
        verifiedCheckpoint.map(manuallyLoadCheckpoint)
    }
  }

  /** Loads the given checkpoint manually to come up with the CheckpointMetaData */
  private def manuallyLoadCheckpoint(cv: CheckpointInstance): CheckpointMetaData = {
    CheckpointMetaData(cv.version, -1L, cv.numParts)
  }

  /**
   * Finds the first verified, complete checkpoint before the given version.
   *
   * @param cv The CheckpointVersion to compare against
   */
  protected def findLastCompleteCheckpoint(cv: CheckpointInstance): Option[CheckpointInstance] = {
    var cur = math.max(cv.version, 0L)
    while (cur >= 0) {
      val checkpoints = store.listFrom(checkpointPrefix(logPath, math.max(0, cur - 1000)))
        .map(_.getPath)
        .filter(isCheckpointFile)
        .map(CheckpointInstance(_))
        .takeWhile(tv => (cur == 0 || tv.version <= cur) && tv.isEarlierThan(cv))
        .toArray
      val lastCheckpoint = getLatestCompleteCheckpointFromList(checkpoints, cv)
      if (lastCheckpoint.isDefined) {
        return lastCheckpoint
      } else {
        cur -= 1000
      }
    }
    None
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  protected def getLatestCompleteCheckpointFromList(
      instances: Array[CheckpointInstance],
      notLaterThan: CheckpointInstance): Option[CheckpointInstance] = {
    val complete = instances.filter(_.isNotLaterThan(notLaterThan)).groupBy(identity).filter {
      case (CheckpointInstance(_, None), inst) => inst.length == 1
      case (CheckpointInstance(_, Some(parts)), inst) => inst.length == parts
    }
    complete.keys.toArray.sorted.lastOption
  }
}
