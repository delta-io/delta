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

package io.delta.standalone.internal.util

import java.net.URI

import org.apache.hadoop.fs.Path

/** Helper for creating file names for specific commits / checkpoints. */
private[internal] object FileNames {

  val deltaFilePattern = "\\d+\\.json".r.pattern
  val checkpointFilePattern = "\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet".r.pattern

  /** Returns the path for a given delta file. */
  def deltaFile(path: Path, version: Long): Path = new Path(path, f"$version%020d.json")

  /** Returns the version for the given delta path. */
  def deltaVersion(path: Path): Long = path.getName.stripSuffix(".json").toLong

  /**
   * Returns the prefix of all checkpoint files for the given version.
   *
   * Intended for use with listFrom to get all files from this version onwards. The returned Path
   * will not exist as a file.
   */
  def checkpointPrefix(path: Path, version: Long): Path =
    new Path(path, f"$version%020d.checkpoint")

  /**
   * Returns the path for a singular checkpoint up to the given version.
   *
   * In a future protocol version this path will stop being written.
   */
  def checkpointFileSingular(path: Path, version: Long): Path =
    new Path(path, f"$version%020d.checkpoint.parquet")

  /**
   * Returns the paths for all parts of the checkpoint up to the given version.
   *
   * In a future protocol version we will write this path instead of checkpointFileSingular.
   *
   * Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
   * checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
   * lexicographic sorting.
   */
  def checkpointFileWithParts(path: Path, version: Long, numParts: Int): Seq[Path] = {
    Range(1, numParts + 1)
      .map(i => new Path(path, f"$version%020d.checkpoint.$i%010d.$numParts%010d.parquet"))
  }

  def numCheckpointParts(path: Path): Option[Int] = {
    val segments = path.getName.split("\\.")

    if (segments.size != 5) None else Some(segments(3).toInt)
  }

  def isCheckpointFile(path: Path): Boolean = checkpointFilePattern.matcher(path.getName).matches()

  def isDeltaFile(path: Path): Boolean = deltaFilePattern.matcher(path.getName).matches()

  def checkpointVersion(path: Path): Long = path.getName.split("\\.")(0).toLong

  /**
   * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
   * file type is seen. These unexpected files should be filtered out to ensure forward
   * compatibility in cases where new file types are added, but without an explicit protocol
   * upgrade.
   */
  def getFileVersion(path: Path): Long = {
    if (isCheckpointFile(path)) {
      checkpointVersion(path)
    } else if (isDeltaFile(path)) {
      deltaVersion(path)
    } else {
      // scalastyle:off throwerror
      throw new AssertionError(
        s"Unexpected file type found in transaction log: $path")
      // scalastyle:on throwerror
    }
  }

  /**
   * Returns the `child` path as an absolute path and resolves any escaped char sequences
   */
  def absolutePath(parentDir: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(parentDir, p)
    }
  }
}
