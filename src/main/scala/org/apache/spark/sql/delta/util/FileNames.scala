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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.hadoop.fs.{FileStatus, Path}

/** Helper for creating file names for specific commits / checkpoints. */
object FileNames {

  val deltaFilePattern = "\\d+\\.json".r.pattern
  val checksumFilePattern = "\\d+\\.crc".r.pattern
  val checkpointFilePattern = "\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet".r.pattern

  /** Returns the path for a given delta file. */
  def deltaFile(path: Path, version: Long): Path = new Path(path, f"$version%020d.json")

  /** Returns the path for a given sample file */
  def sampleFile(path: Path, version: Long): Path = new Path(path, f"$version%020d")

  /** Returns the path to the checksum file for the given version. */
  def checksumFile(path: Path, version: Long): Path = new Path(path, f"$version%020d.crc")

  /** Returns the version for the given delta path. */
  def deltaVersion(path: Path): Long = path.getName.stripSuffix(".json").toLong

  /** Returns the version for the given checksum file. */
  def checksumVersion(path: Path): Long = path.getName.stripSuffix(".crc").toLong

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

  def isChecksumFile(path: Path): Boolean = checksumFilePattern.matcher(path.getName).matches()

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
    } else if (isChecksumFile(path)) {
      checksumVersion(path)
    } else {
      // scalastyle:off throwerror
      throw new AssertionError(
        s"Unexpected file type found in transaction log: $path")
      // scalastyle:on throwerror
    }
  }
}
