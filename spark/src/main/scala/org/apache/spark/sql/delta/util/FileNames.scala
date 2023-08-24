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

import java.util.UUID

import org.apache.hadoop.fs.{FileStatus, Path}

/** Helper for creating file names for specific commits / checkpoints. */
object FileNames {

  val deltaFileRegex = raw"(\d+)\.json".r
  val checksumFileRegex = raw"(\d+)\.crc".r
  val checkpointFileRegex = raw"(\d+)\.checkpoint(\.(\d+)\.(\d+))?\.parquet".r

  val deltaFilePattern = deltaFileRegex.pattern
  val checksumFilePattern = checksumFileRegex.pattern
  val checkpointFilePattern = checkpointFileRegex.pattern

  /** Returns the delta (json format) path for a given delta file. */
  def deltaFile(path: Path, version: Long): Path = new Path(path, f"$version%020d.json")

  /** Returns the path for a given sample file */
  def sampleFile(path: Path, version: Long): Path = new Path(path, f"$version%020d")

  /** Returns the path to the checksum file for the given version. */
  def checksumFile(path: Path, version: Long): Path = new Path(path, f"$version%020d.crc")

  /** Returns the version for the given delta path. */
  def deltaVersion(path: Path): Long = path.getName.split("\\.")(0).toLong
  def deltaVersion(file: FileStatus): Long = deltaVersion(file.getPath)

  /** Returns the version for the given checksum file. */
  def checksumVersion(path: Path): Long = path.getName.stripSuffix(".crc").toLong
  def checksumVersion(file: FileStatus): Long = checksumVersion(file.getPath)

  /**
   * Returns the prefix of all delta log files for the given version.
   *
   * Intended for use with listFrom to get all files from this version onwards. The returned Path
   * will not exist as a file.
   */
  def listingPrefix(path: Path, version: Long): Path = new Path(path, f"$version%020d.")

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
  def isCheckpointFile(file: FileStatus): Boolean = isCheckpointFile(file.getPath)

  def isDeltaFile(path: Path): Boolean = deltaFilePattern.matcher(path.getName).matches()
  def isDeltaFile(file: FileStatus): Boolean = isDeltaFile(file.getPath)

  def isChecksumFile(path: Path): Boolean = checksumFilePattern.matcher(path.getName).matches()
  def isChecksumFile(file: FileStatus): Boolean = isChecksumFile(file.getPath)

  def checkpointVersion(path: Path): Long = path.getName.split("\\.")(0).toLong
  def checkpointVersion(file: FileStatus): Long = checkpointVersion(file.getPath)

  /**
   * Get the version of the checkpoint, checksum or delta file. Returns None if an unexpected
   * file type is seen.
   */
  def getFileVersionOpt(path: Path): Option[Long] = path match {
    case DeltaFile(_, version) => Some(version)
    case ChecksumFile(_, version) => Some(version)
    case CheckpointFile(_, version) => Some(version)
    case _ => None
  }

  /**
   * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
   * file type is seen. These unexpected files should be filtered out to ensure forward
   * compatibility in cases where new file types are added, but without an explicit protocol
   * upgrade.
   */
  def getFileVersion(path: Path): Long = {
    getFileVersionOpt(path).getOrElse {
      // scalastyle:off throwerror
      throw new AssertionError(
        s"Unexpected file type found in transaction log: $path")
      // scalastyle:on throwerror
    }
  }
  def getFileVersion(file: FileStatus): Long = getFileVersion(file.getPath)

  object DeltaFile {
    def unapply(f: FileStatus): Option[(FileStatus, Long)] =
      unapply(f.getPath).map { case (_, version) => (f, version) }
    def unapply(path: Path): Option[(Path, Long)] = {
      deltaFileRegex.unapplySeq(path.getName).map(path -> _.head.toLong)
    }
  }
  object ChecksumFile {
    def unapply(f: FileStatus): Option[(FileStatus, Long)] =
      unapply(f.getPath).map { case (_, version) => (f, version) }
    def unapply(path: Path): Option[(Path, Long)] =
      checksumFileRegex.unapplySeq(path.getName).map(path -> _.head.toLong)
  }
  object CheckpointFile {
    def unapply(f: FileStatus): Option[(FileStatus, Long)] =
      unapply(f.getPath).map { case (_, version) => (f, version) }
    def unapply(path: Path): Option[(Path, Long)] = {
      checkpointFileRegex.unapplySeq(path.getName).map(path -> _.head.toLong)
    }
  }

  object FileType extends Enumeration {
    val DELTA, CHECKPOINT, CHECKSUM, OTHER = Value
  }


  /** File path for a new V2 Checkpoint Json file */
  def newV2CheckpointJsonFile(path: Path, version: Long): Path =
    new Path(path, f"$version%020d.checkpoint.${UUID.randomUUID.toString}.json")

  /** File path for a new V2 Checkpoint Parquet file */
  def newV2CheckpointParquetFile(path: Path, version: Long): Path =
    new Path(path, f"$version%020d.checkpoint.${UUID.randomUUID.toString}.parquet")

  /** File path for a V2 Checkpoint's Sidecar file */
  def newV2CheckpointSidecarFile(
      logPath: Path,
      version: Long,
      numParts: Int,
      currentPart: Int): Path = {
    val basePath = sidecarDirPath(logPath)
    val uuid = UUID.randomUUID.toString
    new Path(basePath, f"$version%020d.checkpoint.$currentPart%010d.$numParts%010d.$uuid.parquet")
  }

  val SIDECAR_SUBDIR = "_sidecars"
  /** Returns path to the sidecar directory */
  def sidecarDirPath(logPath: Path): Path = new Path(logPath, SIDECAR_SUBDIR)
}
