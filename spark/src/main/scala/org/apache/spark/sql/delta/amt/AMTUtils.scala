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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CurrentTransactionInfo, SnapshotDescriptor, WinningCommitSummary}
import org.apache.spark.sql.delta.actions.{LastManifestCommit, Metadata, Protocol}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Path helpers for AMT (Adaptive Metadata Tree) manifest files.
 *
 * AMT manifest `location` / `contentRoot.path` fields follow the Iceberg V4 manifest path rules:
 * they hold raw (non-URL-encoded) literal path strings, stored relative to the table root when the
 * file lives under it and resolved back by string concatenation (`tableRoot + "/" + relative`).
 * This differs from Delta's `AddFile.path`, which is URL-encoded.
 */
object AMTUtils {
  /**
   * Whether AMT (Adaptive Metadata Tree) writes are enabled for a table with this `protocol` and
   * `metadata`.
   */
  def amtEnabled(metadata: Metadata, protocol: Protocol): Boolean =
    protocol.isFeatureSupported(AdaptiveMetadataTableFeature)

  /** Whether AMT writes are enabled for `snapshot`. */
  def amtEnabled(snapshot: SnapshotDescriptor): Boolean =
    amtEnabled(snapshot.metadata, snapshot.protocol)

  private val PathSeparator = "/"

  /**
   * Returns true if the location contains a URI scheme, per RFC 3986 section 3.1.
   * https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
   */
  private[amt] def hasScheme(location: String): Boolean = {
    var i = 0
    while (i < location.length) {
      val ch = location.charAt(i)
      if (ch == ':') {
        return i > 0
      }
      if (!isSchemeChar(ch, i)) {
        return false
      }
      i += 1
    }
    false
  }

  private def isSchemeChar(ch: Char, position: Int): Boolean = {
    (ch >= 'a' && ch <= 'z') ||
      (ch >= 'A' && ch <= 'Z') ||
      (position > 0 && ((ch >= '0' && ch <= '9') || ch == '+' || ch == '-' || ch == '.'))
  }

  /**
   * Returns true if a location is absolute.
   * NOTE: This is not the same implementation as Hadoop [[Path.isAbsolute]].
   */
  def isAbsoluteLocation(location: String): Boolean = {
    hasScheme(location) || location.startsWith(PathSeparator)
  }

  /**
   * Relativizes a location against a table location. A trailing slash on `tableLocation` is
   * ignored. If `location` starts with the normalized table location immediately followed by `/`,
   * the prefix and separator are removed. Otherwise, `location` is returned as-is.
   * This is a lightweight string manipulation compared to [[DeltaFileOperations.tryRelativizePath]]
   * and should be preferred in hot paths.
   *
   * Because the relativization is prefix matching based, callers are expected to pass locations in
   * the same format and encoding. No such checks are performed here.
   */
  def relativizeLocation(tableLocation: String, location: String): String = {
    // Strip trailing slash from tableLocation if present.
    val normalizedTableLocation =
      if (tableLocation.length > PathSeparator.length && tableLocation.endsWith(PathSeparator)) {
        tableLocation.dropRight(PathSeparator.length)
      } else {
        tableLocation
      }

    // Prefix matching based location relativization
    val prefixLength = normalizedTableLocation.length
    if (location.length > prefixLength &&
        location.startsWith(PathSeparator, prefixLength) &&
        location.startsWith(normalizedTableLocation)) {
      location.substring(prefixLength + PathSeparator.length)
    } else {
      location
    }
  }

  /**
   * Relativizes an AMT manifest file `path` against `tableRoot`, returning the raw
   * (non-URL-encoded) string to store in a manifest `location` / `contentRoot.path`. Paths under
   * the table root become relative; paths elsewhere are returned absolute.
   */
  def relativizeManifestPathToTableRoot(fs: FileSystem, tableRoot: Path, path: Path): String =
    DeltaFileOperations.tryRelativizePath(fs, tableRoot, path).toString

  /**
   * Resolves a manifest `location` / `contentRoot.path` back to an absolute [[Path]] against
   * `tableRoot`: a location carrying a URI scheme (or an absolute path) is used as-is; otherwise it
   * is a raw path relative to the table root and is joined onto it. The location is a literal
   * (non-URL-encoded) string.
   */
  def absolutePathForManifestFile(tableRoot: Path, location: String): Path = {
    val child = new Path(location)
    if (child.toUri.getScheme != null || child.isAbsolute) child
    else new Path(tableRoot, child)
  }

  /** Returns a copy of the passed-in current transaction info with `lastManifestCommit` updated. */
  def updateCurrentTransactionInfo(
      currentTransactionInfo: CurrentTransactionInfo,
      newLastManifestCommit: LastManifestCommit): CurrentTransactionInfo = {
    currentTransactionInfo.copy(
      commitInfo = currentTransactionInfo.commitInfo.map(_.copy(
        lastManifestCommit = Some(newLastManifestCommit)))
    )
  }

  /**
   * Returns a copy of the passed-in current transaction info folded onto the winning commit, ready
   * for the next commit attempt.
   */
  def updateCurrentTransactionInfo(
      currentTransactionInfo: CurrentTransactionInfo,
      winningCommitSummary: WinningCommitSummary): CurrentTransactionInfo = {
    // If the winning commit emitted an inline AMT checkpoint, it is now the latest checkpoint
    // before the next commit attempt.
    val withWinnerTree = winningCommitSummary.amtCheckpoint.map { winningAMTCheckpoint =>
      currentTransactionInfo.copy(
        // If the winning commit emitted an inline AMT checkpoint, it is now the latest checkpoint
        // before the next commit attempt.
        preCommitLatestAMTCheckpointOpt = Some(winningAMTCheckpoint),
        // Update the current commitInfo to reflect the winning manifest commit.
        commitInfo = currentTransactionInfo.commitInfo.map(_.copy(
          lastManifestCommit = Some(LastManifestCommit(
            version = winningCommitSummary.commitVersion,
            contentRootVersion = winningAMTCheckpoint.contentRoot.version))))
      )
    }.getOrElse(currentTransactionInfo)
    // Clear `currentCommitAttemptAMTCheckpointOpt` because it is stale once we rebase.
    withWinnerTree.copy(currentCommitAttemptAMTCheckpointOpt = None)
  }

  // Serializes a Manifest Deletion Vector to the on-disk byte form carried in `manifest_info.dv`.
  private[amt] def serializeMdv(mdv: RoaringBitmapArray): Array[Byte] =
    mdv.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)

  // Deserializes a Manifest Deletion Vector previously written by [[serializeMdv]].
  private[amt] def deserializeMdv(bytes: Array[Byte]): RoaringBitmapArray =
    RoaringBitmapArray.readFrom(bytes)
}
