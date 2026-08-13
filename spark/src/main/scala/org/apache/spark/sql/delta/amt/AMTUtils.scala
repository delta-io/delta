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

import org.apache.spark.sql.delta.{CurrentTransactionInfo, WinningCommitSummary}
import org.apache.spark.sql.delta.actions.LastManifestCommit
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
            contentRootVersion = winningAMTCheckpoint.version))))
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
