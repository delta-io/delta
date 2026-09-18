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

/** Usage logs emitted by the AMT (`adaptiveMetadata-preview`) code paths. */
object AMTUsageLogs {
  /** Common prefix for all AMT usage logs. */
  val PREFIX = "delta.amt"

  /**
   * Usage log emitted when [[Snapshot.lastManifestCommitOpt]] reads `CommitInfo` as a fallback.
   */
  val LAST_MANIFEST_COMMIT_READ_FROM_COMMIT_INFO =
    s"$PREFIX.lastManifestCommit.readFromCommitInfo"

  /** Usage log emitted when a losing full AMT OPTIMIZE checkpoint is retried. */
  val CHECKPOINT_FULL_REGENERATE_RETRY = s"$PREFIX.checkpoint.fullRegenerateRetry"

  // ////////////////////// Invariant check usage log suffixes //////////////////////

  // Prefix for all of them is "delta.assert."

  // A manifest_info contains both live files and tombstones.
  val ALERT_MIXED_LEAF_CONTENT = "amt.mixedLeafContent"

  // A leaf without live files gains new manifest deletion vector positions.
  val ALERT_MDV_WITHOUT_LIVE_FILES = "amt.commit.mdvWithoutLiveFiles"

  // A removed file without a back reference remains in the live set.
  // This usually happens when the same (file, DV) is removed and added in one commit.
  val ALERT_NO_BACKREF_REMOVE_STILL_LIVE =
    "amt.commit.noBackrefRemoveStillLive"

  // A removed file without a back reference has no originating add action in the root or
  // intermediate commits.
  val ALERT_NO_BACKREF_REMOVE_MISSING_ADD =
    "amt.commit.noBackrefRemoveMissingAdd"

  // A data-changing commit re-adds an already-live file.
  val ALERT_DATA_CHANGE_READD = "amt.commit.dataChangeReAdd"

  // A dataChange=false compaction with remove also re-adds an already-live file.
  val ALERT_COMPACTION_READD = "amt.commit.compactionReAdd"

  // A metadata refresh dataChange=false commit introduces a new file.
  val ALERT_METADATA_REFRESH_DATA_CHANGE_FALSE_ADDS_NEW_FILE =
    "amt.commit.metadataRefreshDataChangeFalseAddsNewFile"

  // A leaf entry has an unknown tracking status.
  val ALERT_UNEXPECTED_LEAF_TRACKING_STATUS =
    "amt.unexpectedLeafTrackingStatus"

  // A manifest deletion vector has inconsistent bytes and cardinality.
  val ALERT_MALFORMED_MANIFEST_DV = "amt.malformedManifestDV"

  // Alert raised when a base-preserving winning commit's Add/Remove file carries a new commit
  // sequence number (defaultRowCommitVersion newer than the losing full checkpoint's version)
  // yet a non-empty back reference into the base tree -- contradictory signals.
  val ALERT_FILE_CONTAINS_NEW_SEQ_NUMBERS_BUT_NON_EMPTY_BACKREFERENCE =
    "amt.fileContainsNewSeqNumbersButNonEmptyBackreference"
}
