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

  /** Usage log emitted after an async CommitInfo read is fired and completed. */
  val SNAPSHOT_DISCOVERY_READ_LAST_COMMIT_INFO_ASYNC =
    s"$PREFIX.snapshotDiscovery.readLastCommitInfoAsync"

  /** Usage log emitted when an AMT provider reads the root manifest for its leaf pointers. */
  val CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION =
    s"$PREFIX.checkpointProvider.initializeFromCheckpointAction"

  /** Usage log emitted after discovery reads and reconciles an AMT checkpoint provider. */
  val SNAPSHOT_DISCOVERY_UPDATE_CHECKPOINT_PROVIDER =
    s"$PREFIX.snapshotDiscovery.updateCheckpointProvider"

  /** Usage log emitted when a losing full AMT OPTIMIZE checkpoint is retried. */
  val CHECKPOINT_FULL_REGENERATE_RETRY = s"$PREFIX.checkpoint.fullRegenerateRetry"

  /** Usage log emitted for each AMT conflict-resolution round. */
  val CONFLICT_RESOLUTION_ROUND = s"$PREFIX.conflictResolutionRound"

  /** Usage log emitted when an AMT write fails unexpectedly. */
  val WRITE_FAILED = s"$PREFIX.writeFailed"

  // ////////////////////// Invariant check usage log suffixes //////////////////////

  /** Usage log emitted when a logging-only invariant check unexpectedly throws in production. */
  val EXCEPTIONS_THROWN_DUE_TO_LOGGING = s"$PREFIX.exceptionsThrownDueToLogging"

  // Prefix for all of them is "delta.assertions."

  // An AMT snapshot is missing the required non-compacted delta list.
  val SNAPSHOT_INVARIANT_MISSING_NON_COMPACTED_DELTAS =
    "amt.snapshotInvariant.missingNonCompactedDeltas"

  // Trimming an AMT log segment requires a non-compacted delta list.
  val TRIMMING_MISSING_NON_COMPACTED_DELTAS = "amt.trimming.missingNonCompactedDeltas"

  // Selecting log commits for an incremental AMT write requires a non-compacted delta list.
  val INCREMENTAL_LOG_SELECTION_MISSING_NON_COMPACTED_DELTAS =
    "amt.incrementalLogSelection.missingNonCompactedDeltas"

  // An AMT checkpoint is newer than the content root described by the last manifest commit.
  val CHECKPOINT_AHEAD_OF_LAST_MANIFEST_COMMIT_CONTENT_ROOT =
    "amt.reconciliation.checkpointAheadOfLastManifestCommitContentRoot"

  // An AMT checkpoint has no corroborating last manifest commit.
  val MISSING_LAST_MANIFEST_COMMIT = "amt.reconciliation.missingLastManifestCommit"

  // The referenced manifest commit contains no Checkpoint action.
  val MISSING_CHECKPOINT_ACTION = "amt.manifestCommitRead.missingCheckpointAction"

  // An AMT checkpoint is newer than the snapshot being initialized.
  val CHECKPOINT_EXCEEDS_SNAPSHOT_VERSION = "amt.snapshotInvariant.checkpointExceedsSnapshotVersion"

  // An AMT checkpoint is present on a snapshot with AMT disabled.
  val AMT_CHECKPOINT_PROVIDER_WITH_FEATURE_DISABLED =
    "amt.snapshotInvariant.amtCheckpointProviderWithFeatureDisabled"

  // An AMT snapshot has no delta at its checkpoint version.
  val MISSING_DELTA_AT_CHECKPOINT_VERSION = "amt.snapshotInvariant.missingDeltaAtCheckpointVersion"

  // An AMT checkpoint is older than the content root described by the last manifest commit.
  val STALE_CHECKPOINT_PROVIDER = "amt.snapshotInvariant.staleCheckpointProvider"

  // A snapshot has a last manifest commit but no AMT checkpoint provider.
  val MISSING_CHECKPOINT_PROVIDER = "amt.snapshotInvariant.missingCheckpointProvider"

  // The delta at the checkpoint has a different version from the checkpoint provider.
  val DELTA_AT_CHECKPOINT_VERSION_MISMATCH =
    "amt.logSegmentInvariant.deltaAtCheckpointVersionMismatch"

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

  // A leaf's manifest_info per status file counts do not sum to its record_count.
  val ALERT_INCONSISTENT_MANIFEST_INFO_FILE_COUNTS = "amt.inconsistentManifestInfoFileCounts"

  // An inline manifest commit's tree does not reconstruct the commit's own file actions.
  val ALERT_COMMIT_RECONSTRUCTION_MISMATCH = "amt.commit.reconstructionMismatch"

  // An inline manifest commit's CommitInfo carries no commit-level dataChange.
  val ALERT_COMMIT_MISSING_DATA_CHANGE = "amt.commit.missingDataChange"

  // Alert raised when a base-preserving winning commit's Add/Remove file carries a new commit
  // sequence number (defaultRowCommitVersion newer than the losing full checkpoint's version)
  // yet a non-empty back reference into the base tree -- contradictory signals.
  val ALERT_FILE_CONTAINS_NEW_SEQ_NUMBERS_BUT_NON_EMPTY_BACKREFERENCE =
    "amt.fileContainsNewSeqNumbersButNonEmptyBackreference"
}
