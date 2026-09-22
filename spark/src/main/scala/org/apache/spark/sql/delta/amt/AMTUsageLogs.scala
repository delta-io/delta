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
  val PREFIX = "delta.v4amt"

  /**
   * Usage log emitted when [[Snapshot.lastManifestCommitOpt]] reads `CommitInfo` as a fallback.
   */
  val LAST_MANIFEST_COMMIT_READ_FROM_COMMIT_INFO =
    s"$PREFIX.lastManifestCommit.readFromCommitInfo"

  /** Usage log emitted when a losing full AMT OPTIMIZE checkpoint is retried. */
  val CHECKPOINT_FULL_REGENERATE_RETRY = s"$PREFIX.checkpoint.fullRegenerateRetry"

  /** Usage log emitted for each AMT conflict-resolution round. */
  val CONFLICT_RESOLUTION_ROUND = s"$PREFIX.conflictResolutionRound"

  /** Usage log emitted when an AMT write fails unexpectedly. */
  val WRITE_FAILED = s"$PREFIX.writeFailed"

  // Alert raised when a base-preserving winning commit's Add/Remove file carries a new commit
  // sequence number (defaultRowCommitVersion newer than the losing full checkpoint's version)
  // yet a non-empty back reference into the base tree -- contradictory signals.
  val ALERT_SUFFIX_FILE_CONTAINS_NEW_SEQ_NUMBERS_BUT_NON_EMPTY_BACKREFERENCE =
    "v4amt.fileContainsNewSeqNumbersButNonEmptyBackreference"
}
