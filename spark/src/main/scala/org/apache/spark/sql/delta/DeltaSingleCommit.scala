/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}

/**
 * A trait representing a single Delta commit.
 */
trait DeltaSingleCommit {

  /** The version of this commit. */
  def version: Long

  /**
   * The persisted in-commit timestamp when present, otherwise the commit log file's raw
   * modification time, in milliseconds since the Unix epoch.
   *
   * This is a per-commit value and does not depend on the table's current ICT configuration.
   * File modification times are not adjusted to be monotonic across versions, unlike the
   * timestamps used for history and time travel.
   */
  def timestamp: Long

  /**
   * Creates a rewindable iterator over this commit's actions.
   *
   * The caller owns the returned iterator and must close it after use.
   *
   * @param maxInMemoryFileSizeBytes threshold for caching a commit's actions in memory for rewinds.
   *   File-backed implementations stream when the file is not smaller than this threshold.
   *   Implementations that already hold all actions in memory may ignore the threshold.
   */
  def getActionsIterator(
      maxInMemoryFileSizeBytes: Long = 0): ClosableIterator[Action] with SupportsRewinding[Action]
}
