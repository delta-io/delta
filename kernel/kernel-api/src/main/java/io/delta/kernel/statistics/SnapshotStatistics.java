/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.statistics;

import io.delta.kernel.Snapshot;
import io.delta.kernel.annotation.Evolving;

/** Provides statistics and metadata about a {@link Snapshot}. */
@Evolving
public interface SnapshotStatistics {

  /** Indicates how a checksum file should be written for this Snapshot, if at all. */
  enum ChecksumWriteMode {
    /** Checksum file already exists at this version, no write needed. */
    NONE,

    /**
     * Checksum info is already loaded in this Snapshot and can be written cheaply. To write it,
     * call {@link Snapshot#writeChecksumSimple}
     */
    SIMPLE,

    /**
     * Checksum info is not loaded in this Snapshot and requires replaying the delta log since the
     * latest checksum (if present) to compute. To write it, call {@link Snapshot#writeChecksumFull}.
     */
    FULL
  }

  /**
   * Indicates how a checksum file should be written for this Snapshot, if at all.
   *
   * @return the recommended checksum write mode for this snapshot
   */
  ChecksumWriteMode getChecksumWriteMode();

  // TODO: getNumUnpublishedCatalogCommits
  // TODO: getNumDeltasSinceLastCheckpoint
  // TODO: getCheckpointInterval
}
