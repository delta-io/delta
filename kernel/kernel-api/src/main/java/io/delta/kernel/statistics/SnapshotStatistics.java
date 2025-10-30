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
import java.util.Optional;

/** Provides statistics and metadata about a {@link Snapshot}. */
@Evolving
public interface SnapshotStatistics {

  /**
   * Determines the appropriate mode for writing a checksum file for this Snapshot.
   *
   * <p>The returned value can be passed to {@link Snapshot#writeChecksum} to write the checksum
   * file using the most efficient approach available:
   *
   * <ul>
   *   <li>{@link Optional#empty()} - Checksum already exists, no write needed
   *   <li>{@link Optional} of {@link Snapshot.ChecksumWriteMode#SIMPLE} - Fast write using
   *       in-memory CRC info
   *   <li>{@link Optional} of {@link Snapshot.ChecksumWriteMode#FULL} - Requires log replay to
   *       compute CRC info
   * </ul>
   *
   * @return the recommended checksum write mode, or empty if checksum already exists
   */
  Optional<Snapshot.ChecksumWriteMode> getChecksumWriteMode();

  // TODO: getNumUnpublishedCatalogCommits
  // TODO: getNumDeltasSinceLastCheckpoint
  // TODO: getCheckpointInterval
}
