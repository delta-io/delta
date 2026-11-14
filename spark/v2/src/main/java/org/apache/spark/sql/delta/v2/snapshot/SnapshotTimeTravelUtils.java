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
package org.apache.spark.sql.delta.v2.snapshot;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException;

/** Shared time-travel operations for V2 snapshot managers. */
public final class SnapshotTimeTravelUtils {

  private SnapshotTimeTravelUtils() {}

  /**
   * Resolves the commit active at a timestamp using the loaded snapshot and any catalog commits.
   * Catalog commits participate in history resolution alongside commits available from the log.
   *
   * @param kernelEngine engine used to read the transaction log
   * @param snapshot loaded snapshot whose log and version bound the lookup
   * @param catalogCommits catalog-managed commits visible to the loaded snapshot; empty for a
   *     path-based table
   * @param timestampMillis timestamp to resolve, in milliseconds since the epoch
   * @param canReturnLastCommit whether a timestamp after the latest commit may return that commit
   * @param mustBeRecreatable whether the returned commit must be recreatable from available files
   * @param canReturnEarliestCommit whether a timestamp before history may return the earliest
   *     commit
   * @return the commit active at {@code timestampMillis}
   */
  public static DeltaHistoryManager.Commit getActiveCommitAtTime(
      Engine kernelEngine,
      SnapshotImpl snapshot,
      List<ParsedCatalogCommitData> catalogCommits,
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        catalogCommits);
  }

  /**
   * Validates a version against the loaded snapshot and the earliest available history. The minimum
   * catalog-commit version bounds the filesystem search for the earliest version; an empty list
   * performs the search without a catalog bound.
   *
   * <p>When {@code checkUpperBoundFirst} is true, an invalid upper bound is rejected before the
   * filesystem lookup and reports zero as the earliest version. When false, the filesystem lookup
   * happens first and an upper-bound error reports the actual earliest available version.
   *
   * @param kernelEngine engine used to read the transaction log
   * @param snapshot loaded snapshot whose log and latest version bound the validation
   * @param catalogCommits catalog-managed commits visible to the loaded snapshot; empty for a
   *     path-based table
   * @param version version to validate
   * @param mustBeRecreatable whether the version must be recreatable from available files
   * @param allowOutOfRange whether versions above the loaded snapshot are allowed
   * @param checkUpperBoundFirst whether to reject an invalid upper bound before filesystem access
   * @throws VersionNotFoundException if the requested version is unavailable
   */
  public static void checkVersionExists(
      Engine kernelEngine,
      SnapshotImpl snapshot,
      List<ParsedCatalogCommitData> catalogCommits,
      long version,
      boolean mustBeRecreatable,
      boolean allowOutOfRange,
      boolean checkUpperBoundFirst) {
    long latest = snapshot.getVersion();
    if (checkUpperBoundFirst && version > latest && !allowOutOfRange) {
      throw new VersionNotFoundException(version, 0 /* earliest */, latest);
    }

    Optional<Long> earliestCatalogCommitVersion =
        catalogCommits.stream().map(ParsedCatalogCommitData::getVersion).min(Long::compare);
    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), earliestCatalogCommitVersion)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine, snapshot.getLogPath(), earliestCatalogCommitVersion);

    if (version < earliest || (!checkUpperBoundFirst && version > latest && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }
}
