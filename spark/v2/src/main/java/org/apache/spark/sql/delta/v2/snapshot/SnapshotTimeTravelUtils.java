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
