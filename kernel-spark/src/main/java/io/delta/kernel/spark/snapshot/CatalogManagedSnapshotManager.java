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
package io.delta.kernel.spark.snapshot;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.defaults.catalog.CatalogWithManagedCommits;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.annotation.Experimental;

/** SnapshotManager that loads Delta snapshots using catalog-managed commits (CCv2). */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager {

  private final CatalogWithManagedCommits catalog;
  private final String tableId;
  private final String tablePath;
  private final Engine kernelEngine;
  private final Path logPath;

  public CatalogManagedSnapshotManager(
      CatalogWithManagedCommits catalog, String tableId, String tablePath, Configuration hadoopConf) {
    this.catalog = requireNonNull(catalog, "catalog is null");
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.kernelEngine = DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null"));
    this.logPath = new Path(new Path(tablePath), "_delta_log");
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    return loadSnapshotInternal(Optional.empty());
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    if (version < 0) {
      throw new IllegalArgumentException("version must be non-negative");
    }
    return loadSnapshotInternal(Optional.of(version));
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>());
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), Optional.empty())
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine, snapshot.getLogPath(), Optional.empty());

    long latest = snapshot.getVersion();
    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    CatalogWithManagedCommits.GetCommitsResult result =
        fetchCommitRange(startVersion, endVersion);
    List<ParsedLogData> logData = toParsedLogData(result.getCommits());

    CommitRangeBuilder builder = TableManager.loadCommitRange(tablePath);
    builder = builder.withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion));
    endVersion.ifPresent(
        end -> builder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(end)));

    return builder.withLogData(logData).build(engine);
  }

  private Snapshot loadSnapshotInternal(Optional<Long> versionOpt) {
    CatalogWithManagedCommits.GetCommitsResult result = fetchCommitRange(0L, versionOpt);
    List<ParsedLogData> logData = toParsedLogData(result.getCommits());

    if (versionOpt.isPresent()) {
      validateRequestedVersionPresent(versionOpt.get(), result.getCommits());
    }

    SnapshotBuilder builder =
        TableManager.loadSnapshot(tablePath)
            .withLogData(logData)
            .withMaxCatalogVersion(result.getLatestTableVersion());

    if (versionOpt.isPresent()) {
      builder = builder.atVersion(versionOpt.get());
    }

    return builder.build(kernelEngine);
  }

  private CatalogWithManagedCommits.GetCommitsResult fetchCommitRange(
      long startVersion, Optional<Long> endVersion) {
    long nextStart = startVersion;
    long latestTableVersion = -1L;
    List<CatalogWithManagedCommits.CommitEntry> allCommits = new ArrayList<>();

    while (true) {
      CatalogWithManagedCommits.GetCommitsResult page =
          catalog.getCommits(tableId, tablePath, nextStart, endVersion);
      List<CatalogWithManagedCommits.CommitEntry> commits = page.getCommits();
      latestTableVersion = page.getLatestTableVersion();
      allCommits.addAll(commits);

      if (commits.isEmpty()) {
        break;
      }
      long maxVersionInPage =
          commits.stream().mapToLong(CatalogWithManagedCommits.CommitEntry::getVersion).max().getAsLong();
      if ((endVersion.isPresent() && maxVersionInPage >= endVersion.get())
          || maxVersionInPage >= latestTableVersion) {
        break;
      }
      nextStart = maxVersionInPage + 1;
    }

    validateContiguous(allCommits, startVersion);
    if (latestTableVersion < 0) {
      latestTableVersion =
          allCommits.stream()
              .mapToLong(CatalogWithManagedCommits.CommitEntry::getVersion)
              .max()
              .orElse(startVersion - 1);
    }
    return new CatalogWithManagedCommits.GetCommitsResult(allCommits, latestTableVersion);
  }

  private List<ParsedLogData> toParsedLogData(List<CatalogWithManagedCommits.CommitEntry> commits) {
    return commits.stream()
        .sorted(Comparator.comparingLong(CatalogWithManagedCommits.CommitEntry::getVersion))
        .map(
            commit ->
                ParsedCatalogCommitData.forFileStatus(
                    io.delta.kernel.utils.FileStatus.of(
                        new Path(logPath, commit.getFileName()).toString(),
                        commit.getFileSize(),
                        commit.getFileModificationTime())))
        .collect(Collectors.toList());
  }

  private void validateRequestedVersionPresent(
      long requestedVersion, List<CatalogWithManagedCommits.CommitEntry> commits) {
    boolean exists =
        commits.stream().anyMatch(entry -> entry.getVersion() == requestedVersion);
    if (!exists) {
      throw new IllegalArgumentException(
          "Requested version "
              + requestedVersion
              + " not present in catalog-managed commit list for table "
              + tableId);
    }
  }

  private void validateContiguous(
      List<CatalogWithManagedCommits.CommitEntry> commits, long expectedStartVersion) {
    if (commits.isEmpty()) {
      return;
    }
    commits.sort(Comparator.comparingLong(CatalogWithManagedCommits.CommitEntry::getVersion));
    long prev = commits.get(0).getVersion();
    if (prev != expectedStartVersion) {
      throw new IllegalStateException(
          String.format(
              "Expected commits to start at %d but saw %d", expectedStartVersion, prev));
    }
    for (int i = 1; i < commits.size(); i++) {
      long current = commits.get(i).getVersion();
      if (current != prev + 1) {
        throw new IllegalStateException(
            String.format(
                "Detected gap in ratified commits between %d and %d", prev, current));
      }
      prev = current;
    }
  }
}
