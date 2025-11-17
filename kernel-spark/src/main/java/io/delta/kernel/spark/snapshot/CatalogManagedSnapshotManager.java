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
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.unity.UnityCatalogClientFactory;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DeltaSnapshotManager} implementation backed by Unity Catalog commit APIs (CCv2).
 *
 * <p>This manager defers snapshot reconstruction and commit enumeration to {@link
 * UCCatalogManagedClient}, ensuring staged-but-unpublished commits returned by the catalog are
 * honoured for snapshot, time-travel and streaming queries.
 */
public final class CatalogManagedSnapshotManager implements DeltaSnapshotManager {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final String tablePath;
  private final String ucTableId;
  private final UCClient ucClient;
  private final Engine kernelEngine;
  private final AtomicReference<Snapshot> latestSnapshotRef = new AtomicReference<>();

  CatalogManagedSnapshotManager(
      String tablePath,
      CatalogTable catalogTable,
      UnityCatalogClientFactory.UnityCatalogClient unityCatalogClient,
      Configuration hadoopConf) {
    this(tablePath, catalogTable, unityCatalogClient.getUcClient(), hadoopConf);
  }

  CatalogManagedSnapshotManager(
      String tablePath, CatalogTable catalogTable, UCClient ucClient, Configuration hadoopConf) {
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    requireNonNull(catalogTable, "catalogTable is null");
    this.ucTableId = extractUcTableId(catalogTable);
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.kernelEngine = DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null"));
  }

  private static String extractUcTableId(CatalogTable catalogTable) {
    Map<String, String> storageProperties = CatalogTableUtils.getStorageProperties(catalogTable);
    String tableId = storageProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (tableId == null || tableId.trim().isEmpty()) {
      throw new IllegalStateException(
          "Unity Catalog managed table is missing '"
              + UCCommitCoordinatorClient.UC_TABLE_ID_KEY
              + "' storage property.");
    }
    return tableId.trim();
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    Snapshot snapshot = buildSnapshot(Optional.empty());
    latestSnapshotRef.set(snapshot);
    return snapshot;
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    Snapshot snapshot = buildSnapshot(Optional.of(version));
    latestSnapshotRef.set(snapshot);
    return snapshot;
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = ensureSnapshot();
    List<ParsedCatalogCommitData> catalogCommits =
        fetchRatifiedCatalogCommits(Optional.of(snapshot.getVersion()));

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

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    SnapshotImpl snapshot = ensureSnapshot();
    List<ParsedCatalogCommitData> catalogCommits =
        fetchRatifiedCatalogCommits(Optional.of(snapshot.getVersion()));

    Optional<Long> earliestRatifiedVersion =
        catalogCommits.stream().map(ParsedLogData::getVersion).min(Long::compareTo);

    long earliestAvailable =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), earliestRatifiedVersion)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine, snapshot.getLogPath(), earliestRatifiedVersion);

    long latest = snapshot.getVersion();
    if (version < earliestAvailable || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliestAvailable, latest);
    }
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    requireNonNull(engine, "engine is null");
    requireNonNull(endVersion, "endVersion is null");
    List<ParsedCatalogCommitData> catalogCommits = fetchRatifiedCatalogCommits(endVersion);

    CommitRangeBuilder builder =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion))
            .withLogData(
                catalogCommits.stream()
                    .map(commit -> (ParsedLogData) commit)
                    .collect(Collectors.toList()));

    endVersion.ifPresent(
        version -> builder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(version)));

    return builder.build(engine);
  }

  private SnapshotImpl ensureSnapshot() {
    Snapshot snapshot = latestSnapshotRef.get();
    if (snapshot == null) {
      snapshot = loadLatestSnapshot();
    }
    return (SnapshotImpl) snapshot;
  }

  private List<ParsedCatalogCommitData> fetchRatifiedCatalogCommits(Optional<Long> endVersionOpt) {
    GetCommitsResponse response = fetchCommits(endVersionOpt);
    List<ParsedCatalogCommitData> catalogCommits = convertToCatalogCommits(response.getCommits());
    if (catalogCommits.isEmpty()) {
      LOG.debug("No catalog commits returned for Unity Catalog table '{}'.", ucTableId);
    }
    return catalogCommits;
  }

  private Snapshot buildSnapshot(Optional<Long> versionOpt) {
    GetCommitsResponse response = fetchCommits(versionOpt);
    long catalogVersion = resolveCatalogVersion(response.getLatestTableVersion());
    versionOpt.ifPresent(version -> validateRequestedVersion(version, catalogVersion));

    List<ParsedLogData> logData =
        convertToCatalogCommits(response.getCommits()).stream()
            .map(commit -> (ParsedLogData) commit)
            .collect(Collectors.toList());

    SnapshotBuilder builder = TableManager.loadSnapshot(tablePath);
    if (versionOpt.isPresent()) {
      builder = builder.atVersion(versionOpt.get());
    }
    return builder.withLogData(logData).build(kernelEngine);
  }

  private void validateRequestedVersion(long versionToLoad, long catalogVersion) {
    if (versionToLoad > catalogVersion) {
      throw new IllegalArgumentException(
          String.format(
              "[%s] Cannot load table version %d as the latest version ratified by UC is %d",
              ucTableId, versionToLoad, catalogVersion));
    }
  }

  private long resolveCatalogVersion(long latestTableVersion) {
    return latestTableVersion == -1 ? 0 : latestTableVersion;
  }

  private GetCommitsResponse fetchCommits(Optional<Long> endVersionOpt) {
    try {
      URI tableUri = new Path(tablePath).toUri();
      return ucClient.getCommits(ucTableId, tableUri, Optional.empty(), endVersionOpt);
    } catch (IOException | UCCommitCoordinatorException e) {
      throw new RuntimeException(
          "Failed to retrieve Unity Catalog commits for table " + ucTableId, e);
    }
  }

  private List<ParsedCatalogCommitData> convertToCatalogCommits(List<Commit> commits) {
    return commits.stream()
        .sorted(Comparator.comparingLong(Commit::getVersion))
        .map(
            commit ->
                ParsedCatalogCommitData.forFileStatus(
                    FileStatus.of(
                        commit.getFileStatus().getPath().toString(),
                        commit.getFileStatus().getLen(),
                        commit.getFileStatus().getModificationTime())))
        .collect(Collectors.toList());
  }
}
