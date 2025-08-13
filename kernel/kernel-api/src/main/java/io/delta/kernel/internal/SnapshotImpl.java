/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.TableConfig.TOMBSTONE_RETENTION;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Implementation of {@link Snapshot}. */
public class SnapshotImpl implements Snapshot {
  private final Path logPath;
  private final Path dataPath;
  private final long version;
  private final Lazy<LogSegment> lazyLogSegment;
  private final LogReplay logReplay;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Committer committer;
  private final SnapshotMetrics snapshotMetrics;
  private Optional<Long> inCommitTimestampOpt;
  private Lazy<SnapshotReport> lazySnapshotReport;
  private Lazy<Optional<List<Column>>> lazyClusteringColumns;

  public SnapshotImpl(
      Path dataPath,
      long version,
      Lazy<LogSegment> lazyLogSegment,
      LogReplay logReplay,
      Protocol protocol,
      Metadata metadata,
      Committer committer,
      SnapshotQueryContext snapshotContext) {
    checkArgument(version >= 0, "A snapshot cannot have version < 0");
    this.logPath = new Path(dataPath, "_delta_log");
    this.dataPath = dataPath;
    this.version = version;
    this.lazyLogSegment = lazyLogSegment;
    this.logReplay = logReplay;
    this.protocol = requireNonNull(protocol);
    this.metadata = requireNonNull(metadata);
    this.committer = committer;
    this.inCommitTimestampOpt = Optional.empty();
    this.snapshotMetrics = snapshotContext.getSnapshotMetrics();

    // We create the actual Snapshot report lazily (on first access) instead of eagerly in this
    // constructor because some Snapshot metrics, like {@link
    // io.delta.kernel.metrics.SnapshotMetricsResult#getLoadSnapshotTotalDurationNs}, are only
    // completed *after* the Snapshot has been constructed.
    this.lazySnapshotReport = new Lazy<>(() -> SnapshotReportImpl.forSuccess(snapshotContext));
    this.lazyClusteringColumns =
        new Lazy<>(
            () ->
                ClusteringMetadataDomain.fromSnapshot(this)
                    .map(ClusteringMetadataDomain::getClusteringColumns));
  }

  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public String getPath() {
    return dataPath.toString();
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public List<String> getPartitionColumnNames() {
    return VectorUtils.toJavaList(getMetadata().getPartitionColumns());
  }

  /**
   * Get the timestamp (in milliseconds since the Unix epoch) of the latest commit in this Snapshot.
   * If the table does not yet exist (i.e. this Snapshot is being used to create a new table), this
   * method returns -1. Note that this -1 value will never be exposed to users - either they get a
   * valid snapshot for an existing table or they get an exception.
   *
   * <p>When InCommitTimestampTableFeature is enabled, the timestamp is retrieved from the
   * CommitInfo of the latest commit in this Snapshot, which can result in an IO operation.
   *
   * <p>For non-ICT tables, this is the same as the file modification time of the latest commit in
   * this Snapshot.
   */
  @Override
  public long getTimestamp(Engine engine) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      if (!inCommitTimestampOpt.isPresent()) {
        Optional<CommitInfo> commitInfoOpt = CommitInfo.getCommitInfoOpt(engine, logPath, version);
        inCommitTimestampOpt =
            Optional.of(
                CommitInfo.getRequiredInCommitTimestamp(
                    commitInfoOpt, String.valueOf(version), dataPath));
      }
      return inCommitTimestampOpt.get();
    } else {
      return getLogSegment().getLastCommitTimestamp();
    }
  }

  @Override
  public StructType getSchema() {
    return getMetadata().getSchema();
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.ofNullable(getActiveDomainMetadataMap().get(domain))
        .map(DomainMetadata::getConfiguration);
  }

  @Override
  public Map<String, String> getTableProperties() {
    return metadata.getConfiguration();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new ScanBuilderImpl(
        dataPath, version, protocol, metadata, getSchema(), logReplay, getSnapshotReport());
  }

  ///////////////////
  // Internal APIs //
  ///////////////////

  public Committer getCommitter() {
    return committer;
  }

  public Path getLogPath() {
    return logPath;
  }

  public Path getDataPath() {
    return dataPath;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public SnapshotReport getSnapshotReport() {
    return lazySnapshotReport.get();
  }

  /**
   * Returns the clustering columns for this snapshot.
   *
   * <ul>
   *   <li>Optional.empty() - unclustered table (clustering is not enabled)
   *   <li>Optional.of([]) - clustered table with no clustering columns (clustering is enabled)
   *   <li>Optional.of([col1, col2]) - clustered table with the given physical clustering columns
   * </ul>
   *
   * @return the physical clustering columns in this snapshot
   */
  public Optional<List<Column>> getPhysicalClusteringColumns() {
    return lazyClusteringColumns.get();
  }

  /**
   * Get the domain metadata map from the log replay, which lazily loads and replays a history of
   * domain metadata actions, resolving them to produce the current state of the domain metadata.
   * Only active domain metadata are included in this map.
   *
   * @return A map where the keys are domain names and the values are {@link DomainMetadata}
   *     objects.
   */
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    return logReplay.getActiveDomainMetadataMap();
  }

  /** Returns the crc info for the current snapshot if the checksum file is read */
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return logReplay.getCurrentCrcInfo();
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public LogSegment getLogSegment() {
    return lazyLogSegment.get();
  }

  @VisibleForTesting
  public Lazy<LogSegment> getLazyLogSegment() {
    return lazyLogSegment;
  }

  public CreateCheckpointIterator getCreateCheckpointIterator(Engine engine) {
    long minFileRetentionTimestampMillis =
        System.currentTimeMillis() - TOMBSTONE_RETENTION.fromMetadata(metadata);
    return new CreateCheckpointIterator(
        engine, getLogSegment(), minFileRetentionTimestampMillis, snapshotMetrics.scanMetrics);
  }

  /**
   * Get the latest transaction version for given <i>applicationId</i>. This information comes from
   * the transactions identifiers stored in Delta transaction log. This API is not a public API. For
   * now keep this internal to enable Flink upgrade to use Kernel.
   *
   * @param applicationId Identifier of the application that put transaction identifiers in Delta
   *     transaction log
   * @return Last transaction version or {@link Optional#empty()} if no transaction identifier
   *     exists for this application.
   */
  public Optional<Long> getLatestTransactionVersion(Engine engine, String applicationId) {
    return logReplay.getLatestTransactionIdentifier(engine, applicationId);
  }
}
