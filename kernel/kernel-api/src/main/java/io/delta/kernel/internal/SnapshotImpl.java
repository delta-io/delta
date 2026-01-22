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

import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.commit.CatalogCommitter;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.commit.PublishFailedException;
import io.delta.kernel.commit.PublishMetadata;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumUtils;
import io.delta.kernel.internal.checksum.ChecksumWriter;
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.ReplaceTableTransactionBuilder;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link Snapshot}. */
public class SnapshotImpl implements Snapshot {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotImpl.class);

  private final Path logPath;
  private final Path dataPath;
  private final long version;
  private final Lazy<LogSegment> lazyLogSegment;
  private final LogReplay logReplay;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Committer committer;

  /**
   * If this snapshot does not have the InCommitTimestamp (ICT) table feature enabled, then this is
   * always Optional.empty(). If it does, then this is:
   *
   * <ul>
   *   <li>Optional.empty(): if the ICT value is not yet known (i.e. has not yet been read from the
   *       CRC or CommitInfo)
   *   <li>Optional.of(timestamp): if the ICT value has been read from the CRC or CommitInfo, or was
   *       injected into this Snapshot at construction time (e.g. for a post-commit snapshot)
   * </ul>
   */
  private Optional<Long> inCommitTimestampOpt;

  private Lazy<SnapshotReport> lazySnapshotReport;
  private Lazy<Optional<List<Column>>> lazyClusteringColumns;

  // TODO: Do not take in LogReplay as a constructor argument.
  // TODO: Also take in clustering columns for post-commit snapshot
  public SnapshotImpl(
      Path dataPath,
      long version,
      Lazy<LogSegment> lazyLogSegment,
      LogReplay logReplay,
      Protocol protocol,
      Metadata metadata,
      Committer committer,
      SnapshotQueryContext snapshotContext,
      Optional<Long> inCommitTimestampOpt) {
    checkArgument(version >= 0, "A snapshot cannot have version < 0");
    this.logPath = new Path(dataPath, "_delta_log");
    this.dataPath = dataPath;
    this.version = version;
    this.lazyLogSegment = lazyLogSegment;
    this.logReplay = logReplay;
    this.protocol = requireNonNull(protocol);
    this.metadata = requireNonNull(metadata);
    this.committer = committer;
    this.inCommitTimestampOpt = inCommitTimestampOpt;

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
   *
   * <p>When InCommitTimestampTableFeature is enabled, the timestamp is retrieved from the
   * CommitInfo of the latest commit in this Snapshot, which can result in an IO operation.
   *
   * <p>For non-ICT tables, this is the same as the file modification time of the latest commit in
   * this Snapshot.
   */
  // TODO: Support reading from CRC file if available
  @Override
  public long getTimestamp(Engine engine) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      if (!inCommitTimestampOpt.isPresent()) {
        final Optional<CommitInfo> commitInfoOpt =
            CommitInfo.tryReadCommitInfoFromDeltaFile(
                engine, getLogSegment().getDeltaFileAtEndVersion());
        inCommitTimestampOpt =
            Optional.of(
                CommitInfo.extractRequiredIctFromCommitInfoOpt(commitInfoOpt, version, dataPath));
      }
      return inCommitTimestampOpt.get();
    } else {
      return getLogSegment().getDeltaFileAtEndVersion().getModificationTime();
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
  public SnapshotStatistics getStatistics() {
    return new SnapshotStatisticsImpl();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new ScanBuilderImpl(
        dataPath, version, protocol, metadata, getSchema(), logReplay, getSnapshotReport());
  }

  @Override
  public UpdateTableTransactionBuilder buildUpdateTableTransaction(
      String engineInfo, Operation operation) {
    return new UpdateTableTransactionBuilderImpl(this, engineInfo, operation);
  }

  @Override
  public void publish(Engine engine) throws PublishFailedException {
    final List<ParsedCatalogCommitData> allCatalogCommits = getLogSegment().getAllCatalogCommits();
    final boolean isFileSystemBasedTable = !TableFeatures.isCatalogManagedSupported(protocol);
    final boolean isCatalogCommitter = committer instanceof CatalogCommitter;

    if (!allCatalogCommits.isEmpty()) {
      if (isFileSystemBasedTable) {
        throw new IllegalStateException( // This case should be impossible
            "Cannot have catalog commits on a filesystem-managed table");
      }

      if (!isCatalogCommitter) {
        throw new UnsupportedOperationException( // This case should also be impossible
            String.format(
                "[%s] Cannot publish: committer does not support publishing",
                committer.getClass().getName()));
      }
    } else {
      if (isFileSystemBasedTable) {
        logger.info("Publishing not applicable: this is a filesystem-managed table");
        return;
      }

      if (!isCatalogCommitter) {
        logger.info(
            "[{}] Publishing not applicable: committer does not support publishing",
            committer.getClass().getName());
        return;
      }
    }

    // TODO: When we return a post-publish Snapshot, ensure to replace *all* catalog commits with
    //       their published versions, not just the catalog commits that were published. For
    //       example: if we have catalog commits v11, v12, and v13 but the maxPublishedVersion is
    //       12, we will only publish v13. Nonetheless, our post-publish Snapshot must include the
    //       published versions of v11 and v12, too.

    final long maxPublishedDeltaVersion = getMaxPublishedDeltaVersionOrThrow();
    final List<ParsedCatalogCommitData> catalogCommitsToPublish =
        allCatalogCommits.stream()
            .filter(commit -> commit.getVersion() > maxPublishedDeltaVersion)
            .collect(Collectors.toList());

    if (catalogCommitsToPublish.isEmpty()) {
      logger.info("No catalog commits need to be published");
      return;
    }

    final PublishMetadata publishMetadata =
        new PublishMetadata(version, logPath.toString(), catalogCommitsToPublish);

    ((CatalogCommitter) committer).publish(engine, publishMetadata);
  }

  @Override
  public void writeChecksum(Engine engine, Snapshot.ChecksumWriteMode mode) throws IOException {
    final Optional<Snapshot.ChecksumWriteMode> actualOpt = getStatistics().getChecksumWriteMode();

    if (actualOpt.isEmpty()) {
      logger.warn("Not writing checksum: checksum file already exists at version {}", version);
      return;
    }

    final Snapshot.ChecksumWriteMode actual = actualOpt.get();

    switch (mode) {
      case SIMPLE:
        if (actual == ChecksumWriteMode.FULL) {
          throw new IllegalStateException(
              "Cannot write checksum in SIMPLE mode: FULL mode required");
        }

        final CRCInfo crcInfo = logReplay.getCrcInfoAtSnapshotVersion().get();
        logger.info("Executing checksum write in SIMPLE mode");
        new ChecksumWriter(logPath).writeCheckSum(engine, crcInfo);
        return;
      case FULL:
        if (actual == ChecksumWriteMode.SIMPLE) {
          logger.warn("Requested checksum write in FULL mode, but SIMPLE mode is available");
        }
        logger.info("Executing checksum write in FULL mode");
        ChecksumUtils.computeStateAndWriteChecksum(engine, getLogSegment());
        return;
      default:
        throw new IllegalStateException("Unknown checksum write mode: " + mode);
    }
  }

  public void writeCheckpoint(Engine engine) throws IOException {
    // Refuse to create a checkpoint if the table is CatalogManaged but the current snapshot is not
    // published
    if (TableFeatures.isCatalogManagedSupported(protocol)
        && getLogSegment().getMaxPublishedDeltaVersion().orElse(-1L) < version) {
      throw DeltaErrors.checkpointOnUnpublishedCommits(
          getPath(), version, getLogSegment().getMaxPublishedDeltaVersion().orElse(-1L));
    }
    Checkpointer.checkpoint(engine, System::currentTimeMillis, this);
  }

  ///////////////////
  // Internal APIs //
  ///////////////////

  // TODO: make this API public after closing open threads for Replace Table operation
  public ReplaceTableTransactionBuilder buildReplaceTableTransaction(
      StructType schema, String engineInfo) {
    return new ReplaceTableTransactionBuilderV2Impl(this, schema, engineInfo);
  }

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
    return logReplay.getCrcInfoAtSnapshotVersion();
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
    return new CreateCheckpointIterator(engine, getLogSegment(), minFileRetentionTimestampMillis);
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

  ////////////////////
  // Helper Methods //
  ////////////////////

  private long getMaxPublishedDeltaVersionOrThrow() {
    // The maxPublishedDeltaVersion is required for publishing to ensure published deltas are
    // contiguous. The cases where it is unknown should be very rare (e.g. Kernel loaded a
    // LogSegment consisting only of a checkpoint with no corresponding published delta).
    // TODO: Kernel should LIST to authoritatively determine the maxPublishedDeltaVersion, or give
    //       such utilities to CatalogCommitters for them to do this.
    return getLogSegment()
        .getMaxPublishedDeltaVersion()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "maxPublishedDeltaVersion is unknown. This is required for publishing."));
  }

  ///////////////////
  // Inner Classes //
  ///////////////////

  private class SnapshotStatisticsImpl implements SnapshotStatistics {
    @Override
    public Optional<Snapshot.ChecksumWriteMode> getChecksumWriteMode() {
      final boolean checksumFileExists =
          getLogSegment()
              .getLastSeenChecksum()
              .map(checksumFile -> FileNames.checksumVersion(checksumFile.getPath()) == version)
              .orElse(false);

      if (checksumFileExists) {
        return Optional.empty();
      }

      if (logReplay.getCrcInfoAtSnapshotVersion().isPresent()) {
        return Optional.of(Snapshot.ChecksumWriteMode.SIMPLE);
      }

      return Optional.of(Snapshot.ChecksumWriteMode.FULL);
    }
  }
}
