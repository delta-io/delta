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

package io.delta.kernel.internal.table;

import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.replay.ProtocolMetadataLogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class responsible for creating {@link Snapshot} instances.
 *
 * <p>This factory takes validated parameters from {@link SnapshotBuilderImpl} and orchestrates the
 * actual snapshot creation process. It handles path resolution, log segment loading, and
 * coordinates with various internal components to construct a fully initialized {@link Snapshot}.
 *
 * <p>Note: The {@link SnapshotBuilderImpl} is responsible for receiving and validating all builder
 * parameters, and then passing that information to this factory to actually create the {@link
 * Snapshot}.
 */
public class SnapshotFactory {

  //////////////////////////////////////////
  // Static utility methods and variables //
  //////////////////////////////////////////

  /**
   * Resolves the latest table version that exists at or before the given {@code
   * millisSinceEpochUTC}.
   *
   * <p>Updates the given {@code snapshotQueryCtx} with the resolved version and prints out useful
   * log statements.
   */
  public static long resolveTimestampToSnapshotVersion(
      Engine engine,
      SnapshotQueryContext snapshotQueryCtx,
      SnapshotImpl latestSnapshot,
      long millisSinceEpochUTC,
      List<ParsedLogData> logDatas) {
    List<ParsedCatalogCommitData> parsedCatalogCommits =
        logDatas.stream()
            .filter(logData -> logData instanceof ParsedCatalogCommitData && logData.isFile())
            .map(catalogCommit -> (ParsedCatalogCommitData) catalogCommit)
            .collect(Collectors.toList());

    final long resolvedVersionToLoad =
        snapshotQueryCtx
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .time(
                () ->
                    DeltaHistoryManager.getActiveCommitAtTimestamp(
                            engine,
                            latestSnapshot,
                            latestSnapshot.getLogPath(),
                            millisSinceEpochUTC,
                            true /* mustBeRecreatable */,
                            false /* canReturnLastCommit */,
                            false /* canReturnEarliestCommit */,
                            parsedCatalogCommits)
                        .getVersion());

    snapshotQueryCtx.setResolvedVersion(resolvedVersionToLoad);

    logger.info(
        "{}: Took {} ms to resolve timestamp {} to snapshot version {}",
        latestSnapshot.getPath(),
        snapshotQueryCtx
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .totalDurationMs(),
        millisSinceEpochUTC,
        resolvedVersionToLoad);

    return resolvedVersionToLoad;
  }

  /**
   * Creates a lazy loader for CRC file information. The CRC file is loaded only once when needed.
   *
   * <p>If {@link Lazy#isPresent()} is false, then the CRC file was never attempted to be loaded.
   *
   * <p>If {@link Lazy#isPresent()} is true, then the result is:
   *
   * <ul>
   *   <li>{@code Optional.empty()} if there is no CRC file in this LogSegment, we failed to read
   *       it, or we failed to parse it (e.g. missing required fields)
   *   <li>{@code Optional.of(crcInfo)} if the file exists and was successfully read and parsed
   * </ul>
   */
  public static Lazy<Optional<CRCInfo>> createLazyChecksumFileLoaderWithMetrics(
      Engine engine, Lazy<LogSegment> lazyLogSegment, SnapshotMetrics snapshotMetrics) {
    return new Lazy<>(
        () -> {
          final Optional<FileStatus> crcFileOpt = lazyLogSegment.get().getLastSeenChecksum();
          if (!crcFileOpt.isPresent()) {
            return Optional.empty();
          }
          return snapshotMetrics.loadCrcTotalDurationTimer.time(
              () -> ChecksumReader.tryReadChecksumFile(engine, crcFileOpt.get()));
        });
  }

  private static final Logger logger = LoggerFactory.getLogger(SnapshotFactory.class);

  //////////////////////////////////
  // Member methods and variables //
  //////////////////////////////////

  private final SnapshotBuilderImpl.Context ctx;
  private final Path tablePath;

  SnapshotFactory(Engine engine, SnapshotBuilderImpl.Context ctx) {
    this.ctx = ctx;
    this.tablePath = new Path(resolvePath(engine, ctx.unresolvedPath));
  }

  SnapshotImpl create(Engine engine) {
    final SnapshotQueryContext snapshotCtx = getSnapshotQueryContext();

    try {
      final SnapshotImpl snapshot =
          snapshotCtx
              .getSnapshotMetrics()
              .loadSnapshotTotalTimer
              .time(() -> createSnapshot(engine, snapshotCtx));

      logger.info(
          "[{}] Took {}ms to load snapshot (version = {}) for snapshot query {}",
          tablePath.toString(),
          snapshotCtx.getSnapshotMetrics().loadSnapshotTotalTimer.totalDurationMs(),
          snapshot.getVersion(),
          snapshotCtx.getQueryDisplayStr());

      engine
          .getMetricsReporters()
          .forEach(reporter -> reporter.report(snapshot.getSnapshotReport()));

      return snapshot;
    } catch (Exception e) {
      snapshotCtx.recordSnapshotErrorReport(engine, e);
      throw e;
    }
  }

  private SnapshotImpl createSnapshot(Engine engine, SnapshotQueryContext snapshotCtx) {
    final Optional<Long> versionToLoad = getTargetVersionToLoad(engine, snapshotCtx);
    final Lazy<LogSegment> lazyLogSegment = getLazyLogSegment(engine, snapshotCtx, versionToLoad);
    final Lazy<Optional<CRCInfo>> lazyCrcInfo =
        createLazyChecksumFileLoaderWithMetrics(
            engine, lazyLogSegment, snapshotCtx.getSnapshotMetrics());

    Protocol protocol;
    Metadata metadata;

    if (ctx.protocolAndMetadataOpt.isPresent()) {
      protocol = ctx.protocolAndMetadataOpt.get()._1;
      metadata = ctx.protocolAndMetadataOpt.get()._2;
    } else {
      ProtocolMetadataLogReplay.Result result =
          ProtocolMetadataLogReplay.loadProtocolAndMetadata(
              engine,
              tablePath,
              lazyLogSegment.get(),
              lazyCrcInfo,
              snapshotCtx.getSnapshotMetrics());
      protocol = result.protocol;
      metadata = result.metadata;
    }

    // TODO: When LogReplay becomes static utilities, we can create it inside of SnapshotImpl
    final LogReplay logReplay = new LogReplay(engine, tablePath, lazyLogSegment, lazyCrcInfo);

    return new SnapshotImpl(
        tablePath,
        versionToLoad.orElseGet(() -> lazyLogSegment.get().getVersion()),
        lazyLogSegment,
        logReplay,
        protocol,
        metadata,
        ctx.committerOpt.orElse(DefaultFileSystemManagedTableOnlyCommitter.INSTANCE),
        snapshotCtx);
  }

  private SnapshotQueryContext getSnapshotQueryContext() {
    if (ctx.versionOpt.isPresent()) {
      return SnapshotQueryContext.forVersionSnapshot(tablePath.toString(), ctx.versionOpt.get());
    }
    if (ctx.timestampQueryContextOpt.isPresent()) {
      return SnapshotQueryContext.forTimestampSnapshot(
          tablePath.toString(), ctx.timestampQueryContextOpt.get()._2);
    }
    return SnapshotQueryContext.forLatestSnapshot(tablePath.toString());
  }

  private Lazy<LogSegment> getLazyLogSegment(
      Engine engine, SnapshotQueryContext snapshotCtx, Optional<Long> versionToLoad) {
    return new Lazy<>(
        () -> {
          final LogSegment logSegment =
              snapshotCtx
                  .getSnapshotMetrics()
                  .loadLogSegmentTotalDurationTimer
                  .time(
                      () ->
                          new SnapshotManager(tablePath)
                              .getLogSegmentForVersion(engine, versionToLoad, ctx.logDatas));

          snapshotCtx.setResolvedVersion(logSegment.getVersion());
          snapshotCtx.setCheckpointVersion(logSegment.getCheckpointVersionOpt());

          return logSegment;
        });
  }

  private Optional<Long> getTargetVersionToLoad(Engine engine, SnapshotQueryContext snapshotCtx) {
    if (ctx.timestampQueryContextOpt.isPresent()) {
      return Optional.of(
          resolveTimestampToSnapshotVersion(
              engine,
              snapshotCtx,
              ctx.timestampQueryContextOpt.get()._1,
              ctx.timestampQueryContextOpt.get()._2,
              ctx.logDatas));
    } else if (ctx.versionOpt.isPresent()) {
      return ctx.versionOpt;
    }
    return Optional.empty();
  }
}
