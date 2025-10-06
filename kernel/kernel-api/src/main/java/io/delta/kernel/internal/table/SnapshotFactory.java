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

import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
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

    // DeltaHistoryManager only supports ratified staged commits, which is currently what is
    // supported in SnapshotBuilderImpl. Validate this is true and cast to ParsedDeltaData.
    List<ParsedDeltaData> parsedDeltaDatas =
        logDatas.stream()
            .map(
                logData -> {
                  checkState(
                      logData instanceof ParsedDeltaData,
                      String.format(
                          "SnapshotBuilderImpl only supports ParsedDeltaData but found: %s",
                          logData));
                  ParsedDeltaData deltaData = (ParsedDeltaData) logData;
                  checkState(
                      deltaData.isFile() && deltaData.isRatifiedCommit(),
                      String.format(
                          "SnapshotBuilderImpl only supports ratified staged commits but found: %s",
                          deltaData));
                  return deltaData;
                })
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
                            parsedDeltaDatas)
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

  private static final Logger logger = LoggerFactory.getLogger(SnapshotFactory.class);

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
    final LogReplay logReplay = getLogReplay(engine, lazyLogSegment, snapshotCtx);

    return new SnapshotImpl(
        tablePath,
        versionToLoad.orElseGet(() -> lazyLogSegment.get().getVersion()),
        lazyLogSegment,
        logReplay,
        getProtocol(logReplay),
        getMetadata(logReplay),
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

  private LogReplay getLogReplay(
      Engine engine, Lazy<LogSegment> lazyLogSegment, SnapshotQueryContext snapshotCtx) {
    return new LogReplay(tablePath, engine, lazyLogSegment, snapshotCtx.getSnapshotMetrics());
  }

  private Protocol getProtocol(LogReplay logReplay) {
    return ctx.protocolAndMetadataOpt.map(x -> x._1).orElseGet(logReplay::getProtocol);
  }

  private Metadata getMetadata(LogReplay logReplay) {
    return ctx.protocolAndMetadataOpt.map(x -> x._2).orElseGet(logReplay::getMetadata);
  }
}
