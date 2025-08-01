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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

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
class SnapshotFactory {

  private final SnapshotBuilderImpl.Context ctx;
  private final String resolvedPath;
  private final Path wrappedTablePath;

  SnapshotFactory(Engine engine, SnapshotBuilderImpl.Context ctx) {
    this.ctx = ctx;
    this.resolvedPath = resolvePath(engine);
    this.wrappedTablePath = new Path(resolvedPath);
  }

  SnapshotImpl create(Engine engine) {
    final SnapshotQueryContext snapshotCtx = getSnapshotQueryContext();

    try {
      return createImpl(engine, snapshotCtx);
    } catch (Exception e) {
      snapshotCtx.recordSnapshotErrorReport(engine, e);
      throw e;
    }
  }

  private SnapshotImpl createImpl(Engine engine, SnapshotQueryContext snapshotCtx) {
    final Lazy<LogSegment> lazyLogSegment = getLazyLogSegment(engine, snapshotCtx);
    final LogReplay logReplay = getLogReplay(engine, lazyLogSegment);

    return new SnapshotImpl(
        wrappedTablePath,
        ctx.versionOpt.orElseGet(() -> lazyLogSegment.get().getVersion()),
        lazyLogSegment,
        logReplay,
        getProtocol(logReplay),
        getMetadata(logReplay),
        ctx.committerOpt.orElse(DefaultFileSystemManagedTableOnlyCommitter.INSTANCE),
        snapshotCtx);
  }

  private SnapshotQueryContext getSnapshotQueryContext() {
    if (ctx.versionOpt.isPresent()) {
      return SnapshotQueryContext.forVersionSnapshot(resolvedPath, ctx.versionOpt.get());
    }
    return SnapshotQueryContext.forLatestSnapshot(resolvedPath);
  }

  private String resolvePath(Engine engine) {
    try {
      return wrapEngineExceptionThrowsIO(
          () -> engine.getFileSystemClient().resolvePath(ctx.unresolvedPath),
          "Resolving path %s",
          ctx.unresolvedPath);
    } catch (IOException io) {
      throw new UncheckedIOException(io);
    }
  }

  private Lazy<LogSegment> getLazyLogSegment(Engine engine, SnapshotQueryContext snapshotCtx) {
    return new Lazy<>(
        () -> {
          final LogSegment result =
              new SnapshotManager(wrappedTablePath)
                  .getLogSegmentForVersion(
                      engine, getTargetVersionToLoad(engine, snapshotCtx), ctx.logDatas);

          if (!ctx.versionOpt.isPresent()) {
            snapshotCtx.setVersion(result.getVersion());
          }

          return result;
        });
  }

  private Optional<Long> getTargetVersionToLoad(Engine engine, SnapshotQueryContext snapshotCtx) {
    // TODO: if time travel by timestamp, call snapshotCtx.setVersion after resolving the version
    return ctx.versionOpt;
  }

  private LogReplay getLogReplay(Engine engine, Lazy<LogSegment> lazyLogSegment) {
    return new LogReplay(
        wrappedTablePath,
        engine,
        lazyLogSegment,
        Optional.empty() /* snapshotHint */,
        // TODO: Proper metrics
        new SnapshotMetrics());
  }

  private Protocol getProtocol(LogReplay logReplay) {
    return ctx.protocolAndMetadataOpt.map(x -> x._1).orElseGet(logReplay::getProtocol);
  }

  private Metadata getMetadata(LogReplay logReplay) {
    return ctx.protocolAndMetadataOpt.map(x -> x._2).orElseGet(logReplay::getMetadata);
  }
}
