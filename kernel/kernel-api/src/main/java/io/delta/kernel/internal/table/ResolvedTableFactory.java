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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * Factory class responsible for creating {@link ResolvedTableInternal} instances.
 *
 * <p>Note: The {@link ResolvedTableBuilderImpl} is responsible for receiving and validating all
 * builder parameters, and then passing that information to this factory to actually create the
 * {@link ResolvedTableInternal}
 */
class ResolvedTableFactory {

  private final ResolvedTableBuilderImpl.Context ctx;

  ResolvedTableFactory(ResolvedTableBuilderImpl.Context ctx) {
    this.ctx = ctx;
  }

  ResolvedTableInternalImpl create(Engine engine) {
    final String resolvedPath = resolvePath(engine);

    final Lazy<LogSegment> lazyLogSegment = getLazyLogSegment(engine, resolvedPath);
    final long version = ctx.versionOpt.orElseGet(() -> lazyLogSegment.get().getVersion());

    final Lazy<LogReplay> lazyLogReplay = getLazyLogReplay(engine, resolvedPath, lazyLogSegment);
    final Protocol protocol =
        ctx.protocolAndMetadataOpt.map(x -> x._1).orElse(lazyLogReplay.get().getProtocol());

    final Metadata metadata =
        ctx.protocolAndMetadataOpt.map(x -> x._2).orElse(lazyLogReplay.get().getMetadata());

    return new ResolvedTableInternalImpl(
        resolvedPath, version, protocol, metadata, lazyLogSegment, lazyLogReplay, ctx.clock);
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

  private Lazy<LogSegment> getLazyLogSegment(Engine engine, String resolvedPath) {
    return new Lazy<>(
        () ->
            new SnapshotManager(new Path(resolvedPath))
                .getLogSegmentForVersion(engine, ctx.versionOpt, ctx.logDatas));
  }

  private Lazy<LogReplay> getLazyLogReplay(
      Engine engine, String resolvedPath, Lazy<LogSegment> lazyLogSegment) {
    final Path wrappedTablePath = new Path(resolvedPath);
    final Path wrappedLogPath = new Path(wrappedTablePath, "_delta_log");
    return new Lazy<>(
        () ->
            new LogReplay(
                wrappedLogPath,
                wrappedTablePath,
                engine,
                lazyLogSegment.get(),
                // TODO: Proper ResolvedTable-oriented metrics
                Optional.empty() /* snapshotHint */,
                new SnapshotMetrics()));
  }
}
