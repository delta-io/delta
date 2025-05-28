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
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.table.ResolvedTableBuilderInternalImpl.ResolvedTableBuilderContext;
import java.io.IOException;
import java.io.UncheckedIOException;

public class ResolvedTableFactory {

  private final ResolvedTableBuilderContext ctx;

  public ResolvedTableFactory(ResolvedTableBuilderContext ctx) {
    this.ctx = ctx;
  }

  public ResolvedTableInternalImpl create(Engine engine) {
    final String resolvedPath = resolvePath(engine);
    final Lazy<LogSegment> lazyLogSegment = getLazyLogSegment(engine, resolvedPath);
    final long version = ctx.versionOpt.orElseGet(() -> lazyLogSegment.get().getVersion());

    return new ResolvedTableInternalImpl(resolvedPath, version, lazyLogSegment, ctx.clock);
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
                .getLogSegmentForVersion(engine, ctx.versionOpt, ctx.logData));
  }
}
