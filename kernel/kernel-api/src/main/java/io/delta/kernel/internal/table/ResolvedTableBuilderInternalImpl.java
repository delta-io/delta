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
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Clock;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ResolvedTableBuilderInternalImpl implements ResolvedTableBuilderInternal {
  private final String unresolvedPath;
  private Optional<Long> versionOpt;
  private List<ParsedLogData> logData;
  private Clock clock;

  public ResolvedTableBuilderInternalImpl(String unresolvedPath) {
    this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
    this.versionOpt = Optional.empty();
    this.logData = Collections.emptyList();
    this.clock = System::currentTimeMillis;
  }

  /////////////////////////////////////////
  // Public ResolvedTableBuilder Methods //
  /////////////////////////////////////////

  @Override
  public ResolvedTableBuilderInternal atVersion(long version) {
    this.versionOpt = Optional.of(version);
    return this;
  }

  @Override
  public ResolvedTableBuilderInternal withLogData(List<ParsedLogData> logData) {
    this.logData = requireNonNull(logData, "logData is null");
    return this;
  }

  @Override
  public ResolvedTableInternal build(Engine engine) {
    final String resolvedPath = resolvePath(engine);
    validateInputOnBuild();
    return buildTable(engine, resolvedPath);
  }

  //////////////////////////////////////////
  // ResolvedTableBuilderInternal Methods //
  //////////////////////////////////////////

  @Override
  public ResolvedTableBuilderInternal withClock(Clock clock) {
    this.clock = requireNonNull(clock, "clock is null");
    return this;
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private String resolvePath(Engine engine) {
    try {
      return wrapEngineExceptionThrowsIO(
          () -> engine.getFileSystemClient().resolvePath(unresolvedPath),
          "Resolving path %s",
          unresolvedPath);
    } catch (IOException io) {
      throw new UncheckedIOException(io);
    }
  }

  private void validateInputOnBuild() {
    checkArgument(versionOpt.orElse(0L) >= 0, "version must be >= 0");
    // TODO: logData if and only if version is provided
    // TODO: logData only ratified staged commits
    // TODO: logData sorted and contiguous
    // TODO: logData ends with version
  }

  private ResolvedTableInternal buildTable(Engine engine, String resolvedPath) {
    final Lazy<LogSegment> lazyLogSegment =
        new Lazy<>(
            () ->
                new SnapshotManager(new Path(resolvedPath))
                    .getLogSegmentForVersion(engine, versionOpt, logData));

    final long version = versionOpt.orElse(lazyLogSegment.get().getVersion());

    return new ResolvedTableInternalImpl(resolvedPath, version, lazyLogSegment, clock);
  }
}
