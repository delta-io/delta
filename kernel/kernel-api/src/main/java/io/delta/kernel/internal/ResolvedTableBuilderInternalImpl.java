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

package io.delta.kernel.internal;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class ResolvedTableBuilderInternalImpl implements ResolvedTableBuilderInternal {
  private final String path;
  private OptionalLong versionOpt;
  private List<ParsedLogData> logData;
  private Clock clock;

  public ResolvedTableBuilderInternalImpl(String path) {
    this.path = requireNonNull(path, "path is null");
    this.versionOpt = OptionalLong.empty();
    this.logData = Collections.emptyList();
    this.clock = System::currentTimeMillis;
  }

  /////////////////////////////////////////
  // Public ResolvedTableBuilder Methods //
  /////////////////////////////////////////

  @Override
  public ResolvedTableBuilderInternal atVersion(long version) {
    this.versionOpt = OptionalLong.of(version);
    return this;
  }

  @Override
  public ResolvedTableBuilderInternal withLogData(List<ParsedLogData> logData) {
    this.logData = requireNonNull(logData, "logData is null");
    return this;
  }

  @Override
  public ResolvedTableInternal build(Engine engine) {
    validateInputOnBuild();
    return buildTable(engine);
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

  private void validateInputOnBuild() {
    checkArgument(versionOpt.orElse(0) >= 0, "version must be >= 0");
    // TODO: logData if and only if version is provided
    // TODO: logData only ratified staged commits
    // TODO: logData sorted and contiguous
    // TODO: logData ends with version
  }

  private ResolvedTableInternal buildTable(Engine engine) {
    final LogSegment logSegment =
        new SnapshotManager(new Path(path))
            .getLogSegmentForVersion(
                engine,
                // TODO: Refactor SnapshotManager to use OptionalLong not Optional<Long>
                versionOpt.isPresent() ? Optional.of(versionOpt.getAsLong()) : Optional.empty(),
                logData);

    return new ResolvedTableInternalImpl(path, logSegment, clock);
  }
}
