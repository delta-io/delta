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

package io.delta.kernel.internal.commitrange;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link CommitRangeBuilder}.
 *
 * <p>Note: The primary responsibility of this class is to take input, validate that input, and then
 * create a {@link CommitRange} instance with the specified configuration.
 */
public class CommitRangeBuilderImpl implements CommitRangeBuilder {

  public static class Context {
    public final String unresolvedPath;
    public Optional<CommitBoundary> startBoundaryOpt = Optional.empty();
    public Optional<CommitBoundary> endBoundaryOpt = Optional.empty();
    public List<ParsedLogData> logDatas = Collections.emptyList();

    public Context(String unresolvedPath) {
      this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
    }
  }

  private final Context ctx;

  public CommitRangeBuilderImpl(String unresolvedPath) {
    ctx = new Context(unresolvedPath);
  }

  ///////////////////////////////////////
  // Public CommitRangeBuilder Methods //
  ///////////////////////////////////////

  @Override
  public CommitRangeBuilderImpl withStartBoundary(CommitBoundary startBoundary) {
    ctx.startBoundaryOpt = Optional.of(requireNonNull(startBoundary, "startBoundary is null"));
    return this;
  }

  @Override
  public CommitRangeBuilderImpl withEndBoundary(CommitBoundary endBoundary) {
    ctx.endBoundaryOpt = Optional.of(requireNonNull(endBoundary, "endBoundary is null"));
    return this;
  }

  @Override
  public CommitRangeBuilderImpl withLogData(List<ParsedLogData> logData) {
    ctx.logDatas = requireNonNull(logData, "logData is null");
    return this;
  }

  @Override
  public CommitRange build(Engine engine) {
    if (!ctx.logDatas.isEmpty()) {
      throw new UnsupportedOperationException("CommitRange does not support providing logData yet");
    }
    validateInputOnBuild();
    return new CommitRangeFactory(engine, ctx).create(engine);
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private void validateInputOnBuild() {
    // Validate that start boundary is less than or equal to end boundary if both are provided
    if (ctx.startBoundaryOpt.isPresent() && ctx.endBoundaryOpt.isPresent()) {
      CommitBoundary startBoundary = ctx.startBoundaryOpt.get();
      CommitBoundary endBoundary = ctx.endBoundaryOpt.get();

      // If both are version-based, compare versions
      if (startBoundary.isVersion() && endBoundary.isVersion()) {
        checkArgument(
            startBoundary.getVersion() <= endBoundary.getVersion(),
            "startVersion must be <= endVersion");
      }
      // If both are timestamp-based, compare timestamps
      else if (startBoundary.isTimestamp() && endBoundary.isTimestamp()) {
        checkArgument(
            startBoundary.getTimestamp() <= endBoundary.getTimestamp(),
            "startTimestamp must be <= endTimestamp");
      }
      // Mixed types are allowed but will need runtime resolution
    }

    // TODO: validate parsedLogData input
  }
}
