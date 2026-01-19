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
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.LogDataUtils;
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
    public final CommitBoundary startBoundary;
    public Optional<CommitBoundary> endBoundaryOpt = Optional.empty();
    public List<ParsedLogData> logDatas = Collections.emptyList();
    public Optional<Long> maxCatalogVersion = Optional.empty();

    public Context(String unresolvedPath, CommitBoundary startBoundary) {
      this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
      this.startBoundary = requireNonNull(startBoundary, "startBoundary is null");
    }
  }

  private final Context ctx;

  public CommitRangeBuilderImpl(String unresolvedPath, CommitBoundary startBoundary) {
    ctx = new Context(unresolvedPath, startBoundary);
  }

  ///////////////////////////////////////
  // Public CommitRangeBuilder Methods //
  ///////////////////////////////////////

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
  public CommitRangeBuilderImpl withMaxCatalogVersion(long version) {
    checkArgument(version >= 0, "maxCatalogVersion must be >= 0, but got: %d", version);
    ctx.maxCatalogVersion = Optional.of(version);
    return this;
  }

  @Override
  public CommitRange build(Engine engine) {
    validateInputOnBuild();
    return new CommitRangeFactory(engine, ctx).create(engine);
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private void validateInputOnBuild() {
    // Validate that start boundary is less than or equal to end boundary if end boundary is
    // provided
    if (ctx.endBoundaryOpt.isPresent()) {
      CommitBoundary startBoundary = ctx.startBoundary;
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

    // Validate max catalog version constraints if provided
    if (ctx.maxCatalogVersion.isPresent()) {
      long maxVersion = ctx.maxCatalogVersion.get();

      // Validate start boundary against max catalog version
      if (ctx.startBoundary.isVersion()) {
        checkArgument(
            ctx.startBoundary.getVersion() <= maxVersion,
            String.format(
                "startVersion (%d) must be <= maxCatalogVersion (%d)",
                ctx.startBoundary.getVersion(), maxVersion));
      } else if (ctx.startBoundary.isTimestamp()) {
        long latestSnapshotVersion =
            ((SnapshotImpl) ctx.startBoundary.getLatestSnapshot()).getVersion();
        checkArgument(
            latestSnapshotVersion == maxVersion,
            String.format(
                "When using timestamp boundaries with maxCatalogVersion, the provided "
                    + "snapshot version (%d) must equal maxCatalogVersion (%d)",
                latestSnapshotVersion, maxVersion));
      }

      // Validate end boundary against max catalog version
      if (ctx.endBoundaryOpt.isPresent()) {
        CommitBoundary endBoundary = ctx.endBoundaryOpt.get();
        if (endBoundary.isVersion()) {
          checkArgument(
              endBoundary.getVersion() <= maxVersion,
              String.format(
                  "endVersion (%d) must be <= maxCatalogVersion (%d)",
                  endBoundary.getVersion(), maxVersion));
        } else if (endBoundary.isTimestamp()) {
          long latestSnapshotVersion =
              ((SnapshotImpl) endBoundary.getLatestSnapshot()).getVersion();
          checkArgument(
              latestSnapshotVersion == maxVersion,
              String.format(
                  "When using timestamp boundaries with maxCatalogVersion, the provided "
                      + "snapshot version (%d) must equal maxCatalogVersion (%d)",
                  latestSnapshotVersion, maxVersion));
        }
      }

      // Validate logData ends with maxCatalogVersion when no end boundary is provided
      if (!ctx.endBoundaryOpt.isPresent() && !ctx.logDatas.isEmpty()) {
        long lastLogDataVersion = ctx.logDatas.get(ctx.logDatas.size() - 1).getVersion();
        checkArgument(
            lastLogDataVersion == maxVersion,
            String.format(
                "When maxCatalogVersion is specified without an end boundary, the last "
                    + "logData version (%d) must equal maxCatalogVersion (%d)",
                lastLogDataVersion, maxVersion));
      }
    }

    // Validate logData input
    LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(ctx.logDatas);
    LogDataUtils.validateLogDataIsSortedContiguous(ctx.logDatas);

    // Validate that when endVersion is provided with logData, logData must cover the range
    // This is especially important for catalog-managed tables where the catalog must provide
    // sufficient ratified commits to cover the requested range.
    if (ctx.endBoundaryOpt.isPresent() && !ctx.logDatas.isEmpty()) {
      CommitBoundary endBoundary = ctx.endBoundaryOpt.get();
      if (endBoundary.isVersion()) {
        long endVersion = endBoundary.getVersion();
        long lastLogDataVersion = ctx.logDatas.get(ctx.logDatas.size() - 1).getVersion();
        checkArgument(
            lastLogDataVersion >= endVersion,
            String.format(
                "When endVersion is specified with logData, the last logData version (%d) "
                    + "must be >= endVersion (%d) to cover the requested range",
                lastLogDataVersion, endVersion));
      }
      // Note: For timestamp boundaries, we can't validate at build time since the timestamp
      // needs to be resolved to a version first in CommitRangeFactory
    }
  }
}
