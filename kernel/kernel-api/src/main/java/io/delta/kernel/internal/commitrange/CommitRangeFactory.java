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
import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitRangeFactory {

  private static final Logger logger = LoggerFactory.getLogger(CommitRangeFactory.class);

  private final CommitRangeBuilderImpl.Context ctx;
  private final Path tablePath;
  private final Path logPath;

  CommitRangeFactory(Engine engine, CommitRangeBuilderImpl.Context ctx) {
    this.ctx = ctx;
    this.tablePath = new Path(resolvePath(engine, ctx.unresolvedPath));
    this.logPath = new Path(tablePath, "_delta_log");
  }

  CommitRangeImpl create(Engine engine) {
    long startVersion = resolveStartVersion(engine);
    Optional<Long> endVersionOpt = resolveEndVersionIfSpecified(engine);
    endVersionOpt.ifPresent(
        endVersion ->
            checkArgument(
                startVersion <= endVersion,
                String.format(
                    "Resolved startVersion=%d > endVersion=%d", startVersion, endVersion)));
    logger.info(
        "{}: Resolved startVersion={} and endVersion={} from startBoundary={} endBoundary={}",
        tablePath,
        startVersion,
        endVersionOpt,
        ctx.startBoundaryOpt,
        ctx.endBoundaryOpt);
    // TODO: for now we just store a list of commit files, this will be updated when we add support
    //  for ccv2
    List<FileStatus> deltaFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, tablePath, startVersion, endVersionOpt);
    // Once we have listed files, we can resolve endVersion=latestVersion for the default case
    long endVersion =
        endVersionOpt.orElseGet(
            () -> FileNames.deltaVersion(ListUtils.getLast(deltaFiles).getPath()));
    if (!endVersionOpt.isPresent()) {
      logger.info("{}: Resolved endVersion={} to the latest version", tablePath, endVersion);
    }
    return new CommitRangeImpl(
        tablePath, ctx.startBoundaryOpt, ctx.endBoundaryOpt, startVersion, endVersion, deltaFiles);
  }

  private SnapshotImpl asSnapshotImpl(Snapshot snapshot) {
    if (!(snapshot instanceof SnapshotImpl)) {
      throw new IllegalArgumentException(
          "latestSnapshot is not instanceof SnapshotImpl."
              + "You must use SnapshotBuilder to get the latestSnapshot.");
    }
    return (SnapshotImpl) snapshot;
  }

  private long resolveStartVersion(Engine engine) {
    if (!ctx.startBoundaryOpt.isPresent()) {
      // Default to version 0 if no start boundary is provided
      return 0L;
    }
    CommitRangeBuilder.CommitBoundary startBoundary = ctx.startBoundaryOpt.get();

    if (startBoundary.isVersion()) {
      return startBoundary.getVersion();
    } else {
      logger.info(
          "{}: Trying to resolve start-boundary timestamp {} to version",
          tablePath,
          startBoundary.getTimestamp());
      // TODO: support ccv2 tables
      return DeltaHistoryManager.getVersionAtOrAfterTimestamp(
          engine,
          logPath,
          startBoundary.getTimestamp(),
          asSnapshotImpl(startBoundary.getLatestSnapshot()));
    }
  }

  /**
   * This method resolves the endBoundary to a version if it is specified. For a version-based
   * boundary, this just returns the version. For a timestamp-based boundary, this resolves the
   * timestamp to version. When the boundary is not specified, this returns empty, as the version
   * cannot be resolved until later after we have performed any listing.
   */
  private Optional<Long> resolveEndVersionIfSpecified(Engine engine) {
    if (!ctx.endBoundaryOpt.isPresent()) {
      // When endBoundary is not provided, we default to the latest version. We cannot resolve the
      // latest version until later after we have performed any listing.
      return Optional.empty();
    }
    CommitRangeBuilder.CommitBoundary endBoundary = ctx.endBoundaryOpt.get();

    if (endBoundary.isVersion()) {
      return Optional.of(endBoundary.getVersion());
    } else {
      logger.info(
          "{}: Trying to resolve end-boundary timestamp {} to version",
          tablePath,
          endBoundary.getTimestamp());
      // TODO: support ccv2 tables
      long resolvedVersion =
          DeltaHistoryManager.getVersionBeforeOrAtTimestamp(
              engine,
              logPath,
              endBoundary.getTimestamp(),
              asSnapshotImpl(endBoundary.getLatestSnapshot()));
      return Optional.of(resolvedVersion);
    }
  }
}
