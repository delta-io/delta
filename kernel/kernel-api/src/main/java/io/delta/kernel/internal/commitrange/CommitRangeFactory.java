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

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.DeltaErrorsInternal.*;
import static io.delta.kernel.internal.DeltaLogActionUtils.listDeltaLogFilesAsIter;
import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.LogDataUtils;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    List<ParsedCatalogCommitData> ratifiedCommits = getFileBasedRatifiedCommits();
    long startVersion = resolveStartVersion(engine, ratifiedCommits);
    Optional<Long> endVersionOpt = resolveEndVersionIfSpecified(engine, ratifiedCommits);
    validateVersionRange(startVersion, endVersionOpt);
    logResolvedVersions(startVersion, endVersionOpt);
    List<ParsedDeltaData> deltas =
        getDeltasForVersionRangeWithCatalogPriority(
            engine, startVersion, endVersionOpt, ratifiedCommits);
    // Once we have a list of deltas, we can resolve endVersion=latestVersion for the default case
    long endVersion = endVersionOpt.orElseGet(() -> extractLatestVersion(deltas));
    if (!endVersionOpt.isPresent()) {
      logger.info("{}: Resolved end-boundary to the latest version {}", tablePath, endVersion);
    }
    return new CommitRangeImpl(
        tablePath, ctx.startBoundary, ctx.endBoundaryOpt, startVersion, endVersion, deltas);
  }

  private long resolveStartVersion(Engine engine, List<ParsedCatalogCommitData> catalogCommits) {
    if (ctx.startBoundary.isVersion()) {
      return ctx.startBoundary.getVersion();
    } else {
      logger.info(
          "{}: Trying to resolve start-boundary timestamp {} to version",
          tablePath,
          ctx.startBoundary.getTimestamp());
      return DeltaHistoryManager.getVersionAtOrAfterTimestamp(
          engine,
          logPath,
          ctx.startBoundary.getTimestamp(),
          (SnapshotImpl) ctx.startBoundary.getLatestSnapshot(),
          catalogCommits);
    }
  }

  /**
   * This method resolves the endBoundary to a version if it is specified. For a version-based
   * boundary, this just returns the version. For a timestamp-based boundary, this resolves the
   * timestamp to version. When the boundary is not specified, this returns empty, as the version
   * cannot be resolved until later after we have performed any listing.
   */
  private Optional<Long> resolveEndVersionIfSpecified(
      Engine engine, List<ParsedCatalogCommitData> catalogCommits) {
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
      long resolvedVersion =
          DeltaHistoryManager.getVersionBeforeOrAtTimestamp(
              engine,
              logPath,
              endBoundary.getTimestamp(),
              (SnapshotImpl) endBoundary.getLatestSnapshot(),
              catalogCommits);
      return Optional.of(resolvedVersion);
    }
  }

  private void validateVersionRange(long startVersion, Optional<Long> endVersionOpt) {
    endVersionOpt.ifPresent(
        endVersion -> {
          if (startVersion > endVersion) {
            throw invalidResolvedVersionRange(tablePath.toString(), startVersion, endVersion);
          }
        });
  }

  private void logResolvedVersions(long startVersion, Optional<Long> endVersionOpt) {
    logger.info(
        "{}: Resolved startVersion={} and endVersion={} from startBoundary={} endBoundary={}",
        tablePath,
        startVersion,
        endVersionOpt,
        ctx.startBoundary,
        ctx.endBoundaryOpt);
  }

  private long extractLatestVersion(List<ParsedDeltaData> deltaDatas) {
    return ListUtils.getLast(deltaDatas).getVersion();
  }

  private List<ParsedCatalogCommitData> getFileBasedRatifiedCommits() {
    // Note: currently this is all we allow in CommitRangeBuilder anyway, but in the future that
    // could change
    return ctx.logDatas.stream()
        .filter(logData -> logData instanceof ParsedCatalogCommitData)
        .map(logData -> (ParsedCatalogCommitData) logData)
        .filter(deltaData -> deltaData.isFile())
        .collect(Collectors.toList());
  }

  /**
   * Lists the _delta_log and combines the found published deltas with the catalog commits to
   * compile a single contiguous list of deltas. Catalog commits take priority over published deltas
   * when both are present for the same commit version. Returned deltas are guaranteed to start with
   * startVersion, end with endVersion if endVersionOpt is non-empty, and are contiguous. Throws an
   * exception if no deltas are found in the version range, or if startVersion or endVersion cannot
   * be found.
   */
  private List<ParsedDeltaData> getDeltasForVersionRangeWithCatalogPriority(
      Engine engine,
      long startVersion,
      Optional<Long> endVersionOpt,
      List<ParsedCatalogCommitData> ratifiedCommits) {
    // Get published deltas between startVersion and endVersionOpt
    List<ParsedDeltaData> publishedDeltas =
        getPublishedDeltasInVersionRange(engine, startVersion, endVersionOpt);

    // Get ratified deltas between startVersion and endVersionOpt
    List<ParsedDeltaData> ratifiedDeltas =
        ratifiedCommits.stream()
            .filter(
                x ->
                    x.getVersion() >= startVersion
                        && x.getVersion() <= endVersionOpt.orElse(Long.MAX_VALUE))
            .collect(Collectors.toList());

    // Validate they are contiguous and valid (i.e. backfill is ordered)
    validatePublishedPlusRatifiedDeltas(publishedDeltas, ratifiedDeltas);
    List<ParsedDeltaData> combinedDeltas =
        LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
            publishedDeltas, ratifiedDeltas);
    validateDeltasMatchVersionRange(combinedDeltas, startVersion, endVersionOpt);
    return combinedDeltas;
  }

  /**
   * Returns any published deltas found on the file-system within the version range provided and
   * validates that they are contiguous. Returns a sorted and contiguous list of deltas for the
   * version range, but does no validation that the list fills the range.
   */
  private List<ParsedDeltaData> getPublishedDeltasInVersionRange(
      Engine engine, long startVersion, Optional<Long> endVersionOpt) {
    final List<FileStatus> commitFiles =
        listDeltaLogFilesAsIter(
                engine,
                Collections.singleton(FileNames.DeltaLogFileType.COMMIT),
                tablePath,
                startVersion,
                endVersionOpt,
                false /* mustBeRecreatable */)
            .toInMemoryList();
    List<ParsedDeltaData> publishedDeltas =
        commitFiles.stream().map(ParsedDeltaData::forFileStatus).collect(Collectors.toList());

    // Validate listed delta files are contiguous
    if (publishedDeltas.size() > 1) {
      for (int i = 1; i < publishedDeltas.size(); i++) {
        final ParsedLogData prev = publishedDeltas.get(i - 1);
        final ParsedLogData curr = publishedDeltas.get(i);
        if (prev.getVersion() + 1 != curr.getVersion()) {
          throw publishedDeltasNotContiguous(
              tablePath.toString(),
              publishedDeltas.stream()
                  .map(ParsedDeltaData::getVersion)
                  .collect(Collectors.toList()));
        }
      }
    }
    return publishedDeltas;
  }

  private void validatePublishedPlusRatifiedDeltas(
      List<ParsedDeltaData> publishedDeltas, List<ParsedDeltaData> ratifiedDeltas) {
    // Valid example: P0, P1, P2 + R1 (ratified within published)
    // Valid example: P0, P1 + R2, R3 (no overlap)
    // Valid example: P0, P1 + R1, R2 (overlap)
    if (!publishedDeltas.isEmpty() && !ratifiedDeltas.isEmpty()) {
      long earliestPublishedVersion = publishedDeltas.get(0).getVersion();
      long earliestRatifiedVersion = ratifiedDeltas.get(0).getVersion();
      // We cannot have ratifiedDeltas.head.version < publishedDeltas.head.version
      // Invalid example: P2, P3 + R1, R2, R3
      if (earliestRatifiedVersion < earliestPublishedVersion) {
        throw catalogCommitsPrecedePublishedDeltas(
            tablePath.toString(),
            earliestRatifiedVersion,
            publishedDeltas.stream().map(ParsedDeltaData::getVersion).collect(Collectors.toList()));
      }
      long lastPublishedVersion = ListUtils.getLast(publishedDeltas).getVersion();
      // We must have publishedDeltas + ratifiedDeltas be contiguous
      // Invalid example: P0, P1 + R3, R4
      if (lastPublishedVersion + 1 < earliestRatifiedVersion) {
        throw publishedDeltasAndCatalogCommitsNotContiguous(
            tablePath.toString(),
            publishedDeltas.stream().map(ParsedDeltaData::getVersion).collect(Collectors.toList()),
            ratifiedDeltas.stream().map(ParsedDeltaData::getVersion).collect(Collectors.toList()));
      }
    }
  }

  private void validateDeltasMatchVersionRange(
      List<ParsedDeltaData> deltas, long startVersion, Optional<Long> endVersionOpt) {
    // This can only happen if publishedDeltas.isEmpty && ratifiedDeltas.isEmpty
    if (deltas.isEmpty()) {
      throw noCommitFilesFoundForVersionRange(tablePath.toString(), startVersion, endVersionOpt);
    }

    long earliestVersion = ListUtils.getFirst(deltas).getVersion();
    long latestVersion = ListUtils.getLast(deltas).getVersion();

    if (earliestVersion != startVersion) {
      throw startVersionNotFound(tablePath.toString(), startVersion, Optional.of(earliestVersion));
    }
    endVersionOpt.ifPresent(
        endVersion -> {
          if (latestVersion != endVersion) {
            throw endVersionNotFound(tablePath.toString(), endVersion, latestVersion);
          }
        });
  }
}
