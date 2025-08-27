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
import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitRangeFactory {

  // TODO add some logging
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
    Optional<Long> endVersionOpt = tryResolveEndVersion(engine);
    endVersionOpt.ifPresent(endVersion -> checkArgument(endVersion >= startVersion));
    // TODO: for now we just store a list of commit files, this will be updated when we add support
    // for ccv2
    List<FileStatus> deltaFiles = getDeltaFiles(engine, startVersion, endVersionOpt);
    long endVersion =
        endVersionOpt.orElseGet(
            () -> FileNames.deltaVersion(ListUtils.getLast(deltaFiles).getPath()));
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
    return ctx.startBoundaryOpt
        .map(
            spec -> {
              if (spec.isVersion()) {
                return spec.getVersion();
              } else {
                // TODO: support ccv2 tables
                return DeltaHistoryManager.getVersionAtOrAfterTimestamp(
                    engine, logPath, spec.getTimestamp(), asSnapshotImpl(spec.getLatestSnapshot()));
              }
            })
        .orElse(0L); // Default to version 0 if no start spec provided
  }

  private Optional<Long> tryResolveEndVersion(Engine engine) {
    return ctx.endBoundaryOpt.map(
        spec -> {
          if (spec.isVersion()) {
            return spec.getVersion();
          } else {
            // TODO: support ccv2 tables
            return DeltaHistoryManager.getVersionBeforeOrAtTimestamp(
                engine, logPath, spec.getTimestamp(), asSnapshotImpl(spec.getLatestSnapshot()));
          }
        });
  }

  private List<FileStatus> getDeltaFiles(
      Engine engine, long startVersion, Optional<Long> endVersionOpt) {
    return DeltaLogActionUtils.getCommitFilesForVersionRange(
        engine, tablePath, startVersion, endVersionOpt);
  }
}
