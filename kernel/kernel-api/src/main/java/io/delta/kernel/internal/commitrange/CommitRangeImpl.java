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

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.TableChangesUtils;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@link CommitRange}. */
public class CommitRangeImpl implements CommitRange {

  private final Path dataPath;
  private final Optional<CommitRangeBuilder.CommitBoundary> startBoundaryOpt;
  private final Optional<CommitRangeBuilder.CommitBoundary> endBoundaryOpt;

  private final long startVersion;
  private final long endVersion;
  private final List<ParsedDeltaData> deltas;

  public CommitRangeImpl(
      Path dataPath,
      Optional<CommitRangeBuilder.CommitBoundary> startBoundaryOpt,
      Optional<CommitRangeBuilder.CommitBoundary> endBoundaryOpt,
      long startVersion,
      long endVersion,
      List<ParsedDeltaData> deltas) {
    checkArgument(startVersion <= endVersion, "must have startVersion <= endVersion");
    checkArgument(
        deltas.size() == endVersion - startVersion + 1, "deltaFiles size must match size of range");
    this.dataPath = requireNonNull(dataPath, "dataPath cannot be null");
    this.startBoundaryOpt = requireNonNull(startBoundaryOpt, "startSpecOpt cannot be null");
    this.endBoundaryOpt = requireNonNull(endBoundaryOpt, "endSpecOpt cannot be null");
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.deltas = requireNonNull(deltas, "deltas cannot be null");
  }

  ////////////////////////////////////////
  // Public CommitRange Implementation //
  ////////////////////////////////////////

  @Override
  public long getStartVersion() {
    return startVersion;
  }

  @Override
  public long getEndVersion() {
    return endVersion;
  }

  @Override
  public Optional<CommitRangeBuilder.CommitBoundary> getQueryStartBoundary() {
    return startBoundaryOpt;
  }

  @Override
  public Optional<CommitRangeBuilder.CommitBoundary> getQueryEndBoundary() {
    return endBoundaryOpt;
  }

  @VisibleForTesting
  public List<FileStatus> getDeltaFiles() {
    return deltas.stream().map(ParsedDeltaData::getFileStatus).collect(Collectors.toList());
  }

  @Override
  public CloseableIterator<ColumnarBatch> getActions(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    validateParameters(engine, startSnapshot, actionSet);
    // Build on top of getCommitActions() by flattening and adding version/timestamp columns
    CloseableIterator<CommitActions> commits = getCommitActions(engine, startSnapshot, actionSet);

    return TableChangesUtils.flattenCommitsAndAddMetadata(engine, commits);
  }

  @Override
  public CloseableIterator<CommitActions> getCommitActions(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    validateParameters(engine, startSnapshot, actionSet);
    return DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
        engine, dataPath.toString(), getDeltaFiles(), actionSet);
  }

  //////////////////////
  // Private helpers //
  //////////////////////

  private void validateParameters(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    requireNonNull(engine, "engine cannot be null");
    requireNonNull(startSnapshot, "startSnapshot cannot be null");
    requireNonNull(actionSet, "actionSet cannot be null");
    checkArgument(
        startSnapshot.getVersion() == startVersion,
        "startSnapshot must have version = startVersion");
  }
}
