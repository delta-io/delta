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

import static io.delta.kernel.internal.DeltaErrors.commitActionIterationNotSupported;
import static io.delta.kernel.internal.DeltaErrors.missingCommitVersion;
import static io.delta.kernel.internal.DeltaErrors.startVersionNotFound;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitFileStatus;
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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@link CommitRange}. */
public class CommitRangeImpl implements CommitRange {

  private final Path dataPath;
  private final CommitRangeBuilder.CommitBoundary startBoundary;
  private final Optional<CommitRangeBuilder.CommitBoundary> endBoundaryOpt;

  private final long startVersion;
  private final long endVersion;
  private final List<ParsedDeltaData> deltas;
  // Version of the first logical commit after the range, if one was observed. Only used to prove
  // that a version in the range is missing; that commit is never part of the range.
  private final Optional<Long> nextVersionAfterEnd;
  private final boolean commitRangeValidation;
  private final boolean failOnDataLoss;

  public CommitRangeImpl(
      Path dataPath,
      CommitRangeBuilder.CommitBoundary startBoundary,
      Optional<CommitRangeBuilder.CommitBoundary> endBoundaryOpt,
      long startVersion,
      long endVersion,
      List<ParsedDeltaData> deltas,
      Optional<Long> nextVersionAfterEnd,
      boolean commitRangeValidation,
      boolean failOnDataLoss) {
    // R2: Only a validated range must be non-empty and ordered. A relaxed range may be empty, and
    // may even have startVersion > endVersion.
    checkArgument(
        !commitRangeValidation || startVersion <= endVersion,
        "must have startVersion <= endVersion");
    checkArgument(
        !commitRangeValidation || deltas.size() == endVersion - startVersion + 1,
        "deltaFiles size must match size of range");
    this.dataPath = requireNonNull(dataPath, "dataPath cannot be null");
    this.startBoundary = requireNonNull(startBoundary, "startBoundary cannot be null");
    this.endBoundaryOpt = requireNonNull(endBoundaryOpt, "endSpecOpt cannot be null");
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.deltas = requireNonNull(deltas, "deltas cannot be null");
    this.nextVersionAfterEnd =
        requireNonNull(nextVersionAfterEnd, "nextVersionAfterEnd cannot be null");
    this.commitRangeValidation = commitRangeValidation;
    this.failOnDataLoss = failOnDataLoss;
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
  public boolean isEmpty() {
    return deltas.isEmpty();
  }

  @Override
  public CommitRangeBuilder.CommitBoundary getQueryStartBoundary() {
    return startBoundary;
  }

  @Override
  public Optional<CommitRangeBuilder.CommitBoundary> getQueryEndBoundary() {
    return endBoundaryOpt;
  }

  @Override
  public CloseableIterator<CommitFileStatus> getCommitFiles() {
    return new CloseableIterator<CommitFileStatus>() {
      private int index;
      private long expectedVersion = startVersion;
      private boolean done;

      @Override
      public boolean hasNext() {
        if (done) {
          return false;
        }
        boolean inRange = index < deltas.size();
        // R4: A missing version is only reported when a later commit that was actually observed
        // proves it is missing. That is the next commit in the range, or else the first commit
        // after it. Without one, a missing suffix of the range is not an error.
        Optional<Long> observedVersion =
            inRange ? Optional.of(deltas.get(index).getVersion()) : nextVersionAfterEnd;
        if (failOnDataLoss
            && observedVersion.isPresent()
            && observedVersion.get() != expectedVersion
            // Past the range, only the start version (even if it is after the end) and versions up
            // to the end are expected; a gap after the end is out of the range and not an error.
            && (inRange || index == 0 || expectedVersion <= endVersion)) {
          done = true;
          if (index == 0) {
            throw startVersionNotFound(dataPath.toString(), startVersion, observedVersion);
          }
          throw missingCommitVersion(dataPath.toString(), expectedVersion);
        }
        if (!inRange) {
          done = true;
        }
        return inRange;
      }

      @Override
      public CommitFileStatus next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ParsedDeltaData delta = deltas.get(index++);
        if (delta.getVersion() == Long.MAX_VALUE) {
          done = true;
        } else {
          expectedVersion = delta.getVersion() + 1;
        }
        return new CommitFileStatus(delta.getVersion(), delta.getFileStatus());
      }

      @Override
      public void close() {
        done = true;
      }
    };
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

  @Override
  public CloseableIterator<ColumnarBatch> getActions(
      Engine engine, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    validateParameters(engine, actionSet);
    CloseableIterator<CommitActions> commits = getCommitActions(engine, actionSet);
    return TableChangesUtils.flattenCommitsAndAddMetadata(engine, commits);
  }

  @Override
  public CloseableIterator<CommitActions> getCommitActions(
      Engine engine, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    validateParameters(engine, actionSet);
    return DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
        engine, dataPath.toString(), getDeltaFiles(), actionSet);
  }

  //////////////////////
  // Private helpers //
  //////////////////////

  private void validateParameters(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    if (!commitRangeValidation) {
      throw commitActionIterationNotSupported();
    }
    requireNonNull(engine, "engine cannot be null");
    requireNonNull(startSnapshot, "startSnapshot cannot be null");
    requireNonNull(actionSet, "actionSet cannot be null");
    checkArgument(
        startSnapshot.getVersion() == startVersion,
        "startSnapshot must have version = startVersion");
  }

  private void validateParameters(Engine engine, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    requireNonNull(engine, "engine cannot be null");
    requireNonNull(actionSet, "actionSet cannot be null");
  }
}
