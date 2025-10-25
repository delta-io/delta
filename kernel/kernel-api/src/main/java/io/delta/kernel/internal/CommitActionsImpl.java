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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.utils.PeekableIterator;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link CommitActions}.
 *
 * <p>This implementation owns the commit file and supports multiple calls to {@link #getActions()}.
 * The first call reuses initially-read data to avoid double I/O, while subsequent calls re-read the
 * commit file for memory efficiency.
 *
 * <p><b>Resource Management:</b>
 *
 * <ul>
 *   <li>Calling {@link #getActions()} transfers resource ownership to the returned iterator.
 *   <li>Callers MUST close the returned iterator (preferably using try-with-resources) to release
 *       file handles and other resources.
 *   <li>If {@link #getActions()} is never called, internal resources will be released when the
 *       object is garbage collected (though explicit cleanup via {@link #getActions()} followed by
 *       iterator closure is preferred).
 * </ul>
 */
public class CommitActionsImpl implements CommitActions {

  private final Engine engine;
  private final FileStatus commitFile;
  private final StructType readSchema;
  private final String tablePath;
  private final boolean shouldDropProtocolColumn;
  private final boolean shouldDropCommitInfoColumn;
  private final long version;
  private final long timestamp;

  /**
   * Peekable iterator for ActionWrappers. Supports peeking at the first element for metadata
   * extraction. Used for the first getActions() call, then set to null.
   */
  private PeekableIterator<ActionWrapper> peekableIterator;

  /**
   * Creates a CommitActions from a commit file.
   *
   * @param engine the engine for file I/O
   * @param commitFile the commit file to read
   * @param tablePath the table path for error messages
   * @param actionSet the set of actions to read from the commit file
   */
  public CommitActionsImpl(
      Engine engine,
      FileStatus commitFile,
      String tablePath,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    requireNonNull(engine, "engine cannot be null");
    this.commitFile = requireNonNull(commitFile, "commitFile cannot be null");
    this.tablePath = requireNonNull(tablePath, "tablePath cannot be null");

    // Create a new action set which is a super set of the requested actions.
    // The extra actions are needed either for checks or to extract
    // extra information. We will strip out the extra actions before
    // returning the result.
    Set<DeltaLogActionUtils.DeltaAction> copySet = new HashSet<>(actionSet);
    copySet.add(DeltaLogActionUtils.DeltaAction.PROTOCOL);
    // commitInfo is needed to extract the inCommitTimestamp of delta files, this is used in
    // ActionsIterator to resolve the timestamp when available
    copySet.add(DeltaLogActionUtils.DeltaAction.COMMITINFO);
    // Determine whether the additional actions were in the original set.
    this.shouldDropProtocolColumn = !actionSet.contains(DeltaLogActionUtils.DeltaAction.PROTOCOL);
    this.shouldDropCommitInfoColumn =
        !actionSet.contains(DeltaLogActionUtils.DeltaAction.COMMITINFO);

    this.readSchema =
        new StructType(
            copySet.stream()
                .map(action -> new StructField(action.colName, action.schema, true))
                .collect(Collectors.toList()));
    this.engine = engine;
    this.peekableIterator = getNewIterator();

    // Extract version and timestamp from first action (or use fallback)
    Optional<ActionWrapper> firstWrapper = peekableIterator.peek();
    if (firstWrapper.isPresent()) {
      this.version = firstWrapper.get().getVersion();
      this.timestamp =
          firstWrapper
              .get()
              .getTimestamp()
              .orElseThrow(
                  () -> new RuntimeException("timestamp should always exist for Delta File"));
    } else {
      // Empty commit file - from file
      this.version = FileNames.deltaVersion(new Path(commitFile.getPath()));
      this.timestamp = commitFile.getModificationTime();
    }
  }

  private PeekableIterator<ActionWrapper> getNewIterator() {
    CloseableIterator<ActionWrapper> actionsIter =
        new ActionsIterator(
            engine, Collections.singletonList(commitFile), readSchema, Optional.empty());
    return new PeekableIterator<>(actionsIter);
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public synchronized CloseableIterator<ColumnarBatch> getActions() {
    CloseableIterator<ColumnarBatch> result =
        peekableIterator.map(
            wrapper ->
                validateProtocolAndDropInternalColumns(
                    wrapper.getColumnarBatch(),
                    tablePath,
                    shouldDropProtocolColumn,
                    shouldDropCommitInfoColumn));
    peekableIterator = getNewIterator();
    return result;
  }

  /** Validates protocol and drops protocol/commitInfo columns if not requested. */
  private static ColumnarBatch validateProtocolAndDropInternalColumns(
      ColumnarBatch batch,
      String tablePath,
      boolean shouldDropProtocolColumn,
      boolean shouldDropCommitInfoColumn) {

    // Validate protocol if present in the batch.
    int protocolIdx = batch.getSchema().indexOf("protocol");
    Preconditions.checkState(protocolIdx >= 0, "protocol column must be present in readSchema");
    ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
    for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
      if (!protocolVector.isNullAt(rowId)) {
        Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
        TableFeatures.validateKernelCanReadTheTable(protocol, tablePath);
      }
    }

    // Drop columns if not requested
    ColumnarBatch result = batch;
    if (shouldDropProtocolColumn && protocolIdx >= 0) {
      result = result.withDeletedColumnAt(protocolIdx);
    }

    int commitInfoIdx = result.getSchema().indexOf("commitInfo");
    if (shouldDropCommitInfoColumn && commitInfoIdx >= 0) {
      result = result.withDeletedColumnAt(commitInfoIdx);
    }

    return result;
  }
}
