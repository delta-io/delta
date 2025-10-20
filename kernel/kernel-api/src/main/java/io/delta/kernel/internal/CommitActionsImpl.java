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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.Collections;
import java.util.Optional;

/**
 * Implementation of {@link CommitActions}.
 *
 * <p>This implementation owns the commit file and supports multiple calls to {@link #getActions()}.
 * The first call reuses initially-read data to avoid double I/O, while subsequent calls re-read the
 * commit file for memory efficiency.
 *
 * <p>This class implements {@link AutoCloseable} to allow callers to explicitly release cached
 * resources. If {@link #close()} is not called, resources will be released when the object is
 * garbage collected or when {@link #getActions()} is called for the first time.
 */
public class CommitActionsImpl implements CommitActions, AutoCloseable {

  private final long version;
  private final long timestamp;
  private final Engine engine;
  private final FileStatus commitFile;
  private final StructType readSchema;
  private final String tablePath;
  private final boolean shouldDropProtocolColumn;
  private final boolean shouldDropCommitInfoColumn;

  private CloseableIterator<ColumnarBatch> firstReadCache;
  private boolean firstCallDone = false;

  /**
   * Creates a CommitActions from a commit file.
   *
   * @param engine the engine for file I/O
   * @param commitFile the commit file to read
   * @param readSchema the schema to use when reading the file
   * @param tablePath the table path for error messages
   * @param shouldDropProtocolColumn whether to drop the protocol column
   * @param shouldDropCommitInfoColumn whether to drop the commitInfo column
   */
  public CommitActionsImpl(
      Engine engine,
      FileStatus commitFile,
      StructType readSchema,
      String tablePath,
      boolean shouldDropProtocolColumn,
      boolean shouldDropCommitInfoColumn) {
    this.engine = requireNonNull(engine, "engine cannot be null");
    this.commitFile = requireNonNull(commitFile, "commitFile cannot be null");
    this.readSchema = requireNonNull(readSchema, "readSchema cannot be null");
    this.tablePath = requireNonNull(tablePath, "tablePath cannot be null");
    this.shouldDropProtocolColumn = shouldDropProtocolColumn;
    this.shouldDropCommitInfoColumn = shouldDropCommitInfoColumn;

    // Read file once to extract version and timestamp
    CloseableIterator<ActionWrapper> wrappers =
        new ActionsIterator(
            engine, Collections.singletonList(commitFile), readSchema, Optional.empty());
    ;
    if (!wrappers.hasNext()) {
      // Empty commit file - use fallback
      this.version = FileNames.deltaVersion(new Path(commitFile.getPath()));
      this.timestamp = commitFile.getModificationTime();
      this.firstReadCache = toCloseableIterator(Collections.emptyIterator());
    } else {
      ActionWrapper firstWrapper = wrappers.next();
      this.version = firstWrapper.getVersion();
      this.timestamp =
          firstWrapper
              .getTimestamp()
              .orElseThrow(
                  () -> new RuntimeException("timestamp should always exist for Delta File"));

      // Process first batch and cache the iterator
      ColumnarBatch firstBatch =
          TableChangesUtils.processAndDropColumns(
              firstWrapper.getColumnarBatch(),
              tablePath,
              shouldDropProtocolColumn,
              shouldDropCommitInfoColumn);

      this.firstReadCache =
          toCloseableIterator(Collections.singletonList(firstBatch).iterator())
              .combine(
                  wrappers.map(
                      wrapper ->
                          TableChangesUtils.processAndDropColumns(
                              wrapper.getColumnarBatch(),
                              tablePath,
                              shouldDropProtocolColumn,
                              shouldDropCommitInfoColumn)));
    }
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
    if (!firstCallDone) {
      firstCallDone = true;
      // First call: return cached iterator and clear internal reference
      // Caller is now responsible for closing this iterator
      CloseableIterator<ColumnarBatch> result = firstReadCache;
      firstReadCache = null; // Clear reference - caller owns it now
      return result;
    } else {
      // Subsequent calls: re-read from file for memory efficiency
      return new ActionsIterator(
              engine, Collections.singletonList(commitFile), readSchema, Optional.empty())
          .map(
              wrapper ->
                  TableChangesUtils.processAndDropColumns(
                      wrapper.getColumnarBatch(),
                      tablePath,
                      shouldDropProtocolColumn,
                      shouldDropCommitInfoColumn));
    }
  }

  /**
   * Closes this CommitActions and releases any cached resources.
   *
   * <p>This method is safe to call multiple times. If {@link #getActions()} has already been
   * called, this method does nothing as the cached iterator ownership has been transferred to the
   * caller.
   *
   * <p>Calling {@link #close()} is optional. If not called, resources will be released when {@link
   * #getActions()} is called for the first time, or when the object is garbage collected.
   */
  @Override
  public synchronized void close() {
    if (firstReadCache != null) {
      try {
        firstReadCache.close();
      } catch (Exception e) {
        // Log but don't throw - close() should be best-effort
      }
      firstReadCache = null;
    }
  }
}
