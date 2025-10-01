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
package io.delta.kernel.spark.utils;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;

/**
 * Helper class for managing Delta table snapshots in streaming scenarios.
 *
 * <p>This class provides utilities to load, update, and query Delta table snapshots using the Delta
 * Kernel API. It maintains a cached snapshot that can be accessed and updated as needed.
 */
@Experimental
public class StreamingHelper {

  /** Sentinel index values matching DeltaSourceOffset */
  private static final long BASE_INDEX = -100L;

  private static final long END_INDEX = Long.MAX_VALUE - 100L;

  /**
   * Standard action set for reading file changes.
   *
   * <p>Only includes ADD and REMOVE actions. PROTOCOL is automatically added and validated by
   * Kernel's getActionsFromCommitFilesWithProtocolValidation(). METADATA is not needed for basic
   * file tracking.
   */
  private static final Set<DeltaLogActionUtils.DeltaAction> STANDARD_ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  DeltaLogActionUtils.DeltaAction.ADD, DeltaLogActionUtils.DeltaAction.REMOVE)));

  private final String tablePath;
  private final AtomicReference<Snapshot> snapshotAtomicReference;
  private final Engine kernelEngine;

  /**
   * Constructs a new StreamingHelper for the specified Delta table.
   *
   * @param tablePath the path to the Delta table
   * @param hadoopConf the Hadoop configuration to use for file system access
   */
  public StreamingHelper(String tablePath, Configuration hadoopConf) {
    this.tablePath = tablePath;
    this.snapshotAtomicReference = new AtomicReference<>();
    this.kernelEngine = DefaultEngine.create(hadoopConf);
  }

  /**
   * Returns the cached snapshot without guaranteeing its freshness.
   *
   * @return the cached snapshot, or a newly loaded snapshot if none exists
   */
  public Snapshot unsafeVolatileSnapshot() {
    Snapshot unsafeVolatileSnapshot = snapshotAtomicReference.get();
    if (unsafeVolatileSnapshot == null) {
      return loadLatestSnapshot();
    }
    return unsafeVolatileSnapshot;
  }

  /**
   * Loads and caches the latest snapshot of the Delta table.
   *
   * @return the newly loaded snapshot
   */
  public Snapshot loadLatestSnapshot() {
    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
    snapshotAtomicReference.set(snapshot);
    return snapshot;
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p>This method searches the Delta table's commit history to find the commit that was active at
   * the specified timestamp.
   *
   * @param timeStamp the timestamp to query
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *     commits
   * @param mustBeRecreatable if true, only considers commits that can be recreated (i.e., all
   *     necessary log files are available)
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *     all commits
   * @return the commit that was active at the specified timestamp
   */
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timeStamp.getTime(),
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>());
  }

  /**
   * Checks if a specific version of the Delta table exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *     available log files
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *     throwing an exception
   * @throws VersionNotFoundException if the version is not available
   */
  public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws VersionNotFoundException {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/);

    long latest = snapshot.getVersion();
    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  /**
   * This method retrieves AddFile and RemoveFile actions from Delta commits within the specified
   * range. The logic is similar to the getFileChanges method in DeltaSource.scala.
   *
   * <p>When isInitialSnapshot=true, it first reads the snapshot files at fromVersion using
   * Snapshot.getScanBuilder().build(), then reads commits from fromVersion + 1 onwards. When
   * isInitialSnapshot=false, it only reads commits starting from fromVersion.
   *
   * @param fromVersion The starting version (inclusive) to read changes from
   * @param fromIndex The starting index within the fromVersion to filter results. Only actions with
   *     index > fromIndex will be returned
   * @param isInitialSnapshot If true, reads snapshot at fromVersion first, then commits from
   *     fromVersion + 1. If false, reads commits starting from fromVersion.
   * @param endVersion Optional end version (inclusive). If not provided, reads to the latest
   *     available version
   * @return A CloseableIterator of IndexedFileAction containing the file changes in the range
   */
  public CloseableIterator<IndexedFileAction> getFileChanges(
      long fromVersion, long fromIndex, boolean isInitialSnapshot, Optional<Long> endVersion) {

    // If initial snapshot, read snapshot at fromVersion, otherwise use empty iterator
    CloseableIterator<IndexedFileAction> snapshotIterator =
        isInitialSnapshot
            ? getSnapshotFiles(fromVersion)
            : io.delta.kernel.utils.CloseableIterable.<IndexedFileAction>emptyIterable().iterator();

    // Start reading commits from fromVersion+1 if initial snapshot, otherwise from fromVersion
    long commitStartVersion = isInitialSnapshot ? fromVersion + 1 : fromVersion;
    CloseableIterator<IndexedFileAction> commitIterator =
        (commitStartVersion <= endVersion.orElse(Long.MAX_VALUE))
            ? getCommits(commitStartVersion, endVersion)
            : io.delta.kernel.utils.CloseableIterable.<IndexedFileAction>emptyIterable().iterator();

    // Combine snapshot and commits
    CloseableIterator<IndexedFileAction> rawIterator = snapshotIterator.combine(commitIterator);

    // Filter actions to only include those after fromVersion/fromIndex
    // This matches DeltaSource.getFileChanges behavior (line 854-858):
    // iter.withClose { it => it.filter { file =>
    //   file.version > fromVersion || file.index > fromIndex
    // }}
    return rawIterator.filter(
        action ->
            action.getVersion() > fromVersion
                || (action.getVersion() == fromVersion && action.getIndex() > fromIndex));
  }

  /**
   * Gets file changes from commits in the specified version range.
   *
   * <p>This method returns all actions from the specified version range without any filtering.
   */
  /**
   * State machine for processing commits with sentinel values.
   *
   * <p>State transitions: ITER_START → NEED_BEGIN_INDEX → NEED_FILES → NEED_END_INDEX →
   * NEED_BEGIN_INDEX (loop)
   */
  private enum CommitProcessingState {
    ITER_START, // Initial state
    NEED_BEGIN_INDEX, // Emit BEGIN_INDEX sentinel
    NEED_FILES, // Process file actions
    NEED_END_INDEX // Emit END_INDEX sentinel
  }

  private CloseableIterator<IndexedFileAction> getCommits(
      long fromVersion, Optional<Long> endVersion) {

    // Build the commit range
    CommitRangeBuilder builder =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(fromVersion));
    endVersion.ifPresent(
        version -> builder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(version)));
    CommitRange commitRange = builder.build(kernelEngine);

    // Load the start snapshot (required by getActions API)
    Snapshot startSnapshot = loadSnapshotAt(commitRange.getStartVersion());

    CloseableIterator<Row> allRowsIter =
        io.delta.kernel.internal.util.Utils.intoRows(
            commitRange
                .getActions(kernelEngine, startSnapshot, STANDARD_ACTION_SET)
                .map(batch -> new FilteredColumnarBatch(batch, Optional.empty())));

    // Wrap with peekable to detect version boundaries
    final PeekableCloseableIterator<Row> peekableRows =
        new PeekableCloseableIterator<>(allRowsIter);

    // Convert rows to IndexedFileAction with sentinel values
    return new CloseableIterator<IndexedFileAction>() {
      private long currentVersion = -1;
      private long currentIndex = -1;
      private CommitProcessingState state = CommitProcessingState.ITER_START;
      private IndexedFileAction nextAction = null;

      @Override
      public boolean hasNext() {
        if (nextAction != null) {
          return true;
        }

        while (true) {
          // Use state machine to determine what to emit
          switch (state) {
            case ITER_START:
              // Initial state - transition to NEED_BEGIN_INDEX
              state = CommitProcessingState.NEED_BEGIN_INDEX;
              continue;

            case NEED_BEGIN_INDEX:
              // Emit BEGIN_INDEX sentinel for a new version
              if (!peekableRows.hasNext()) {
                return false; // No more data
              }
              // Peek to get the next version
              Optional<Row> peeked = peekableRows.peek();
              if (peeked.isPresent()) {
                Row row = peeked.get();
                currentVersion = getVersion(row);
                currentIndex = -1;
                nextAction = new IndexedFileAction(currentVersion, BASE_INDEX, null, null);
                state = CommitProcessingState.NEED_FILES;
                return true;
              }
              return false;

            case NEED_FILES:
              // Process files for current version
              if (!peekableRows.hasNext()) {
                // No more rows - transition to NEED_END_INDEX
                state = CommitProcessingState.NEED_END_INDEX;
                continue;
              }

              // Peek to check if version changed
              Optional<Row> nextRow = peekableRows.peek();
              if (nextRow.isPresent()) {
                Row row = nextRow.get();
                long rowVersion = getVersion(row);

                if (rowVersion != currentVersion) {
                  // Version changed - transition to NEED_END_INDEX
                  state = CommitProcessingState.NEED_END_INDEX;
                  continue;
                }

                // Same version - consume and process the row
                peekableRows.next(); // Consume it
                if (processRow(row)) {
                  return true; // Found an action
                }
                // No action in this row, continue loop
              } else {
                // peek returned empty - no more data
                state = CommitProcessingState.NEED_END_INDEX;
                continue;
              }
              break;

            case NEED_END_INDEX:
              // Emit END_INDEX sentinel for current version (unless it's the endVersion)
              nextAction = new IndexedFileAction(currentVersion, END_INDEX, null, null);
              state = CommitProcessingState.NEED_BEGIN_INDEX; // Loop back for next version
              return true;
          }
        }
      }

      private long getVersion(Row row) {
        return row.getLong(row.getSchema().indexOf("version"));
      }

      private boolean processRow(Row row) {
        StructType schema = row.getSchema();
        int addIdx = schema.indexOf("add");
        int removeIdx = schema.indexOf("remove");

        Row addRow = null;
        Row removeRow = null;

        if (addIdx >= 0 && !row.isNullAt(addIdx)) {
          addRow = row.getStruct(addIdx);
        }
        if (removeIdx >= 0 && !row.isNullAt(removeIdx)) {
          removeRow = row.getStruct(removeIdx);
        }

        if (addRow != null || removeRow != null) {
          currentIndex++;
          nextAction = new IndexedFileAction(currentVersion, currentIndex, addRow, removeRow);
          return true;
        }
        return false; // No action in this row
      }

      @Override
      public IndexedFileAction next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException();
        }
        IndexedFileAction result = nextAction;
        nextAction = null;
        return result;
      }

      @Override
      public void close() {
        try {
          peekableRows.close();
        } catch (Exception e) {
          throw new RuntimeException("Error closing peekable iterator", e);
        }
      }
    };
  }

  /**
   * Gets snapshot files at a specific version, converting them to IndexedFileAction with proper
   * indexing.
   *
   * <p>This is similar to DeltaSourceSnapshot.iterator() in Spark Delta Source.
   *
   * <p>This method adds BEGIN_INDEX and END_INDEX sentinel values, similar to DeltaSource's
   * addBeginAndEndIndexOffsetsForVersion method.
   *
   * <p>Uses combine() to chain BEGIN_INDEX, actual files, and END_INDEX.
   */
  private CloseableIterator<IndexedFileAction> getSnapshotFiles(long version) {
    Snapshot snapshot = loadSnapshotAt(version);
    Scan scan = snapshot.getScanBuilder().build();

    // Get scan files (AddFile actions from the snapshot)
    CloseableIterator<FilteredColumnarBatch> scanFilesIter = scan.getScanFiles(kernelEngine);

    // Sort files by modificationTime and path (matching DeltaSource behavior)
    CloseableIterator<Row> sortedAddFiles =
        LocalFileActionSorter.INSTANCE.sortScanFiles(scanFilesIter);

    // BEGIN_INDEX sentinel
    CloseableIterator<IndexedFileAction> beginIter =
        singletonCloseableIterator(new IndexedFileAction(version, BASE_INDEX, null, null));

    // Sorted files with index
    CloseableIterator<IndexedFileAction> filesIter =
        new CloseableIterator<IndexedFileAction>() {
          private long currentIndex = -1;

          @Override
          public boolean hasNext() {
            return sortedAddFiles.hasNext();
          }

          @Override
          public IndexedFileAction next() {
            if (!hasNext()) {
              throw new java.util.NoSuchElementException();
            }
            currentIndex++;
            Row addFileRow = sortedAddFiles.next();
            return new IndexedFileAction(version, currentIndex, addFileRow, null);
          }

          @Override
          public void close() {
            try {
              sortedAddFiles.close();
            } catch (Exception e) {
              throw new RuntimeException("Error closing sorted files iterator", e);
            }
          }
        };

    // END_INDEX sentinel
    CloseableIterator<IndexedFileAction> endIter =
        singletonCloseableIterator(new IndexedFileAction(version, END_INDEX, null, null));

    // Combine: BEGIN_INDEX → files → END_INDEX
    return beginIter.combine(filesIter).combine(endIter);
  }

  /**
   * Loads a snapshot at a specific version.
   *
   * @param version the version to load
   * @return the snapshot at the specified version
   */
  private Snapshot loadSnapshotAt(long version) {
    return TableManager.loadSnapshot(tablePath).atVersion(version).build(kernelEngine);
  }
}
