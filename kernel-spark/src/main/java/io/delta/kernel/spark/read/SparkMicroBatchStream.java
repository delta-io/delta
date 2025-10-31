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
package io.delta.kernel.spark.read;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.kernel.spark.utils.StreamingHelper;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;

public class SparkMicroBatchStream implements MicroBatchStream, SupportsAdmissionControl {

  private static final Set<DeltaAction> ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE)));

  private final Engine engine;
  private final String tablePath;
  private final DeltaOptions options;
  private final String tableId;
  private final boolean shouldValidateOffsets;
  private final SparkSession spark;
  private final StreamingHelper streamingHelper;
  private final Snapshot snapshotAtSourceInit;

  /** Convenience constructor for testing. */
  public SparkMicroBatchStream(String tablePath, Configuration hadoopConf) {
    this(
        tablePath,
        hadoopConf,
        SparkSession.active(),
        new DeltaOptions(
            scala.collection.immutable.Map$.MODULE$.empty(),
            SparkSession.active().sessionState().conf()));
  }

  public SparkMicroBatchStream(
      String tablePath, Configuration hadoopConf, SparkSession spark, DeltaOptions options) {
    this.spark = spark;
    this.tablePath = tablePath;
    this.engine = DefaultEngine.create(hadoopConf);
    this.options = options;
    this.streamingHelper = new StreamingHelper(tablePath, hadoopConf);

    // Initialize snapshot at source init to get table ID, similar to DeltaSource.scala
    this.snapshotAtSourceInit = streamingHelper.loadLatestSnapshot();
    this.tableId = ((SnapshotImpl) snapshotAtSourceInit).getMetadata().getId();

    this.shouldValidateOffsets =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION());
  }

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    // TODO(#5318): Implement initialOffset
    throw new UnsupportedOperationException("initialOffset is not supported");
  }

  @Override
  public Offset latestOffset() {
    // TODO(#5318): Implement latestOffset with proper start offset and limit
    throw new UnsupportedOperationException(
        "latestOffset() should not be called - use latestOffset(Offset, ReadLimit) instead");
  }

  /**
   * Get the latest offset with rate limiting (SupportsAdmissionControl).
   *
   * @param startOffset The starting offset (can be null if initialOffset() returned null)
   * @param limit The read limit for rate limiting
   * @return The latest offset, or null if no data is available to read.
   */
  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    // For the first batch, initialOffset() should be called before latestOffset().
    // if startOffset is null: no data is available to read.
    if (startOffset == null) {
      return null;
    }
    // TODO(#5318): init trigger available now support

    DeltaSourceOffset deltaStartOffset = DeltaSourceOffset.apply(tableId, startOffset);
    Optional<DeltaSource.AdmissionLimits> limits =
        ScalaUtils.toJavaOptional(DeltaSource.AdmissionLimits$.MODULE$.apply(options, limit));
    Optional<DeltaSourceOffset> endOffset =
        getNextOffsetFromPreviousOffset(deltaStartOffset, limits);

    if (shouldValidateOffsets && endOffset.isPresent()) {
      DeltaSourceOffset.validateOffsets(deltaStartOffset, endOffset.get());
    }

    // endOffset is null: no data is available to read for this batch.
    return endOffset.orElse(null);
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  @Override
  public ReadLimit getDefaultReadLimit() {
    return DeltaSource.AdmissionLimits$.MODULE$.toReadLimit(options);
  }

  /**
   * Return the next offset when previous offset exists. Mimics
   * DeltaSource.getNextOffsetFromPreviousOffset.
   *
   * @param previousOffset The previous offset
   * @param limits Rate limits for this batch (Optional.empty() for no limits)
   * @return The next offset, or the previous offset if no new data is available
   */
  private Optional<DeltaSourceOffset> getNextOffsetFromPreviousOffset(
      DeltaSourceOffset previousOffset, Optional<DeltaSource.AdmissionLimits> limits) {
    // TODO(#5319): Special handling for schema tracking.

    CloseableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            previousOffset.reservoirVersion(),
            previousOffset.index(),
            previousOffset.isInitialSnapshot(),
            limits);

    Optional<IndexedFile> lastFileChange = StreamingHelper.iteratorLast(changes);

    if (!lastFileChange.isPresent()) {
      return Optional.of(previousOffset);
    }
    // TODO(#5318): Check read-incompatible schema changes during stream start
    IndexedFile lastFile = lastFileChange.get();
    return Optional.of(
        DeltaSource.buildOffsetFromIndexedFile(
            tableId,
            lastFile.getVersion(),
            lastFile.getIndex(),
            previousOffset.reservoirVersion(),
            previousOffset.isInitialSnapshot()));
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }

  ////////////////////
  // getFileChanges //
  ////////////////////

  /**
   * Get file changes with rate limiting applied. Mimics DeltaSource.getFileChangesWithRateLimit.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param limits Rate limits to apply (Optional.empty() for no limits)
   * @return An iterator of IndexedFile with rate limiting applied
   */
  CloseableIterator<IndexedFile> getFileChangesWithRateLimit(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits) {
    // TODO(#5319): getFileChangesForCDC if CDC is enabled.

    CloseableIterator<IndexedFile> changes =
        getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, /* endOffset= */ Optional.empty());

    // Take each change until we've seen the configured number of addFiles. Some changes don't
    // represent file additions; we retain them for offset tracking, but they don't count toward
    // the maxFilesPerTrigger conf.
    if (limits.isPresent()) {
      DeltaSource.AdmissionLimits admissionLimits = limits.get();
      changes = changes.takeWhile(admissionLimits::admit);
    }

    // TODO(#5318): Stop at schema change barriers
    return changes;
  }

  /**
   * Get file changes between fromVersion/fromIndex and endOffset. This is the Kernel-based
   * implementation of DeltaSource.getFileChanges.
   *
   * <p>Package-private for testing.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param endOffset The end offset (inclusive), or empty to read all available commits
   * @return An iterator of IndexedFile representing the file changes
   */
  CloseableIterator<IndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSourceOffset> endOffset) {

    CloseableIterator<IndexedFile> result;

    if (isInitialSnapshot) {
      // TODO(#5318): Implement initial snapshot
      throw new UnsupportedOperationException("initial snapshot is not supported yet");
    } else {
      result = filterDeltaLogs(fromVersion, endOffset);
    }

    // Check start boundary (exclusive)
    result =
        result.filter(
            file ->
                file.getVersion() > fromVersion
                    || (file.getVersion() == fromVersion && file.getIndex() > fromIndex));

    // Check end boundary (inclusive)
    if (endOffset.isPresent()) {
      DeltaSourceOffset bound = endOffset.get();
      result =
          result.takeWhile(
              file ->
                  file.getVersion() < bound.reservoirVersion()
                      || (file.getVersion() == bound.reservoirVersion()
                          && file.getIndex() <= bound.index()));
    }

    return result;
  }

  // TODO(#5318): implement lazy loading (one batch at a time).
  private CloseableIterator<IndexedFile> filterDeltaLogs(
      long startVersion, Optional<DeltaSourceOffset> endOffset) {
    List<IndexedFile> allIndexedFiles = new ArrayList<>();
    // StartBoundary (inclusive)
    CommitRangeBuilder builder =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion));
    if (endOffset.isPresent()) {
      // EndBoundary (inclusive)
      builder =
          builder.withEndBoundary(
              CommitRangeBuilder.CommitBoundary.atVersion(endOffset.get().reservoirVersion()));
    }
    CommitRange commitRange;
    try {
      commitRange = builder.build(engine);
    } catch (io.delta.kernel.exceptions.KernelException e) {
      // If the requested version range doesn't exist (e.g., we're asking for version 6 when
      // the table only has versions 0-5), return an empty iterator.
      // TODO(#5318): catch a fine-grained exception when kernel API is ready.
      if (e.getMessage() != null && e.getMessage().contains("no log files found")) {
        return Utils.toCloseableIterator(allIndexedFiles.iterator());
      }
      throw e;
    }
    // Required by kernel: perform protocol validation by creating a snapshot at startVersion.
    // TODO(#5318): This is not working with ccv2 table
    Snapshot startSnapshot =
        TableManager.loadSnapshot(tablePath).atVersion(startVersion).build(engine);
    try (CloseableIterator<ColumnarBatch> actionsIter =
        commitRange.getActions(engine, startSnapshot, ACTION_SET)) {
      // Each ColumnarBatch belongs to a single commit version,
      // but a single version may span multiple ColumnarBatches.
      long currentVersion = -1;
      long currentIndex = 0;
      List<IndexedFile> currentVersionFiles = new ArrayList<>();

      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        if (batch.getSize() == 0) {
          // TODO(#5318): this shouldn't happen, empty commits will still have a non-empty row
          // with the version set. Make sure the kernel API is explicit about this.
          continue;
        }
        long version = StreamingHelper.getVersion(batch);
        // When version changes, flush the completed version
        if (currentVersion != -1 && version != currentVersion) {
          flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
          currentVersionFiles.clear();
          currentIndex = 0;
        }

        // Validate the commit before processing files from this batch
        // TODO(#5318): migrate to kernel's commit-level iterator (WIP).
        // The current one-pass algorithm assumes REMOVE actions proceed ADD actions
        // in a commit; we should implement a proper two-pass approach once kernel API is ready.
        validateCommit(batch, version, endOffset);

        currentVersion = version;
        currentIndex =
            extractIndexedFilesFromBatch(batch, version, currentIndex, currentVersionFiles);
      }

      // Flush the last version
      if (currentVersion != -1) {
        flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read commit range", e);
    }
    // TODO(#5318): implement lazy loading (only load a batch into memory if needed).
    return Utils.toCloseableIterator(allIndexedFiles.iterator());
  }

  /**
   * Flushes a completed version by adding BEGIN/END sentinels around data files.
   *
   * <p>Sentinels are IndexedFiles with null addFile that mark version boundaries. They serve
   * several purposes:
   *
   * <ul>
   *   <li>Enable offset tracking at version boundaries (before any files or after all files)
   *   <li>Allow streaming to resume at the start or end of a version
   *   <li>Handle versions with only metadata/protocol changes (no data files)
   * </ul>
   *
   * <p>This mimics DeltaSource.addBeginAndEndIndexOffsetsForVersion
   */
  private void flushVersion(
      long version, List<IndexedFile> versionFiles, List<IndexedFile> output) {
    // Add BEGIN sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), /* addFile= */ null));
    // TODO(#5319): implement getMetadataOrProtocolChangeIndexedFileIterator.
    // Add all data files
    output.addAll(versionFiles);
    // Add END sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.END_INDEX(), /* addFile= */ null));
  }

  /**
   * Validates a commit and fail the stream if it's invalid. Mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @throws RuntimeException if the commit is invalid.
   */
  private void validateCommit(
      ColumnarBatch batch, long version, Optional<DeltaSourceOffset> endOffsetOpt) {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isPresent()) {
      DeltaSourceOffset endOffset = endOffsetOpt.get();
      if (endOffset.reservoirVersion() == version
          && endOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        return;
      }
    }
    int numRows = batch.getSize();
    // TODO(#5319): Implement ignoreChanges & skipChangeCommits & ignoreDeletes (legacy)
    // TODO(#5318): validate METADATA actions
    for (int rowId = 0; rowId < numRows; rowId++) {
      // RULE 1: If commit has RemoveFile(dataChange=true), fail this stream.
      Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
      if (removeOpt.isPresent()) {
        RemoveFile removeFile = removeOpt.get();
        Throwable error =
            DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFile.getPath(), tablePath);
        if (error instanceof RuntimeException) {
          throw (RuntimeException) error;
        } else {
          throw new RuntimeException(error);
        }
      }
    }
  }

  /**
   * Extracts IndexedFiles from a batch of actions for a given version and adds them to the output
   * list. Assigns an index to each IndexedFile.
   *
   * @return The next available index after processing this batch
   */
  private long extractIndexedFilesFromBatch(
      ColumnarBatch batch, long version, long startIndex, List<IndexedFile> output) {
    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      Optional<AddFile> addOpt = StreamingHelper.getDataChangeAdd(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        output.add(new IndexedFile(version, index++, addFile));
      }
    }

    return index;
  }
}
