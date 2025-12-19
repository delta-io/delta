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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaStartingVersion;
import org.apache.spark.sql.delta.StartingVersion;
import org.apache.spark.sql.delta.StartingVersionLatest$;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset$;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FilePartition$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkMicroBatchStream
    implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private static final Set<DeltaAction> ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE)));

  private final Engine engine;
  private final DeltaSnapshotManager snapshotManager;
  private final DeltaOptions options;
  private final Snapshot snapshotAtSourceInit;
  private final String tableId;
  private final boolean shouldValidateOffsets;
  private final SparkSession spark;
  private final String tablePath;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Filter[] dataFilters;
  private final Configuration hadoopConf;
  private final SQLConf sqlConf;
  private final scala.collection.immutable.Map<String, String> scalaOptions;

  /**
   * Tracks whether this is the first batch for this stream (no checkpointed offset).
   *
   * <p>- First batch: initialOffset() -> latestOffset(Offset, ReadLimit) - Set `isFirstBatch` to
   * true in initialOffset() - in latestOffset(Offset, ReadLimit), use `isFirstBatch` to determine
   * whether to return null vs previousOffset (when no data is available) - set `isFirstBatch` to
   * false - Subsequent batches: latestOffset(Offset, ReadLimit)
   */
  private boolean isFirstBatch = false;

  /**
   * When AvailableNow is used, this offset will be the upper bound where this run of the query will
   * process up. We may run multiple micro batches, but the query will stop itself when it reaches
   * this offset.
   */
  private Optional<DeltaSourceOffset> lastOffsetForTriggerAvailableNow = Optional.empty();

  private boolean isLastOffsetForTriggerAvailableNowInitialized = false;

  private boolean isTriggerAvailableNow = false;

  public SparkMicroBatchStream(
      DeltaSnapshotManager snapshotManager,
      Snapshot snapshotAtSourceInit,
      Configuration hadoopConf,
      SparkSession spark,
      DeltaOptions options,
      String tablePath,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    this.spark = Objects.requireNonNull(spark, "spark is null");
    this.engine = DefaultEngine.create(hadoopConf);
    this.options = Objects.requireNonNull(options, "options is null");
    // Normalize tablePath to ensure it ends with "/" for consistent path construction
    String normalizedTablePath = Objects.requireNonNull(tablePath, "tablePath is null");
    this.tablePath =
        normalizedTablePath.endsWith("/") ? normalizedTablePath : normalizedTablePath + "/";
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.dataFilters =
        Arrays.copyOf(
            Objects.requireNonNull(dataFilters, "dataFilters is null"), dataFilters.length);
    this.sqlConf = SQLConf.get();
    this.scalaOptions = Objects.requireNonNull(scalaOptions, "scalaOptions is null");

    this.snapshotAtSourceInit = snapshotAtSourceInit;
    this.tableId = ((SnapshotImpl) snapshotAtSourceInit).getMetadata().getId();
    this.shouldValidateOffsets =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION());
  }

  @Override
  public void prepareForTriggerAvailableNow() {
    logger.info("The streaming query reports to use Trigger.AvailableNow.");
    isTriggerAvailableNow = true;
  }

  /**
   * initialize the internal states for AvailableNow if this method is called first time after
   * prepareForTriggerAvailableNow.
   */
  private void initForTriggerAvailableNowIfNeeded(DeltaSourceOffset startOffsetOpt) {
    if (isTriggerAvailableNow && !isLastOffsetForTriggerAvailableNowInitialized) {
      isLastOffsetForTriggerAvailableNowInitialized = true;
      initLastOffsetForTriggerAvailableNow(startOffsetOpt);
    }
  }

  private void initLastOffsetForTriggerAvailableNow(DeltaSourceOffset startOffsetOpt) {
    lastOffsetForTriggerAvailableNow =
        latestOffsetInternal(startOffsetOpt, ReadLimit.allAvailable());

    lastOffsetForTriggerAvailableNow.ifPresent(
        lastOffset ->
            logger.info("lastOffset for Trigger.AvailableNow has set to " + lastOffset.json()));
  }

  ////////////
  // offset //
  ////////////

  /**
   * Returns the initial offset for a streaming query to start reading from (if there's no
   * checkpointed offset).
   */
  @Override
  public Offset initialOffset() {
    Optional<Long> startingVersionOpt = getStartingVersion();
    long version;
    boolean isInitialSnapshot;
    isFirstBatch = true;

    if (startingVersionOpt.isPresent()) {
      version = startingVersionOpt.get();
      isInitialSnapshot = false;
    } else {
      // TODO(#5318): Support initial snapshot case (isInitialSnapshot == true)
      throw new UnsupportedOperationException(
          "initialOffset with initial snapshot is not supported yet");
    }

    return DeltaSourceOffset.apply(
        tableId, version, DeltaSourceOffset.BASE_INDEX(), isInitialSnapshot);
  }

  @Override
  public Offset latestOffset() {
    throw new IllegalStateException(
        "latestOffset() should not be called - use latestOffset(Offset, ReadLimit) instead");
  }

  /**
   * Get the latest offset with rate limiting (SupportsAdmissionControl).
   *
   * @param startOffset The starting offset
   * @param limit The read limit for rate limiting
   * @return The latest offset, or null if no data is available to read.
   */
  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    Objects.requireNonNull(startOffset, "startOffset should not be null for MicroBatchStream");
    Objects.requireNonNull(limit, "limit should not be null for MicroBatchStream");

    DeltaSourceOffset deltaStartOffset = DeltaSourceOffset.apply(tableId, startOffset);
    initForTriggerAvailableNowIfNeeded(deltaStartOffset);
    // Return null when no data is available for this batch.
    DeltaSourceOffset endOffset = latestOffsetInternal(deltaStartOffset, limit).orElse(null);
    isFirstBatch = false;
    return endOffset;
  }

  /**
   * Internal implementation of latestOffset using DeltaSourceOffset directly, without null checks
   * and state management.
   */
  private Optional<DeltaSourceOffset> latestOffsetInternal(
      DeltaSourceOffset deltaStartOffset, ReadLimit limit) {
    Optional<DeltaSource.AdmissionLimits> limits =
        ScalaUtils.toJavaOptional(DeltaSource.AdmissionLimits$.MODULE$.apply(options, limit));
    Optional<DeltaSourceOffset> endOffset =
        getNextOffsetFromPreviousOffset(deltaStartOffset, limits, isFirstBatch);

    if (shouldValidateOffsets && endOffset.isPresent()) {
      DeltaSourceOffset.validateOffsets(deltaStartOffset, endOffset.get());
    }

    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return DeltaSourceOffset$.MODULE$.apply(tableId, json);
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
   * @param isFirstBatch Whether this is the first batch for this stream
   * @return The next offset, or the previous offset if no new data is available (except on the
   *     initial batch where we return empty to match DSv1's
   *     getStartingOffsetFromSpecificDeltaVersion behavior)
   */
  private Optional<DeltaSourceOffset> getNextOffsetFromPreviousOffset(
      DeltaSourceOffset previousOffset,
      Optional<DeltaSource.AdmissionLimits> limits,
      boolean isFirstBatch) {
    // TODO(#5319): Special handling for schema tracking.

    CloseableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            previousOffset.reservoirVersion(),
            previousOffset.index(),
            previousOffset.isInitialSnapshot(),
            limits);

    Optional<IndexedFile> lastFileChange = Utils.iteratorLast(changes);

    if (!lastFileChange.isPresent()) {
      // For the first batch, return empty to match DSv1's
      // getStartingOffsetFromSpecificDeltaVersion
      if (isFirstBatch) {
        return Optional.empty();
      }
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
    DeltaSourceOffset startOffset = (DeltaSourceOffset) start;
    DeltaSourceOffset endOffset = (DeltaSourceOffset) end;

    long fromVersion = startOffset.reservoirVersion();
    long fromIndex = startOffset.index();
    boolean isInitialSnapshot = startOffset.isInitialSnapshot();

    List<PartitionedFile> partitionedFiles = new ArrayList<>();
    long totalBytesToRead = 0;
    try (CloseableIterator<IndexedFile> fileChanges =
        getFileChanges(fromVersion, fromIndex, isInitialSnapshot, Optional.of(endOffset))) {
      while (fileChanges.hasNext()) {
        IndexedFile indexedFile = fileChanges.next();
        if (!indexedFile.hasFileAction() || indexedFile.getAddFile() == null) {
          continue;
        }
        AddFile addFile = indexedFile.getAddFile();
        PartitionedFile partitionedFile =
            PartitionUtils.buildPartitionedFile(
                addFile, partitionSchema, tablePath, ZoneId.of(sqlConf.sessionLocalTimeZone()));

        totalBytesToRead += addFile.getSize();
        partitionedFiles.add(partitionedFile);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to get file changes for table %s from version %d index %d to offset %s",
              tablePath, fromVersion, fromIndex, endOffset),
          e);
    }

    long maxSplitBytes =
        PartitionUtils.calculateMaxSplitBytes(
            spark, totalBytesToRead, partitionedFiles.size(), sqlConf);
    // Partitions files into Spark FilePartitions.
    Seq<FilePartition> filePartitions =
        FilePartition$.MODULE$.getFilePartitions(
            spark, JavaConverters.asScalaBuffer(partitionedFiles).toSeq(), maxSplitBytes);
    return JavaConverters.seqAsJavaList(filePartitions).toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return PartitionUtils.createDeltaParquetReaderFactory(
        snapshotAtSourceInit,
        dataSchema,
        partitionSchema,
        readDataSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf);
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    // TODO(#5319): update metadata tracking log.
  }

  @Override
  public void stop() {
    // TODO(#5318): unpersist any cached initial snapshot.
  }

  ///////////////////////
  // getStartingVersion //
  ///////////////////////

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never be called.
   *
   * <p>Returns Optional.empty() if no starting version is provided.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.getStartingVersion.
   */
  Optional<Long> getStartingVersion() {
    // TODO(#5319): DeltaSource.scala uses `allowOutOfRange` parameter from
    // DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.
    if (options.startingVersion().isDefined()) {
      DeltaStartingVersion startingVersion = options.startingVersion().get();
      if (startingVersion instanceof StartingVersionLatest$) {
        Snapshot latestSnapshot = snapshotManager.loadLatestSnapshot();
        // "latest": start reading from the next commit
        return Optional.of(latestSnapshot.getVersion() + 1);
      } else if (startingVersion instanceof StartingVersion) {
        long version = ((StartingVersion) startingVersion).version();
        if (!validateProtocolAt(spark, snapshotManager, engine, version)) {
          // When starting from a given version, we don't require that the snapshot of this
          // version can be reconstructed, even though the input table is technically in an
          // inconsistent state. If the snapshot cannot be reconstructed, then the protocol
          // check is skipped, so this is technically not safe, but we keep it this way for
          // historical reasons.
          snapshotManager.checkVersionExists(
              version, /* mustBeRecreatable= */ false, /* allowOutOfRange= */ false);
        }
        return Optional.of(version);
      }
    }
    // TODO(#5319): Implement startingTimestamp support
    return Optional.empty();
  }

  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than unsupported feature exception, we suppress it. This allows fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.validateProtocolAt.
   *
   * <p>Returns true when the validation was performed and succeeded.
   */
  private static boolean validateProtocolAt(
      SparkSession spark, DeltaSnapshotManager snapshotManager, Engine engine, long version) {
    boolean alwaysValidateProtocol =
        (Boolean)
            spark
                .sessionState()
                .conf()
                .getConf(DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL());
    if (!alwaysValidateProtocol) {
      return false;
    }

    try {
      // Attempt to construct a snapshot at the startingVersion to validate the protocol
      // If snapshot reconstruction fails, fall back to old behavior where the only
      // requirement was for the commit to exist
      snapshotManager.loadSnapshotAt(version);
      return true;
    } catch (UnsupportedTableFeatureException e) {
      // Re-throw fatal unsupported table feature exceptions
      throw e;
    } catch (Exception e) {
      // Suppress non-fatal exceptions
      logger.warn("Protocol validation failed at version {} with: {}", version, e.getMessage());
      return false;
    }
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

    // If endOffset is provided, we are getting a batch on a constructed range so we should use
    // the endOffset as the limit.
    // Otherwise, we are looking for a new offset, so we try to use the latestOffset we found for
    // Trigger.availableNow() as limit. We know endOffset <= lastOffsetForTriggerAvailableNow.
    Optional<DeltaSourceOffset> lastOffsetForThisScan =
        endOffset.or(() -> lastOffsetForTriggerAvailableNow);

    // Check end boundary (inclusive)
    if (lastOffsetForThisScan.isPresent()) {
      DeltaSourceOffset bound = lastOffsetForThisScan.get();
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
    Optional<Long> endVersionOpt =
        endOffset.isPresent() ? Optional.of(endOffset.get().reservoirVersion()) : Optional.empty();

    CommitRange commitRange;
    try {
      commitRange = snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
    } catch (io.delta.kernel.exceptions.CommitRangeNotFoundException e) {
      // If the requested version range doesn't exist (e.g., we're asking for version 6 when
      // the table only has versions 0-5).
      return Utils.toCloseableIterator(allIndexedFiles.iterator());
    }

    // Use getActionsFromRangeUnsafe instead of CommitRange.getActions() because:
    // 1. CommitRange.getActions() requires a snapshot at exactly the startVersion, but when
    //    startingVersion option is used, we may not be able to recreate that exact snapshot
    //    (e.g., if log files have been cleaned up after checkpointing).
    // 2. This matches DSv1 behavior which uses snapshotAtSourceInit's P&M to interpret all
    //    AddFile actions and performs per-commit protocol validation.
    try (CloseableIterator<ColumnarBatch> actionsIter =
        StreamingHelper.getActionsFromRangeUnsafe(
            engine,
            (io.delta.kernel.internal.commitrange.CommitRangeImpl) commitRange,
            snapshotAtSourceInit.getPath(),
            ACTION_SET)) {
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
        validateCommit(batch, version, snapshotAtSourceInit.getPath(), endOffset);

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
      ColumnarBatch batch,
      long version,
      String tablePath,
      Optional<DeltaSourceOffset> endOffsetOpt) {
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
