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

import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_FEATURE;
import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
import org.apache.spark.sql.delta.sources.DeltaStreamUtils;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FilePartition$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.jdk.javaapi.CollectionConverters;

// TODO(#5318): Use DeltaErrors error framework for consistent error handling.
public class SparkMicroBatchStream
    implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private static final Set<DeltaAction> ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE, DeltaAction.METADATA)));

  private final Engine engine;
  private final DeltaSnapshotManager snapshotManager;
  private final DeltaOptions options;
  private final SnapshotImpl snapshotAtSourceInit;
  private final String tableId;
  private final StructType readSchemaAtSourceInit;
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
   * Configuration options for handling schema changes behavior. Controls unsafe operations like
   * column mapping changes, partition column changes, nullability changes, and type widening.
   */
  private DeltaStreamUtils.SchemaReadOptions schemaReadOptions;

  /**
   * A global flag to mark whether we have done a per-stream start check for column mapping schema
   * changes (rename / drop).
   */
  private volatile boolean hasCheckedReadIncompatibleSchemaChangesOnStreamStart = false;

  /**
   * When AvailableNow is used, this offset will be the upper bound where this run of the query will
   * process up. We may run multiple micro batches, but the query will stop itself when it reaches
   * this offset.
   */
  private Optional<DeltaSourceOffset> lastOffsetForTriggerAvailableNow = Optional.empty();

  private boolean isLastOffsetForTriggerAvailableNowInitialized = false;

  private boolean isTriggerAvailableNow = false;

  // Cached starting version to ensure idempotent behavior for "latest" starting version.
  // getStartingVersion() must return the same value across multiple calls.
  private volatile Optional<Long> cachedStartingVersion = null;

  // Cache for the initial snapshot files to avoid re-sorting on repeated access.
  private static class InitialSnapshotCache {
    final Long version;
    final List<IndexedFile> files;

    InitialSnapshotCache(Long version, List<IndexedFile> files) {
      this.version = version;
      this.files = files;
    }
  }

  private final AtomicReference<InitialSnapshotCache> cachedInitialSnapshot =
      new AtomicReference<>(null);

  private final int maxInitialSnapshotFiles;

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

    this.snapshotAtSourceInit = (SnapshotImpl) snapshotAtSourceInit;
    this.tableId = this.snapshotAtSourceInit.getMetadata().getId();
    // TODO(#5319): schema tracking for non-additive schema changes
    this.readSchemaAtSourceInit =
        Objects.requireNonNull(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshotAtSourceInit.getSchema()),
            "readSchemaAtSourceInit is null");
    this.shouldValidateOffsets =
        Objects.requireNonNull(
            (Boolean)
                spark.sessionState().conf().getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION()),
            "shouldValidateOffsets is null");
    this.maxInitialSnapshotFiles =
        (Integer)
            spark
                .sessionState()
                .conf()
                .getConf(DeltaSQLConf.DELTA_STREAMING_INITIAL_SNAPSHOT_MAX_FILES());

    boolean isStreamingFromColumnMappingTable =
        ColumnMapping.getColumnMappingMode(
                this.snapshotAtSourceInit.getMetadata().getConfiguration())
            != ColumnMapping.ColumnMappingMode.NONE;
    boolean isTypeWideningSupportedInProtocol =
        this.snapshotAtSourceInit.getProtocol().supportsFeature(TYPE_WIDENING_RW_PREVIEW_FEATURE)
            || this.snapshotAtSourceInit.getProtocol().supportsFeature(TYPE_WIDENING_RW_FEATURE);
    this.schemaReadOptions =
        Objects.requireNonNull(
            DeltaStreamUtils.SchemaReadOptions$.MODULE$.fromSparkSession(
                spark, isStreamingFromColumnMappingTable, isTypeWideningSupportedInProtocol),
            "schemaReadOptions is null");
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
      // No starting version specified in the options, use snapshot captured
      // at source initialization.
      version = snapshotAtSourceInit.getVersion();
      isInitialSnapshot = true;
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
    // Block latestOffset() from generating an invalid offset by proactively
    // verifying incompatible schema changes under column mapping. See more details in the
    // method java doc.
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(
        previousOffset.reservoirVersion(), /* batchEndVersion= */ null);
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
    cachedInitialSnapshot.set(null);
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
  synchronized Optional<Long> getStartingVersion() {
    if (cachedStartingVersion != null) {
      return cachedStartingVersion;
    }

    // TODO(#5319): DeltaSource.scala uses `allowOutOfRange` parameter from
    // DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.
    if (options.startingVersion().isDefined()) {
      DeltaStartingVersion startingVersion = options.startingVersion().get();
      if (startingVersion instanceof StartingVersionLatest$) {
        Snapshot latestSnapshot = snapshotManager.loadLatestSnapshot();
        // "latest": start reading from the next commit
        cachedStartingVersion = Optional.of(latestSnapshot.getVersion() + 1);
        return cachedStartingVersion;
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
        cachedStartingVersion = Optional.of(version);
        return cachedStartingVersion;
      }
    }
    // TODO(#5319): Implement startingTimestamp support
    cachedStartingVersion = Optional.empty();
    return cachedStartingVersion;
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
      // Lazily combine snapshot files with delta logs starting from fromVersion + 1.
      // filterDeltaLogs handles the case when no commits exist after fromVersion.
      CloseableIterator<IndexedFile> snapshotFiles = getSnapshotFiles(fromVersion);
      CloseableIterator<IndexedFile> deltaChanges = filterDeltaLogs(fromVersion + 1, endOffset);
      result = snapshotFiles.combine(deltaChanges);
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

  private CloseableIterator<IndexedFile> filterDeltaLogs(
      long startVersion, Optional<DeltaSourceOffset> endOffset) {
    Optional<Long> endVersionOpt =
        endOffset.isPresent() ? Optional.of(endOffset.get().reservoirVersion()) : Optional.empty();

    if (endVersionOpt.isPresent()) {
      // Cap endVersion to the latest available version. The Kernel's getTableChanges requires
      // endVersion to be an actual existing version or empty.
      long latestVersion = snapshotAtSourceInit.getVersion();
      if (endVersionOpt.get() > latestVersion) {
        // This could happen because:
        // 1. data could be added after snapshotAtSourceInit was captured.
        // 2. buildOffsetFromIndexedFile bumps the version up by one when we hit the END_INDEX.
        // TODO(#5318): consider caching the latest version to avoid loading a new snapshot.
        // TODO(#5318): kernel should ideally relax this constraint.
        endVersionOpt = Optional.of(snapshotManager.loadLatestSnapshot().getVersion());
      }

      // After capping, check if startVersion is beyond the endVersion.
      // This can happen when all files in the batch come from the initial snapshot
      // (e.g., offset was bumped to next version due to END_INDEX, but no new commits exist).
      if (startVersion > endVersionOpt.get()) {
        return Utils.toCloseableIterator(Collections.emptyIterator());
      }
    } else {
      // When endOffset is empty (offset discovery), check if startVersion exceeds the current
      // latest version. We must load the current latest (not snapshotAtSourceInit) because new
      // commits may have arrived since stream initialization.
      long currentLatestVersion = snapshotManager.loadLatestSnapshot().getVersion();
      if (startVersion > currentLatestVersion) {
        return Utils.toCloseableIterator(Collections.emptyIterator());
      }
    }

    CommitRange commitRange;
    try {
      commitRange = snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
    } catch (io.delta.kernel.exceptions.CommitRangeNotFoundException e) {
      // If the requested version range doesn't exist (e.g., we're asking for version 6 when
      // the table only has versions 0-5).
      return Utils.toCloseableIterator(Collections.emptyIterator());
    }

    // Use getCommitActionsFromRangeUnsafe instead of CommitRange.getCommitActions() because:
    // 1. CommitRange.getCommitActions() requires a snapshot at exactly the startVersion, but when
    //    startingVersion option is used, we may not be able to recreate that exact snapshot
    //    (e.g., if log files have been cleaned up after checkpointing).
    // 2. This matches DSv1 behavior which uses snapshotAtSourceInit's P&M to interpret all
    //    AddFile actions and performs per-commit protocol validation.
    CloseableIterator<CommitActions> commitsIterator =
        StreamingHelper.getCommitActionsFromRangeUnsafe(
            engine,
            (io.delta.kernel.internal.commitrange.CommitRangeImpl) commitRange,
            snapshotAtSourceInit.getPath(),
            ACTION_SET);

    return commitsIterator.flatMap(
        commit -> processCommitToIndexedFiles(commit, startVersion, endOffset));
  }

  /**
   * Processes a single commit and returns an iterator of IndexedFiles wrapped with BEGIN/END
   * sentinels.
   */
  private CloseableIterator<IndexedFile> processCommitToIndexedFiles(
      CommitActions commit, long startVersion, Optional<DeltaSourceOffset> endOffsetOpt) {
    try {
      long version = commit.getVersion();

      // First pass: Validate the commit.
      //
      // We must validate the ENTIRE commit before emitting ANY files. This is a correctness
      // requirement: commits could contain both AddFiles and RemoveFiles.
      // If we emitted AddFiles before discovering a RemoveFile(dataChange=true) later in the
      // commit, downstream would produce incorrect results.
      //
      // TODO(#5318): consider caching the commit actions to avoid reading the same commit twice.
      // TODO(#5319): don't verify metadata action when schema tracking is enabled
      validateCommit(
          commit,
          version,
          startVersion,
          snapshotAtSourceInit.getPath(),
          endOffsetOpt,
          /* verifyMetadataAction= */ true);
      // Second pass: Build a lazy iterator of IndexedFiles.
      //
      //   BEGIN (BASE_INDEX) + actual file actions + END (END_INDEX)
      //
      // These sentinel IndexedFiles have null file actions and are used for proper offset
      // tracking:
      //   - BASE_INDEX: marks "before any files in this version", allowing the offset to
      //                 reference the start of a version.
      //   - END_INDEX:  marks end of version, triggers version advancement in
      //                 buildOffsetFromIndexedFile to skip re-reading completed versions.
      //
      // See DeltaSource.addBeginAndEndIndexOffsetsForVersion for the Scala equivalent.
      return Utils.singletonCloseableIterator(
              new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), /* addFile= */ null))
          .combine(getFilesFromCommit(commit, version))
          .combine(
              Utils.singletonCloseableIterator(
                  new IndexedFile(version, DeltaSourceOffset.END_INDEX(), /* addFile= */ null)));
    } catch (Exception e) {
      // commit is not a CloseableIterator, we need to close it manually.
      Utils.closeCloseables(commit);
      throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  private CloseableIterator<IndexedFile> getFilesFromCommit(CommitActions commit, long version) {
    // Assign each IndexedFile a unique index within the commit. We use a mutable array
    // because variables captured by a lambda must be effectively final (never reassigned).
    long[] fileIndex = {0};

    return commit
        .getActions()
        .flatMap(
            batch -> {
              // Processing each batch eagerly because they are already loaded into memory.
              List<IndexedFile> files = new ArrayList<>();
              fileIndex[0] = addIndexedFilesAndReturnNextIndex(batch, version, fileIndex[0], files);
              return Utils.toCloseableIterator(files.iterator());
            });
  }

  /**
   * Validates a commit and fail the stream if it's invalid. Mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @param commit the CommitActions representing a single commit
   * @param version the commit version
   * @param batchStartVersion Starting version of the batch being processed
   * @param tablePath the path to the Delta table
   * @param endOffsetOpt optional end offset for boundary checking
   * @param verifyMetadataAction Whether to verify metadata action compatibility
   * @throws RuntimeException if the commit is invalid.
   */
  private void validateCommit(
      CommitActions commit,
      long version,
      long batchStartVersion,
      String tablePath,
      Optional<DeltaSourceOffset> endOffsetOpt,
      boolean verifyMetadataAction) {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isPresent()) {
      DeltaSourceOffset endOffset = endOffsetOpt.get();
      if (endOffset.reservoirVersion() == version
          && endOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        return;
      }
    }
    // TODO(#5319): Implement ignoreChanges & skipChangeCommits & ignoreDeletes (legacy)

    try (CloseableIterator<ColumnarBatch> actionsIter = commit.getActions()) {
      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        int numRows = batch.getSize();
        Metadata metadataAction = null;
        for (int rowId = 0; rowId < numRows; rowId++) {
          // RULE 1: If commit has RemoveFile(dataChange=true), fail this stream.
          Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
          if (removeOpt.isPresent()) {
            RemoveFile removeFile = removeOpt.get();
            throw (RuntimeException)
                DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFile.getPath(), tablePath);
          }

          // RULE 2: If commit has Metadata, check read-incompatible schema changes.
          Optional<Metadata> metadataOpt = StreamingHelper.getMetadata(batch, rowId);
          if (metadataOpt.isPresent()) {
            Metadata metadata = metadataOpt.get();
            Preconditions.checkArgument(
                metadataAction == null,
                "Should not encounter two metadata actions in the same commit of version %d",
                version);
            metadataAction = metadata;
            Long batchEndVersion =
                endOffsetOpt.map(DeltaSourceOffset::reservoirVersion).orElse(null);
            if (verifyMetadataAction) {
              checkReadIncompatibleSchemaChanges(
                  metadata,
                  version,
                  batchStartVersion,
                  batchEndVersion,
                  /* validatedDuringStreamStart */ false);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to process commit at version " + version, e);
    }
  }

  /**
   * Narrow waist to verify a metadata action for read-incompatible schema changes, specifically: 1.
   * Any column mapping related schema changes (rename / drop) columns 2. Standard
   * read-compatibility changes including: a) No missing columns b) No data type changes c) No
   * read-incompatible nullability changes If the check fails, we throw an exception to exit the
   * stream. If lazy log initialization is required, we also run a one time scan to safely
   * initialize the metadata tracking log upon any non-additive schema change failures.
   *
   * @param metadata Metadata that contains a potential schema change
   * @param version Version for the metadata action
   * @param batchStartVersion Starting version of the batch being processed
   * @param batchEndVersion Ending version of the batch being processed, or null for the latest
   * @param validatedDuringStreamStart Whether this check is being done during stream start.
   */
  private void checkReadIncompatibleSchemaChanges(
      Metadata metadata,
      long version,
      long batchStartVersion,
      Long batchEndVersion,
      boolean validatedDuringStreamStart) {
    logger.info(
        "checking read incompatibility with schema at version {}, inside batch[{}, {}].",
        version,
        batchStartVersion,
        batchEndVersion != null ? batchEndVersion : "latest");

    Metadata newMetadata, oldMetadata;
    if (version < snapshotAtSourceInit.getVersion()) {
      newMetadata = snapshotAtSourceInit.getMetadata();
      oldMetadata = metadata;
    } else {
      newMetadata = metadata;
      oldMetadata = snapshotAtSourceInit.getMetadata();
    }

    // Table ID has changed during streaming
    if (!Objects.equals(newMetadata.getId(), oldMetadata.getId())) {
      throw (RuntimeException)
          DeltaErrors.differentDeltaTableReadByStreamingSource(
              newMetadata.getId(), oldMetadata.getId());
    }

    // TODO(#5319): schema tracking for non-additive schema changes

    // Other standard read compatibility changes
    if (!validatedDuringStreamStart
        || !schemaReadOptions
            .forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart()) {

      StructType schemaChange = SchemaUtils.convertKernelSchemaToSparkSchema(metadata.getSchema());

      // There is a schema change. All files after this commit will use `schemaChange`. Hence, we
      // check whether we can use `schema` (the fixed source schema we use in the same run of the
      // query) to read these new files safely.
      boolean backfilling = version < snapshotAtSourceInit.getVersion();
      // Partition column change will be ignored if user enable the unsafe flag
      Seq<String> newPartitionColumns, oldPartitionColumns;
      if (schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges()) {
        newPartitionColumns = (Seq<String>) Seq$.MODULE$.empty();
        oldPartitionColumns = (Seq<String>) Seq$.MODULE$.empty();
      } else {
        newPartitionColumns =
            CollectionConverters.asScala(
                    VectorUtils.toJavaList(newMetadata.getPartitionColumns()).stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()))
                .toSeq();
        oldPartitionColumns =
            CollectionConverters.asScala(
                    VectorUtils.toJavaList(oldMetadata.getPartitionColumns()).stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()))
                .toSeq();
      }

      DeltaStreamUtils.SchemaCompatibilityResult checkResult =
          DeltaStreamUtils.checkSchemaChangesWhenNoSchemaTracking(
              schemaChange,
              readSchemaAtSourceInit,
              newPartitionColumns,
              oldPartitionColumns,
              backfilling,
              schemaReadOptions);

      if (!DeltaStreamUtils.SchemaCompatibilityResult$.MODULE$.isCompatible(checkResult)) {
        boolean isRetryable =
            DeltaStreamUtils.SchemaCompatibilityResult$.MODULE$.isRetryableIncompatible(
                checkResult);
        throw (RuntimeException)
            DeltaErrors.schemaChangedException(
                readSchemaAtSourceInit,
                schemaChange,
                isRetryable,
                Some.apply(version),
                options.containsStartingVersionOrTimestamp());
      }
    }
  }

  /**
   * Check read-incompatible schema changes during stream (re)start so we could fail fast.
   *
   * <p>This is called ONCE during the first latestOffset call to catch edge cases that normal
   * per-commit validation (checkReadIncompatibleSchemaChanges) misses.
   *
   * <p><b>Why needed?</b> Normal validation only checks commits with metadata actions. If a stream
   * starts at version 1 with the latest version is version 3 and there is a schema change at
   * version 2, validateCommits only validates version 2's schema against version 3 (SAME, passes).
   * But version 1 files may have an incompatible older schema that was never checked since version
   * 1 has no metadata action.
   *
   * <p>This method explicitly loads and validates the snapshot in the scan range, regardless of
   * whether it has a metadata action, catching such incompatibilities before planInputPartitions
   *
   * <p>Skipped if schema tracking log is already initialized.
   *
   * @param batchStartVersion Start version we want to verify read compatibility against
   * @param batchEndVersion Optionally, if we are checking against an existing constructed batch
   *     during streaming initialization, we would also like to verify all schema changes in between
   *     as well before we can lazily initialize the schema log if needed.
   */
  private void checkReadIncompatibleSchemaChangeOnStreamStartOnce(
      long batchStartVersion, Long batchEndVersion) {
    // TODO(#5319): skip if enable schema tracking log

    if (hasCheckedReadIncompatibleSchemaChangesOnStreamStart) return;

    SnapshotImpl startVersionSnapshot = null;
    Exception err = null;
    try {
      startVersionSnapshot = (SnapshotImpl) snapshotManager.loadSnapshotAt(batchStartVersion);
    } catch (Exception e) {
      err = e;
    }

    // Cannot perfectly verify column mapping schema changes if we cannot compute a start snapshot.
    if (!schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges()
        && schemaReadOptions.isStreamingFromColumnMappingTable()
        && (err != null)) {
      throw (RuntimeException)
          DeltaErrors.failedToGetSnapshotDuringColumnMappingStreamingReadCheck(err);
    }

    // Perform schema check if we need to, considering all escape flags.
    if (!schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges()
        || schemaReadOptions.typeWideningEnabled()
        || !schemaReadOptions
            .forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart()) {
      if (startVersionSnapshot != null) {
        checkReadIncompatibleSchemaChanges(
            startVersionSnapshot.getMetadata(),
            startVersionSnapshot.getVersion(),
            batchStartVersion,
            batchEndVersion,
            /* validatedDuringStreamStart= */ true);
        // If end version is defined (i.e. we have a pending batch), let's also eagerly check all
        // intermediate schema changes against the stream read schema to capture corners cases such
        // as rename and rename back.
        if (batchEndVersion != null) {
          for (Map.Entry<Long, Metadata> entry :
              StreamingHelper.collectMetadataActionsFromRangeUnsafe(
                      batchStartVersion,
                      Optional.of(batchEndVersion),
                      snapshotManager,
                      engine,
                      snapshotAtSourceInit.getPath())
                  .entrySet()) {
            long version = entry.getKey();
            Metadata metadata = entry.getValue();
            checkReadIncompatibleSchemaChanges(
                metadata,
                version,
                batchStartVersion,
                batchEndVersion,
                /* validatedDuringStreamStart= */ true);
          }
        }
      }
    }

    // Mark as checked
    hasCheckedReadIncompatibleSchemaChangesOnStreamStart = true;
  }

  /**
   * Extracts IndexedFiles from a batch of actions for a given version and adds them to the output
   * list. Assigns an index to each IndexedFile.
   *
   * @return The next available index after processing this batch
   */
  private long addIndexedFilesAndReturnNextIndex(
      ColumnarBatch batch, long version, long startIndex, List<IndexedFile> output) {
    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      // Only include AddFiles with dataChange=true. Skip changes that optimize or reorganize
      // data without changing the logical content.
      Optional<AddFile> addOpt = StreamingHelper.getAddFileWithDataChange(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        output.add(new IndexedFile(version, index++, addFile));
      }
    }

    return index;
  }

  /**
   * Get all files from a snapshot at the specified version, sorted by modificationTime and path,
   * with indices assigned sequentially, and wrapped with BEGIN/END sentinels.
   *
   * <p>Mimics DeltaSourceSnapshot in DSv1.
   *
   * @param version The snapshot version to read
   * @return An iterator of IndexedFile representing the snapshot files
   */
  private CloseableIterator<IndexedFile> getSnapshotFiles(long version) {
    InitialSnapshotCache cache = cachedInitialSnapshot.get();

    if (cache != null && cache.version != null && cache.version == version) {
      return Utils.toCloseableIterator(cache.files.iterator());
    }

    List<IndexedFile> indexedFiles = loadAndValidateSnapshot(version);

    cachedInitialSnapshot.set(
        new InitialSnapshotCache(version, Collections.unmodifiableList(indexedFiles)));

    return Utils.toCloseableIterator(indexedFiles.iterator());
  }

  /** Loads snapshot files at the specified version using distributed log replay and sorting. */
  private List<IndexedFile> loadAndValidateSnapshot(long version) {
    Snapshot snapshot = snapshotManager.loadSnapshotAt(version);

    // Use distributed log replay + sorting (V1's DeltaSourceSnapshot algorithm)
    SparkSession spark = SparkSession.active();
    int numPartitions =
        spark
            .conf()
            .getOption("spark.databricks.delta.v2.streaming.initialSnapshot.numPartitions")
            .map(Integer::parseInt)
            .getOrElse(() -> 50);

    // Get sorted files using distributed DataFrame operations
    // Returns DataFrame with "add" struct, sorted by (modificationTime, path)
    org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> sortedFilesDF =
        DistributedLogReplayHelper.getInitialSnapshotForStreaming(spark, snapshot, numPartitions);

    // Validate size limit (use count for efficiency)
    long fileCount = sortedFilesDF.count();
    if (fileCount > maxInitialSnapshotFiles) {
      throw (RuntimeException)
          DeltaErrors.initialSnapshotTooLargeForStreaming(
              version, fileCount, maxInitialSnapshotFiles, tablePath);
    }

    // Convert sorted DataFrame to AddFiles using DistributedScanBuilder
    // withSortKey() ensures ordering is preserved for streaming
    io.delta.kernel.ScanBuilder scanBuilder =
        new DistributedScanBuilder(spark, snapshot, numPartitions, sortedFilesDF).withSortKey();
    io.delta.kernel.Scan scan = scanBuilder.build();

    List<AddFile> addFiles = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> filesIter = scan.getScanFiles(engine)) {
      while (filesIter.hasNext()) {
        FilteredColumnarBatch filteredBatch = filesIter.next();
        ColumnarBatch batch = filteredBatch.getData();

        for (int rowId = 0; rowId < batch.getSize(); rowId++) {
          Optional<AddFile> addOpt = StreamingHelper.getAddFile(batch, rowId);
          if (addOpt.isPresent()) {
            addFiles.add(addOpt.get());
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to read snapshot files at version %d", version), e);
    }

    // Build IndexedFile list with sentinels
    List<IndexedFile> indexedFiles = new ArrayList<>();

    // Add BEGIN sentinel
    indexedFiles.add(new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), null));

    // Add data files with sequential indices starting from 0
    for (int i = 0; i < addFiles.size(); i++) {
      indexedFiles.add(new IndexedFile(version, i, addFiles.get(i)));
    }

    // Add END sentinel
    indexedFiles.add(new IndexedFile(version, DeltaSourceOffset.END_INDEX(), null));

    return indexedFiles;
  }
}
