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
package org.apache.spark.sql.delta.v2.read;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_FEATURE;
import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.DeltaColumnMapping$;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaSingleCommit;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.SnapshotDescriptor;
import org.apache.spark.sql.delta.TypeWidening;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.CommitInfo;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.actions.RemoveFile;
import org.apache.spark.sql.delta.commands.cdc.CDCReader$;
import org.apache.spark.sql.delta.files.DeltaSourceSnapshot;
import org.apache.spark.sql.delta.sources.AdmittableFile;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset$;
import org.apache.spark.sql.delta.sources.DeltaStreamUtils;
import org.apache.spark.sql.delta.sources.DeltaStreamingBackend;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.storage.ClosableIterator.ClosableWrapper;
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps;
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorFlatMapCloseOp;
import org.apache.spark.sql.delta.v2.CDCDeletionVectorHelper;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SingleCommit$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;
import org.apache.spark.sql.delta.v2.kernel.KernelEngineFactory;
import org.apache.spark.sql.delta.v2.utils.PartitionUtils;
import org.apache.spark.sql.delta.v2.utils.PartitionValueCaster;
import org.apache.spark.sql.delta.v2.utils.ScalaUtils;
import org.apache.spark.sql.delta.v2.utils.StreamingHelper;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;
import scala.Option;
import scala.Some;
import scala.collection.Iterator;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction2;
import scala.util.matching.Regex;

// TODO(#5318): Use DeltaErrors error framework for consistent error handling.
/**
 * Package-private micro-batch stream implementation for Delta's Spark DataSource V2 read path.
 *
 * <p>This class must remain package-private so callers outside {@code v2.read} depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
class DeltaV2MicroBatchStream
    implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

  private static final Logger logger = LoggerFactory.getLogger(DeltaV2MicroBatchStream.class);

  public static final Set<DeltaAction> ACTION_SET =
      ImmutableSet.of(
          DeltaAction.ADD,
          DeltaAction.REMOVE,
          DeltaAction.METADATA,
          DeltaAction.PROTOCOL,
          DeltaAction.COMMITINFO);

  /** Action set for CDC reads: {@link #ACTION_SET} plus {@link DeltaAction#CDC}. */
  public static final Set<DeltaAction> CDC_ACTION_SET =
      ImmutableSet.<DeltaAction>builder().addAll(ACTION_SET).add(DeltaAction.CDC).build();

  /**
   * Max attempts to resolve the start version when failOnDataLoss=false. Attempt 1 is the initial
   * try with the user's startVersion (surfaces the StartVersionNotFoundException that triggers the
   * failOnDataLoss check). Attempt 2 retries with the earliest available version to recover when
   * failOnDataLoss=false. Attempt 3 guards against a concurrent log-retention prune in the tiny
   * window between attempt 2's exception and its retry.
   */
  private static final int FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS = 3;

  private final Engine engine;
  private final DeltaV2SnapshotManager snapshotManager;

  /** The Delta log operations that compute offsets. */
  private final DeltaStreamingBackend streamingBackend;

  private final DeltaOptions options;
  private final boolean ignoreFileDeletion;
  private final boolean skipChangeCommits;
  // TODO: migrate to the V1 Snapshot class and remove deltaSnapshotAtSourceInit.
  private final SnapshotImpl snapshotAtSourceInit;

  /** The V1 view of {@link #snapshotAtSourceInit}, the provider of the shared V1 usage events. */
  private final Snapshot deltaSnapshotAtSourceInit;

  private final String tableId;
  private final SnapshotDescriptor readSnapshotDescriptor;
  private final StructType readSchemaAtSourceInit;
  private final boolean shouldValidateOffsets;
  private final Optional<Regex> excludeRegex;
  private final SparkSession spark;
  private final String tablePath;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final StructType ddlOrderedReadOutputSchema;
  private final Filter[] dataFilters;
  private final Configuration hadoopConf;
  private final SQLConf sqlConf;
  private final scala.collection.immutable.Map<String, String> scalaOptions;

  /** Handler for non-additive schema evolution (rename/drop/type widening). */
  private final MetadataEvolutionHandler metadataEvolutionHandler;

  // Spark can call initialOffset multiple times while constructing and executing the first batch.
  private DeltaSourceOffset cachedInitialOffset;

  /**
   * Tracks whether this is the first batch for this stream (no checkpointed offset).
   *
   * <p>- First batch: initialOffset() -> latestOffset(Offset, ReadLimit) - Set `isFirstBatch` to
   * true in initialOffset() - in latestOffset(Offset, ReadLimit), use `isFirstBatch` to determine
   * whether to return null vs previousOffset (when no data is available) - keep `isFirstBatch` true
   * until a non-null offset is returned - Subsequent batches: latestOffset(Offset, ReadLimit)
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

  /** One-shot flag for {@link #validateCDFEnabledOnTable}. */
  private volatile boolean cdfEnabledOnStreamStartValidated = false;

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

  // A metadata snapshot when starting the query.
  private DeltaSourceSnapshot initialSnapshot = null;
  private long initialSnapshotVersion = -1L;

  public DeltaV2MicroBatchStream(
      DeltaV2SnapshotManager snapshotManager,
      Snapshot snapshotAtSourceInit,
      Configuration hadoopConf,
      SparkSession spark,
      DeltaOptions options,
      String tablePath,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      StructType ddlOrderedReadOutputSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Option<DeltaSourceMetadataTrackingLog> metadataTrackingLog,
      String metadataPath) {
    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.streamingBackend = new DeltaV2StreamingBackend(snapshotManager);
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    this.spark = Objects.requireNonNull(spark, "spark is null");
    this.engine = KernelEngineFactory.createDefaultEngine(hadoopConf);
    this.options = Objects.requireNonNull(options, "options is null");
    this.ignoreFileDeletion = this.options.ignoreFileDeletion();
    // Deprecated. Please use `skipChangeCommits` from now on.
    if (this.ignoreFileDeletion) {
      logger.warn(DeltaErrors.ignoreStreamingUpdatesAndDeletesWarning(this.spark));
    }
    this.skipChangeCommits = this.options.skipChangeCommits();
    // Normalize tablePath to ensure it ends with "/" for consistent path construction
    String normalizedTablePath = Objects.requireNonNull(tablePath, "tablePath is null");
    this.tablePath =
        normalizedTablePath.endsWith("/") ? normalizedTablePath : normalizedTablePath + "/";
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.ddlOrderedReadOutputSchema =
        Objects.requireNonNull(ddlOrderedReadOutputSchema, "ddlOrderedReadOutputSchema is null");
    this.dataFilters =
        Arrays.copyOf(
            Objects.requireNonNull(dataFilters, "dataFilters is null"), dataFilters.length);
    this.sqlConf = SQLConf.get();
    this.scalaOptions = Objects.requireNonNull(scalaOptions, "scalaOptions is null");

    this.deltaSnapshotAtSourceInit = snapshotAtSourceInit;
    this.snapshotAtSourceInit = DeltaV2Snapshot$.MODULE$.getKernelSnapshot(snapshotAtSourceInit);
    this.tableId = this.snapshotAtSourceInit.getMetadata().getId();

    // The effective descriptor for reading, mirroring v1's readSnapshotDescriptor. When schema
    // tracking has a persisted entry, layer it onto the source-init descriptor so
    // the read schema/protocol/config reflect the evolved state. The merged entry has already
    // been written to the durable schema log during analysis (by DeltaV2Table's call to
    // getPersistedMetadataForMicroBatchStream with mergeConsecutiveSchemaChanges=true), so
    // reading it back from the log here yields the same value V1 obtains.
    Option<PersistedMetadata> persistedMetadataAtSourceInit =
        metadataTrackingLog.isDefined()
            ? metadataTrackingLog.get().getCurrentTrackedMetadata()
            : Option.empty();
    this.readSnapshotDescriptor =
        Objects.requireNonNull(
            persistedMetadataAtSourceInit.isDefined()
                ? MetadataEvolutionHandler.buildReadSnapshotDescriptorFromPersistedMetadata(
                    snapshotAtSourceInit, persistedMetadataAtSourceInit.get())
                : snapshotAtSourceInit,
            "readSnapshotDescriptor is null");
    this.readSchemaAtSourceInit =
        Objects.requireNonNull(readSnapshotDescriptor.schema(), "readSchemaAtSourceInit is null");

    this.shouldValidateOffsets =
        Objects.requireNonNull(
            (Boolean)
                spark.sessionState().conf().getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION()),
            "shouldValidateOffsets is null");
    this.excludeRegex = ScalaUtils.toJavaOptional(options.excludeRegex());

    boolean isStreamingFromColumnMappingTable =
        ColumnMapping.getColumnMappingMode(
                this.snapshotAtSourceInit.getMetadata().getConfiguration())
            != ColumnMappingMode.NONE;
    boolean isTypeWideningSupportedInProtocol =
        this.snapshotAtSourceInit.getProtocol().supportsFeature(TYPE_WIDENING_RW_PREVIEW_FEATURE)
            || this.snapshotAtSourceInit.getProtocol().supportsFeature(TYPE_WIDENING_RW_FEATURE);
    this.schemaReadOptions =
        Objects.requireNonNull(
            DeltaStreamUtils.SchemaReadOptions$.MODULE$.fromSparkSession(
                spark, isStreamingFromColumnMappingTable, isTypeWideningSupportedInProtocol),
            "schemaReadOptions is null");
    this.metadataEvolutionHandler =
        new MetadataEvolutionHandler(
            spark,
            tableId,
            snapshotManager,
            streamingBackend,
            engine,
            options,
            schemaReadOptions,
            metadataTrackingLog,
            readSnapshotDescriptor.metadata(),
            readSnapshotDescriptor.protocol(),
            metadataPath);
    boolean shouldValidateSchemaOnRestart =
        (Boolean)
            spark
                .sessionState()
                .conf()
                .getConf(DeltaSQLConf.STREAMING_SCHEMA_VALIDATION_ON_RESTART());
    // When schema tracking is enabled, the MetadataEvolutionHandler manages schema changes via
    // the schema log; the analysis-vs-snapshot mismatch this check guards against is expected on
    // restart and is surfaced via the schema-log evolution exception instead.
    if (shouldValidateSchemaOnRestart && !metadataEvolutionHandler.shouldTrackMetadataChange()) {
      validateSchemaCompatibilityOnStartup(dataSchema, partitionSchema, readSchemaAtSourceInit);
    }
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
    // TODO(#6817): Explore unifying TAN offset computation into latestOffsetInternal.
    // Blocked: latestOffset per-batch shares the same conditions but must NOT bypass rate limiting.
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
    if (cachedInitialOffset != null) {
      return cachedInitialOffset;
    }

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

    cachedInitialOffset =
        DeltaSourceOffset.apply(
            tableId, version, DeltaSourceOffset.BASE_INDEX(), isInitialSnapshot);
    return cachedInitialOffset;
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

    try {
      DeltaSourceOffset deltaStartOffset = DeltaSourceOffset.apply(tableId, startOffset);
      initForTriggerAvailableNowIfNeeded(deltaStartOffset);
      // Return null when no data is available for this batch.
      DeltaSourceOffset endOffset = latestOffsetInternal(deltaStartOffset, limit).orElse(null);
      if (endOffset != null) {
        isFirstBatch = false;
      }
      return endOffset;
    } catch (Exception e) {
      // Kernel's DefaultJsonHandler wraps ClosedByInterruptException (thrown by NIO
      // channels on thread interrupt) inside KernelEngineException (a RuntimeException).
      // Spark's StreamExecution.isInterruptionException recognizes
      // ClosedByInterruptException and UncheckedIOException but not
      // KernelEngineException. Re-wrap so Spark's isInterruptedByStop — which also
      // verifies state == TERMINATED — handles it as a clean stream shutdown.
      Optional<ClosedByInterruptException> interruptCause = findClosedByInterruptCause(e);
      if (interruptCause.isPresent()) {
        throw new UncheckedIOException(interruptCause.get());
      }
      // Translate Kernel's protocol-version error into Delta's InvalidProtocolVersionException.
      Optional<UnsupportedProtocolVersionException> protocolCause = findUnsupportedProtocolCause(e);
      if (protocolCause.isPresent()) {
        UnsupportedProtocolVersionException p = protocolCause.get();
        throw (RuntimeException)
            DeltaErrors.invalidProtocolVersionError(
                p.getTablePath(), p.getMinReaderVersion(), p.getMinWriterVersion());
      }
      throw e;
    }
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
    // Initialize schema tracking log eagerly if possible (fresh stream, first batch).
    // This mirrors v1's getStartingOffsetFromSpecificDeltaVersion eager init path.
    if (isFirstBatch) {
      if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
        metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
            previousOffset.reservoirVersion(),
            /* batchEndVersion= */ null,
            /* alwaysFailUponLogInitialized= */ false);
      }
    } else {
      long startVersionForMetadataLogInit;
      if (previousOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        startVersionForMetadataLogInit = previousOffset.reservoirVersion() - 1;
      } else {
        startVersionForMetadataLogInit = previousOffset.reservoirVersion();
      }
      if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
        metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
            startVersionForMetadataLogInit,
            /* batchEndVersion= */ null,
            /* alwaysFailUponLogInitialized= */ false);
      }
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(startVersionForMetadataLogInit, null);
    }

    // Handle pending schema change offsets (two-barrier protocol).
    if (metadataEvolutionHandler.shouldTrackMetadataChange()) {
      Optional<DeltaSourceOffset> pendingOffset =
          metadataEvolutionHandler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(
              previousOffset);
      if (pendingOffset.isPresent()) {
        return pendingOffset;
      }
    }

    ClosableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            previousOffset.reservoirVersion(),
            previousOffset.index(),
            previousOffset.isInitialSnapshot(),
            limits);

    Optional<IndexedFile> lastFileChange =
        ScalaUtils.toJavaOptional(DeltaSource.iteratorLast(changes));

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

    // Validate and lazily initialize the metadata tracking log for planned batches during
    // stream (re)start. Mirrors v1's validateAndInitMetadataLogForPlannedBatchesDuringStreamStart.
    validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(fromVersion, endOffset);

    List<PartitionedFile> partitionedFiles = new ArrayList<>();
    long totalBytesToRead = 0;
    boolean isCDC = options.readChangeFeed();
    ZoneId zoneId = DateTimeUtils.getZoneId(sqlConf.sessionLocalTimeZone());
    PartitionValueCaster caster = new PartitionValueCaster(partitionSchema, zoneId);

    try (ClosableIterator<IndexedFile> fileChanges =
        isCDC
            ? getFileChangesWithRateLimitForCDC(
                fromVersion,
                fromIndex,
                isInitialSnapshot,
                /* limits= */ Optional.empty(),
                Optional.of(endOffset))
            : getFileChanges(fromVersion, fromIndex, isInitialSnapshot, Optional.of(endOffset))) {
      while (fileChanges.hasNext()) {
        PartitionedFile pf = buildPartitionedFile(fileChanges.next(), isCDC, caster);
        if (pf == null) continue;
        totalBytesToRead += pf.fileSize();
        partitionedFiles.add(pf);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format(
              "Failed to get file changes for table %s from version %d index %d to offset %s",
              tablePath, fromVersion, fromIndex, endOffset),
          e);
    } catch (RuntimeException e) {
      // Same interrupt handling as latestOffset(): Kernel wraps ClosedByInterruptException
      // in KernelEngineException (a RuntimeException). Re-wrap as UncheckedIOException so
      // Spark's isInterruptedByStop recognizes it as a clean shutdown.
      Optional<ClosedByInterruptException> interruptCause = findClosedByInterruptCause(e);
      if (interruptCause.isPresent()) {
        throw new UncheckedIOException(interruptCause.get());
      }
      throw e;
    }

    return PartitionUtils.planInputPartitions(
        spark, partitionedFiles, totalBytesToRead, hadoopConf, sqlConf);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return PartitionUtils.createDeltaParquetReaderFactory(
        readSnapshotDescriptor,
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf,
        /* isWriteTimeCDCRead */ options.readChangeFeed());
  }

  /**
   * Returns a {@link PartitionedFile} for the given {@link IndexedFile}, or {@code null} if the
   * file should be skipped (no AddFile/CDC payload, or path matches the excludeRegex).
   */
  private PartitionedFile buildPartitionedFile(
      IndexedFile indexedFile, boolean isCDC, PartitionValueCaster caster) {
    if (indexedFile.getAddFile() != null) {
      AddFile addFile = indexedFile.getAddFile();
      if (isExcludedPath(addFile.path())) return null;
      return PartitionUtils.buildPartitionedFile(addFile, caster, tablePath);
    }
    if (isCDC && indexedFile.getCDCDataFile() != null) {
      CDCDataFile cdcFile = indexedFile.getCDCDataFile();
      if (isExcludedPath(cdcFile.getPath())) return null;
      return PartitionUtils.buildCDCPartitionedFile(
          cdcFile, indexedFile.getVersion(), caster, tablePath);
    }
    return null;
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    // IMPORTANT: for future developers, please place any work you would like to do in commit()
    // before updateMetadataTrackingLogAndFailTheStreamIfNeeded as it may throw an exception.
    DeltaSourceOffset offset = DeltaSourceOffset.apply(tableId, end);
    if (!offset.isInitialSnapshot()) {
      // Release snapshot resources after initial snapshot processing has completed.
      cleanUpSnapshotResources();
    }
    metadataEvolutionHandler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(offset);
  }

  @Override
  public void stop() {
    cleanUpSnapshotResources();
  }

  private void cleanUpSnapshotResources() {
    if (initialSnapshot != null) {
      initialSnapshot.close(
          /* unpersistSnapshot= */ initialSnapshotVersion < snapshotAtSourceInit.getVersion());
      initialSnapshot = null;
    }
  }

  /**
   * If the given exception wraps a {@link ClosedByInterruptException} as its direct cause, returns
   * it. This occurs when Spark interrupts the micro-batch thread during stream shutdown and the
   * thread is blocked inside Kernel's {@code DefaultJsonHandler} reading delta log files via NIO
   * channels.
   */
  static Optional<ClosedByInterruptException> findClosedByInterruptCause(Throwable t) {
    Throwable cause = t.getCause();
    if (cause instanceof ClosedByInterruptException) {
      return Optional.of((ClosedByInterruptException) cause);
    }
    return Optional.empty();
  }

  /**
   * Walks the cause chain of the given throwable looking for a Kernel {@link
   * UnsupportedProtocolVersionException}.
   */
  static Optional<UnsupportedProtocolVersionException> findUnsupportedProtocolCause(Throwable t) {
    for (Throwable current = t; current != null; current = current.getCause()) {
      if (current instanceof UnsupportedProtocolVersionException) {
        return Optional.of((UnsupportedProtocolVersionException) current);
      }
      if (current.getCause() == current) {
        break;
      }
    }
    return Optional.empty();
  }

  private boolean isExcludedPath(String path) {
    return excludeRegex.isPresent() && excludeRegex.get().findFirstIn(path).isDefined();
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
   * <p>Delegates to the shared {@code DeltaSource.getStartingVersionFromOptions} and caches the
   * result in {@code cachedStartingVersion}, so that "latest" resolves to one version.
   */
  synchronized Optional<Long> getStartingVersion() {
    if (cachedStartingVersion == null) {
      cachedStartingVersion =
          ScalaUtils.toJavaOptional(
                  DeltaSource.getStartingVersionFromOptions(
                      spark, options, streamingBackend, deltaSnapshotAtSourceInit))
              .map(version -> (Long) version);
    }
    return cachedStartingVersion;
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
  ClosableIterator<IndexedFile> getFileChangesWithRateLimit(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits) {
    if (options.readChangeFeed()) {
      return getFileChangesWithRateLimitForCDC(
          fromVersion, fromIndex, isInitialSnapshot, limits, /* endOffset= */ Optional.empty());
    }

    ClosableIterator<IndexedFile> changes =
        getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, /* endOffset= */ Optional.empty());

    // Take each change until we've seen the configured number of addFiles. Some changes don't
    // represent file additions; we retain them for offset tracking, but they don't count toward
    // the maxFilesPerTrigger conf.
    if (limits.isPresent()) {
      DeltaSource.AdmissionLimits admissionLimits = limits.get();
      changes =
          new IteratorCloseOps<>(changes)
              .withClose(iterator -> iterator.takeWhile(admissionLimits::admit));
    }

    // Stop before any schema change barrier if detected.
    // Mirrors v1's stopIndexedFileIteratorAtSchemaChangeBarrier call in
    // getFileChangesWithRateLimit.
    return metadataEvolutionHandler.stopIndexedFileIteratorAtSchemaChangeBarrier(changes);
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
  ClosableIterator<IndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSourceOffset> endOffset) {

    ClosableIterator<IndexedFile> result;

    if (isInitialSnapshot) {
      // Read snapshot files, then delta logs from fromVersion + 1.
      // Create each iterator on demand.
      // filterDeltaLogs handles the case when no commits exist after fromVersion.
      result =
          new IteratorFlatMapCloseOp<>(CollectionConverters.asScala(Arrays.asList(1, 2).iterator()))
              .flatMapWithClose(
                  part -> {
                    if (part == 1) {
                      return getSnapshotAt(fromVersion);
                    }
                    return filterDeltaLogs(fromVersion + 1, endOffset);
                  });
    } else {
      result = filterDeltaLogs(fromVersion, endOffset);
    }

    return applyBoundaryFiltering(result, fromVersion, fromIndex, endOffset);
  }

  /**
   * Gets file changes for CDC mode with rate limiting. Mirrors DSv1:
   * DeltaSourceCDCSupport.getFileChangesForCDC.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param limits Rate limits to apply (Optional.empty() for no limits)
   * @param endOffset The end offset (inclusive), or empty to read all available commits
   * @return An iterator of IndexedFile representing the CDC file changes
   */
  ClosableIterator<IndexedFile> getFileChangesWithRateLimitForCDC(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits,
      Optional<DeltaSourceOffset> endOffset) {
    validateCDFEnabledOnTable(fromVersion);
    ClosableIterator<IndexedFile> result;
    if (isInitialSnapshot) {
      result =
          new IteratorFlatMapCloseOp<>(CollectionConverters.asScala(Arrays.asList(1, 2).iterator()))
              .flatMapWithClose(
                  part -> {
                    if (part == 1) {
                      ClosableIterator<IndexedFile> snapshotFiles = getSnapshotAt(fromVersion);
                      long commitTimestamp = initialSnapshot.snapshot().timestamp();
                      snapshotFiles =
                          new IteratorCloseOps<>(snapshotFiles)
                              .withClose(
                                  iterator ->
                                      iterator.map(
                                          file ->
                                              file.getAddFile() != null
                                                  ? IndexedFile.cdc(
                                                      file.getVersion(),
                                                      file.getIndex(),
                                                      CDCDataFile.fromAddFile(
                                                          file.getAddFile(), commitTimestamp))
                                                  : file));
                      // Filter already-processed files BEFORE admission so they don't consume
                      // budget.
                      snapshotFiles =
                          new IteratorCloseOps<>(snapshotFiles)
                              .withClose(
                                  iterator ->
                                      iterator.filter(
                                          file ->
                                              isAfterStartBoundary(file, fromVersion, fromIndex)));
                      // Delta-log admission shares the same limits so the budget is continuous.
                      if (limits.isPresent()) {
                        snapshotFiles =
                            new IteratorCloseOps<>(snapshotFiles)
                                .withClose(iterator -> iterator.takeWhile(limits.get()::admit));
                      }
                      return snapshotFiles;
                    }
                    return filterDeltaLogsWithRateLimitForCDC(
                        fromVersion + 1, fromVersion, fromIndex, limits, endOffset);
                  });
    } else {
      result =
          filterDeltaLogsWithRateLimitForCDC(
              fromVersion, fromVersion, fromIndex, limits, endOffset);
    }

    // Start boundary already applied above (before admission). Only apply end boundary here.
    result = applyEndBoundaryFiltering(result, endOffset);

    // Stop before any schema change barrier if detected. V1's wrap is in
    // DeltaSource.getFileChangesWithRateLimit, shared by CDC and non-CDC; V2 applies it inside
    // this CDC-specific method because both planInputPartitions and the outer
    // getFileChangesWithRateLimit reach the CDC iterator through here.
    return metadataEvolutionHandler.stopIndexedFileIteratorAtSchemaChangeBarrier(result);
  }

  /**
   * Applies start/end boundary filtering to an iterator of {@link IndexedFile}s.
   *
   * @param result The unfiltered iterator
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param endOffset The end offset (inclusive), or empty to read all available commits
   * @return The filtered iterator
   */
  private ClosableIterator<IndexedFile> applyBoundaryFiltering(
      ClosableIterator<IndexedFile> result,
      long fromVersion,
      long fromIndex,
      Optional<DeltaSourceOffset> endOffset) {
    // Check start boundary (exclusive)
    result =
        new IteratorCloseOps<>(result)
            .withClose(
                iterator ->
                    iterator.filter(file -> isAfterStartBoundary(file, fromVersion, fromIndex)));

    return applyEndBoundaryFiltering(result, endOffset);
  }

  /**
   * Returns true if the file is strictly after the (exclusive) resume point (fromVersion,
   * fromIndex).
   */
  private static boolean isAfterStartBoundary(IndexedFile file, long fromVersion, long fromIndex) {
    return file.getVersion() > fromVersion
        || (file.getVersion() == fromVersion && file.getIndex() > fromIndex);
  }

  /**
   * Applies end boundary filtering only. Used by CDC paths where start boundary filtering is
   * applied before admission to avoid wasting budget on already-processed files.
   */
  private ClosableIterator<IndexedFile> applyEndBoundaryFiltering(
      ClosableIterator<IndexedFile> result, Optional<DeltaSourceOffset> endOffset) {
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
          new IteratorCloseOps<>(result)
              .withClose(
                  iterator ->
                      iterator.takeWhile(
                          file ->
                              file.getVersion() < bound.reservoirVersion()
                                  || (file.getVersion() == bound.reservoirVersion()
                                      && file.getIndex() <= bound.index())));
    }

    return result;
  }

  @VisibleForTesting
  void validateCDFEnabledOnTable(long startVersion) {
    // Make sure CDF is enabled at startVersion during stream start.
    if (cdfEnabledOnStreamStartValidated) {
      return;
    }
    SnapshotImpl startSnapshot;
    if (startVersion == snapshotAtSourceInit.getVersion()) {
      startSnapshot = snapshotAtSourceInit;
    } else {
      try {
        startSnapshot =
            DeltaV2Snapshot$.MODULE$.getKernelSnapshot(
                streamingBackend.getSnapshotAt(startVersion));
      } catch (io.delta.kernel.exceptions.KernelException e) {
        // startVersion may not yet exist (e.g. startingVersion=latest resolves to latest+1).
        // TODO(#6745): narrow this catch once kernel exposes a specific exception subclass
        // for "version not yet materialized".
        return;
      }
    }
    // TODO: Replace the Kernel startSnapshot with a V1 snapshot to remove this conversion.
    if (!isCDFEnabled(KernelActionUtils$.MODULE$.metadataFromKernel(startSnapshot.getMetadata()))) {
      throw new RuntimeException(
          DeltaErrors.changeDataNotRecordedException(startVersion, startVersion, startVersion));
    }
    cdfEnabledOnStreamStartValidated = true;
  }

  private ClosableIterator<IndexedFile> filterDeltaLogs(
      long startVersion, Optional<DeltaSourceOffset> endOffset) {
    ClosableIterator<DeltaSingleCommit> commits =
        new IteratorCloseOps<>(getCommitsFromRange(startVersion, endOffset, ACTION_SET))
            // TODO: Remove once the table manager returns DeltaSingleCommit.
            .withClose(iterator -> iterator.map(DeltaV2SingleCommit$.MODULE$::apply));
    ClosableIterator<IndexedFile> files =
        new IteratorFlatMapCloseOp<>(commits)
            .flatMapWithClose(
                commit -> processCommitToIndexedFiles(commit, startVersion, endOffset));
    return wrapIteratorWithCommitIteratorClose(files, commits);
  }

  private ClosableIterator<IndexedFile> filterDeltaLogsWithRateLimitForCDC(
      long startVersion,
      long lastProcessedVersion,
      long lastProcessedIndex,
      Optional<DeltaSource.AdmissionLimits> limits,
      Optional<DeltaSourceOffset> endOffset) {
    ClosableIterator<CommitActions> commits =
        getCommitsFromRange(startVersion, endOffset, CDC_ACTION_SET);

    // Stop processing further commits once the budget is spent.
    if (limits.isPresent()) {
      commits =
          new IteratorCloseOps<>(commits)
              .withClose(
                  iterator ->
                      iterator.takeWhile(
                          commit -> {
                            if (limits.get().hasCapacity()) {
                              return true;
                            }
                            Utils.closeCloseables(commit);
                            return false;
                          }));
    }

    ClosableIterator<IndexedFile> files =
        new IteratorFlatMapCloseOp<>(commits)
            .flatMapWithClose(
                commit ->
                    processCommitToIndexedFilesForCDC(
                        commit,
                        startVersion,
                        limits,
                        endOffset,
                        lastProcessedVersion,
                        lastProcessedIndex));
    return wrapIteratorWithCommitIteratorClose(files, commits);
  }

  /** Resolves the version range and returns commit actions from delta logs. */
  private ClosableIterator<CommitActions> getCommitsFromRange(
      long startVersion, Optional<DeltaSourceOffset> endOffset, Set<DeltaAction> actionSet) {
    // Schema tracking needs every metadata-change commit; failOnDataLoss=false may skip one.
    Preconditions.checkArgument(
        options.failOnDataLoss() || !metadataEvolutionHandler.shouldTrackMetadataChange(),
        "Using schema from schema tracking log cannot tolerate missing commit files.");

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
        endVersionOpt = Optional.of(streamingBackend.update().version());
      }

      // After capping, check if startVersion is beyond the endVersion.
      // This can happen when all files in the batch come from the initial snapshot
      // (e.g., offset was bumped to next version due to END_INDEX, but no new commits exist).
      if (startVersion > endVersionOpt.get()) {
        return new ClosableWrapper<>(List$.MODULE$.<CommitActions>empty().iterator()).toClosable();
      }
    } else {
      // When endOffset is empty (offset discovery), check if startVersion exceeds the current
      // latest version. We must load the current latest (not snapshotAtSourceInit) because new
      // commits may have arrived since stream initialization.
      long currentLatestVersion = streamingBackend.update().version();
      if (startVersion > currentLatestVersion) {
        return new ClosableWrapper<>(List$.MODULE$.<CommitActions>empty().iterator()).toClosable();
      }
    }

    // Start commit may be missing (e.g. due to log retention). When failOnDataLoss=false, skip
    // to the earliest available version. Mid-log gaps still throw regardless of failOnDataLoss —
    // stricter than DSv1.
    CommitRange commitRange = null;
    long earliestVersionToFetch = startVersion;
    for (int attempt = 1; attempt <= FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS; attempt++) {
      try {
        commitRange =
            snapshotManager.getTableChanges(engine, earliestVersionToFetch, endVersionOpt);
        break;
      } catch (io.delta.kernel.exceptions.StartVersionNotFoundException e) {
        if (options.failOnDataLoss()
            || !e.getEarliestAvailableVersion().isPresent()
            || attempt >= FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS) {
          throw e;
        }
        earliestVersionToFetch = e.getEarliestAvailableVersion().get();
        logger.warn(
            "Start version commit {} no longer exists for table {} (attempt {}/{}). "
                + "Skipping to earliest available version {} because failOnDataLoss is false.",
            startVersion,
            tablePath,
            attempt,
            FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS,
            earliestVersionToFetch);
      } catch (io.delta.kernel.exceptions.CommitRangeNotFoundException e) {
        return new ClosableWrapper<>(List$.MODULE$.<CommitActions>empty().iterator()).toClosable();
      }
    }

    // Use getCommitActionsFromRangeUnsafe instead of CommitRange.getCommitActions() because:
    // 1. CommitRange.getCommitActions() requires a snapshot at exactly the startVersion, but when
    //    startingVersion option is used, we may not be able to recreate that exact snapshot
    //    (e.g., if log files have been cleaned up after checkpointing).
    // 2. This matches DSv1 behavior which uses snapshotAtSourceInit's P&M to interpret all
    //    AddFile actions and performs per-commit protocol validation.
    return ScalaUtils.toClosableIterator(
        StreamingHelper.getCommitActionsFromRangeUnsafe(
            engine,
            (io.delta.kernel.internal.commitrange.CommitRangeImpl) commitRange,
            snapshotAtSourceInit.getPath(),
            actionSet));
  }

  /**
   * Processes a single commit and returns an iterator of IndexedFiles wrapped with BEGIN/END
   * sentinels.
   */
  private ClosableIterator<IndexedFile> processCommitToIndexedFiles(
      DeltaSingleCommit commit, long startVersion, Optional<DeltaSourceOffset> endOffsetOpt) {
    try {
      long version = commit.version();

      // First pass: Validate the commit and decide whether to skip it.
      //
      // We must validate the ENTIRE commit before emitting ANY files. This is a correctness
      // requirement: commits could contain both AddFiles and RemoveFiles.
      // If we emitted AddFiles before discovering a RemoveFile(dataChange=true) later in the
      // commit, downstream would produce incorrect results.
      //
      // When schema tracking is enabled, skip per-commit metadata verification — the handler
      // manages schema changes through the barrier protocol instead.
      boolean trackingMetadataChange = metadataEvolutionHandler.shouldTrackMetadataChange();
      CommitValidationResult validationResult =
          validateCommitAndDecideSkipping(
              commit,
              version,
              startVersion,
              snapshotAtSourceInit.getPath(),
              endOffsetOpt,
              /* verifyMetadataAction= */ !trackingMetadataChange);

      // Second pass: Build a lazy iterator of IndexedFiles.
      //
      //   BEGIN (BASE_INDEX) + [metadata barrier] + actual file actions + END (END_INDEX)
      //
      // These sentinel IndexedFiles have null file actions and are used for proper offset
      // tracking:
      //   - BASE_INDEX: marks "before any files in this version", allowing the offset to
      //                 reference the start of a version.
      //   - METADATA_CHANGE_INDEX: schema change barrier (stops offset generation here).
      //   - END_INDEX:  marks end of version, triggers version advancement in
      //                 buildOffsetFromIndexedFile to skip re-reading completed versions.
      //
      // See DeltaSource.addBeginAndEndIndexOffsetsForVersion for the Scala equivalent.
      ClosableIterator<IndexedFile> fileActions =
          validationResult.shouldSkipCommit
              ? new ClosableWrapper<>(List$.MODULE$.<IndexedFile>empty().iterator()).toClosable()
              : getFilesFromCommit(commit, version);

      // If metadata tracking is active, inject schema change barrier sentinel before file
      // actions. This mirrors v1's getMetadataOrProtocolChangeIndexedFileIterator concatenation
      // in filterAndGetIndexedFiles.
      ClosableIterator<IndexedFile> metadataBarrier =
          metadataEvolutionHandler.getMetadataOrProtocolChangeIndexedFileIterator(
              validationResult.metadataAction, validationResult.protocolAction, version);

      ClosableIterator<IndexedFile> inner =
          new IteratorCloseOps<>(fileActions)
              .withClose(
                  files ->
                      Iterator.single(IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX()))
                          .$plus$plus(() -> metadataBarrier)
                          .$plus$plus(() -> files)
                          .$plus$plus(
                              () ->
                                  Iterator.single(
                                      IndexedFile.sentinel(
                                          version, DeltaSourceOffset.END_INDEX()))));

      return inner;
    } catch (Exception e) {
      throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  /** Closes the current commit's files before the owning commit iterator. */
  static <T> ClosableIterator<T> wrapIteratorWithCommitIteratorClose(
      ClosableIterator<T> inner, ClosableIterator<?> commits) {
    return new ClosableIterator<T>() {
      private boolean innerClosed;

      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public T next() {
        return inner.next();
      }

      @Override
      public void close() throws IOException {
        try (commits) {
          if (!innerClosed) {
            innerClosed = true;
            inner.close();
          }
        }
      }
    };
  }

  private ClosableIterator<IndexedFile> getFilesFromCommit(DeltaSingleCommit commit, long version) {
    ClosableIterator<Action> actions =
        commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L);
    // Assign each IndexedFile a unique index within the commit. We use a mutable array
    // because variables captured by a lambda must be effectively final (never reassigned).
    long[] fileIndex = {0};

    // Only include AddFiles with dataChange=true. Skip changes that optimize or reorganize
    // data without changing the logical content.
    return new IteratorCloseOps<>(actions)
        .withClose(
            actionIterator ->
                actionIterator
                    .filter(action -> action instanceof AddFile && ((AddFile) action).dataChange())
                    .map(action -> IndexedFile.addFile(version, fileIndex[0]++, (AddFile) action)));
  }

  /** Result of commit validation, including skip decision and detected metadata/protocol. */
  private static class CommitValidationResult {
    final boolean shouldSkipCommit;
    @Nullable final Metadata metadataAction;
    @Nullable final Protocol protocolAction;

    CommitValidationResult(
        boolean shouldSkipCommit,
        @Nullable Metadata metadataAction,
        @Nullable Protocol protocolAction) {
      this.shouldSkipCommit = shouldSkipCommit;
      this.metadataAction = metadataAction;
      this.protocolAction = protocolAction;
    }
  }

  /**
   * Validates a commit, fail the stream if it's invalid and decides whether to skip it. Mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @param commit the DeltaSingleCommit representing a single commit
   * @param version the commit version
   * @param batchStartVersion Starting version of the batch being processed
   * @param tablePath the path to the Delta table
   * @param endOffsetOpt optional end offset for boundary checking
   * @param verifyMetadataAction Whether to verify metadata action compatibility
   * @return validation result containing skip decision and detected metadata/protocol actions
   * @throws RuntimeException if the commit is invalid.
   */
  private CommitValidationResult validateCommitAndDecideSkipping(
      DeltaSingleCommit commit,
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
        return new CommitValidationResult(false, null, null);
      }
    }

    // "Changes" are commits that contain both AddFile and RemoveFile actions (e.g. UPDATE,
    // MERGE that rewrites data files). Allowing changes means these commits won't cause failures.
    boolean shouldAllowChanges = options.ignoreChanges() || ignoreFileDeletion || skipChangeCommits;
    // "Deletes" are commits that contain only RemoveFile actions without any AddFile actions
    // (e.g. DELETE that purely removes data). This is a subset of changes, so allowing changes
    // implies allowing deletes.
    boolean shouldAllowDeletes = shouldAllowChanges || options.ignoreDeletes();

    boolean hasFileAdd = false;
    boolean shouldSkipCommit = false;
    Metadata metadataAction = null;
    Protocol protocolAction = null;
    String removeFileActionPath = null;
    String operation = null;

    try (ClosableIterator<Action> actionsIter =
        commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L)) {
      while (actionsIter.hasNext()) {
        Action action = actionsIter.next();
        if (action instanceof AddFile && ((AddFile) action).dataChange()) {
          hasFileAdd = true;
        } else if (action instanceof RemoveFile && ((RemoveFile) action).dataChange()) {
          // skip change commits include delete-only commits
          shouldSkipCommit = skipChangeCommits;
          if (removeFileActionPath == null) {
            removeFileActionPath = ((RemoveFile) action).path();
          }
        } else if (action instanceof Metadata) {
          metadataAction =
              validateMetadata(
                  (Metadata) action,
                  metadataAction,
                  version,
                  batchStartVersion,
                  endOffsetOpt,
                  verifyMetadataAction);
        } else if (action instanceof Protocol) {
          Preconditions.checkArgument(
              protocolAction == null,
              "Should not encounter two protocol actions in the same commit of version %d",
              version);
          protocolAction = (Protocol) action;
        } else if (action instanceof CommitInfo) {
          CommitInfo commitInfo = (CommitInfo) action;
          operation =
              commitInfo.operation() == null
                  ? null
                  : String.format(
                      "%s (%s)",
                      commitInfo.operation(),
                      CollectionConverters.asJava(commitInfo.operationParameters()));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to process commit at version " + version, e);
    }

    if (removeFileActionPath != null) {
      if (hasFileAdd && !shouldAllowChanges) {
        // Commit contains data changes (adds + removes) and changes are disallowed.
        String changeInfo = operation != null ? operation : removeFileActionPath;
        throw (RuntimeException)
            DeltaErrors.deltaSourceIgnoreChangesError(version, changeInfo, tablePath);
      } else if (!hasFileAdd && !shouldAllowDeletes) {
        // Commit contains only removes (deletes) and deletes are disallowed.
        throw (RuntimeException)
            DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFileActionPath, tablePath);
      }
    }

    return new CommitValidationResult(shouldSkipCommit, metadataAction, protocolAction);
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
    // TODO: Use a V1 snapshot for snapshotAtSourceInit to remove these conversions.
    if (version < snapshotAtSourceInit.getVersion()) {
      newMetadata =
          KernelActionUtils$.MODULE$.metadataFromKernel(snapshotAtSourceInit.getMetadata());
      oldMetadata = metadata;
    } else {
      newMetadata = metadata;
      oldMetadata =
          KernelActionUtils$.MODULE$.metadataFromKernel(snapshotAtSourceInit.getMetadata());
    }

    // Table ID has changed during streaming
    if (!Objects.equals(newMetadata.id(), oldMetadata.id())) {
      throw (RuntimeException)
          DeltaErrors.differentDeltaTableReadByStreamingSource(newMetadata.id(), oldMetadata.id());
    }

    // Partition column change will be ignored if user enable the unsafe flag
    Seq<String> newPartitionColumns, oldPartitionColumns;
    if (schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges()) {
      newPartitionColumns = (Seq<String>) Seq$.MODULE$.empty();
      oldPartitionColumns = (Seq<String>) Seq$.MODULE$.empty();
    } else {
      newPartitionColumns = newMetadata.partitionColumns().toList();
      oldPartitionColumns = oldMetadata.partitionColumns().toList();
    }

    checkNonAdditiveSchemaChanges(oldMetadata, newMetadata, validatedDuringStreamStart);

    // Other standard read compatibility changes
    if (!validatedDuringStreamStart
        || !schemaReadOptions
            .forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart()) {

      StructType schemaChange = metadata.schema();

      // There is a schema change. All files after this commit will use `schemaChange`. Hence, we
      // check whether we can use `schema` (the fixed source schema we use in the same run of the
      // query) to read these new files safely.
      boolean backfilling = version < snapshotAtSourceInit.getVersion();

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

  // Non-additive schema changes include rename column, drop column and change column type.
  // When schema tracking is enabled, this method is NOT called — the MetadataEvolutionHandler
  // manages these changes through the barrier protocol instead.
  private void checkNonAdditiveSchemaChanges(
      Metadata oldMetadata, Metadata newMetadata, boolean validatedDuringStreamStart) {
    boolean shouldTrackSchema;
    if (schemaReadOptions.typeWideningEnabled()
        && schemaReadOptions.enableSchemaTrackingForTypeWidening()
        && TypeWidening.containsWideningTypeChanges(oldMetadata.schema(), newMetadata.schema())) {
      // If schema tracking is enabled for type widening, we will detect widening type changes and
      // block the stream until the user sets `allowSourceColumnTypeChange` - similar to handling
      // DROP/RENAME for column mapping.
      shouldTrackSchema = true;
    } else if (schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges()) {
      shouldTrackSchema = false;
    } else {
      // Delegate to the shared utility which handles all column mapping mode transitions
      // (both enabled, upgrade from none, downgrade to none).
      shouldTrackSchema =
          !DeltaColumnMapping$.MODULE$.hasNoColumnMappingSchemaChanges(
              newMetadata,
              oldMetadata,
              schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges());
    }

    if (shouldTrackSchema) {
      throw (RuntimeException)
          DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
              spark, oldMetadata.schema(), newMetadata.schema(), !validatedDuringStreamStart);
    }
  }

  /**
   * Validate and lazily initialize the metadata tracking log for planned batches during stream
   * (re)start. Mirrors v1's {@code validateAndInitMetadataLogForPlannedBatchesDuringStreamStart}.
   *
   * <p>Called from {@code planInputPartitions} (the v2 equivalent of v1's {@code getBatch}).
   */
  private void validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(
      long startVersion, DeltaSourceOffset endOffset) {
    long endVersionForMetadataLogInit =
        endOffset.index() == DeltaSourceOffset.BASE_INDEX()
            ? endOffset.reservoirVersion() - 1
            : endOffset.reservoirVersion();

    // For eager initialization, we initialize the log right now.
    if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
      metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
          startVersion, endVersionForMetadataLogInit, /* alwaysFailUponLogInitialized= */ false);
    }

    // Check for column mapping + streaming incompatible schema changes
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(startVersion, endVersionForMetadataLogInit);
  }

  /**
   * Check read-incompatible schema changes during stream (re)start so we could fail fast.
   *
   * <p>This is called ONCE during the first latestOffset call to catch cases that normal per-commit
   * validation (checkReadIncompatibleSchemaChanges) misses.
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
    // When schema tracking is enabled, the MetadataEvolutionHandler manages schema changes
    // through the barrier protocol — skip the traditional compatibility check.
    if (metadataEvolutionHandler.shouldTrackMetadataChange()) return;

    if (hasCheckedReadIncompatibleSchemaChangesOnStreamStart) return;

    SnapshotImpl startVersionSnapshot = null;
    Exception err = null;
    try {
      startVersionSnapshot =
          DeltaV2Snapshot$.MODULE$.getKernelSnapshot(
              streamingBackend.getSnapshotAt(batchStartVersion));
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
        // TODO: Replace the Kernel startVersionSnapshot with a V1 snapshot to remove
        // this metadata conversion.
        checkReadIncompatibleSchemaChanges(
            KernelActionUtils$.MODULE$.metadataFromKernel(startVersionSnapshot.getMetadata()),
            startVersionSnapshot.getVersion(),
            batchStartVersion,
            batchEndVersion,
            /* validatedDuringStreamStart= */ true);
        // If end version is defined (i.e. we have a pending batch), let's also eagerly check all
        // intermediate schema changes against the stream read schema to capture corners cases such
        // as rename and rename back.
        if (batchEndVersion != null) {
          for (Map.Entry<Long, Metadata> entry :
              StreamingHelper.collectMetadataActions(
                      batchStartVersion, Optional.of(batchEndVersion), snapshotManager, engine)
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
   * Validates that the analysis-time schema matches the latest snapshot schema. This catches cases
   * where the table schema changed after the streaming query was analyzed, and the user restarts
   * the stream with a stale DataFrame.
   *
   * @param dataSchema data columns from analysis time (excludes partition columns)
   * @param partitionSchema partition columns from analysis time
   * @param snapshotSchema full table schema from the latest snapshot at stream start
   */
  @VisibleForTesting
  static void validateSchemaCompatibilityOnStartup(
      StructType dataSchema, StructType partitionSchema, StructType snapshotSchema) {
    // Reconstruct the full analysis-time table schema from dataSchema + partitionSchema.
    // StructType is immutable — add() returns a new instance without modifying the original.
    StructType querySchema = dataSchema;
    for (StructField field : partitionSchema.fields()) {
      querySchema = querySchema.add(field);
    }
    // Sort both sides so a partition column declared in the middle of the table doesn't
    // trip the check.
    StructType sortedQuery = sortFieldsByName(querySchema);
    StructType sortedSnapshot = sortFieldsByName(snapshotSchema);
    // equalsStructurally checks types + nullability but ignores field names;
    // equalsStructurallyByName checks names but ignores types/nullability.
    boolean typesAndNullabilityMatch =
        DataType.equalsStructurally(sortedQuery, sortedSnapshot, /* ignoreNullability= */ false);
    boolean namesMatch =
        DataType.equalsStructurallyByName(sortedQuery, sortedSnapshot, CASE_INSENSITIVE_RESOLVER);
    if (!typesAndNullabilityMatch || !namesMatch) {
      throw DeltaErrors.streamingSchemaMismatchOnRestart(querySchema, snapshotSchema);
    }
  }

  private static final Function2<String, String, Object> CASE_INSENSITIVE_RESOLVER =
      new AbstractFunction2<String, String, Object>() {
        @Override
        public Object apply(String a, String b) {
          return a.equalsIgnoreCase(b);
        }
      };

  private static StructType sortFieldsByName(StructType schema) {
    StructField[] fields = Arrays.copyOf(schema.fields(), schema.fields().length);
    Arrays.sort(fields, Comparator.comparing(StructField::name));
    StructType result = new StructType();
    for (StructField f : fields) {
      result = result.add(f);
    }
    return result;
  }

  /**
   * Adds dummy BEGIN_INDEX and END_INDEX IndexedFiles for {@code version} before and after the
   * contents of the iterator. The contents of the iterator must be the IndexedFiles that correspond
   * to this version.
   */
  private static ClosableIterator<IndexedFile> addBeginAndEndIndexOffsetsForVersion(
      long version, ClosableIterator<IndexedFile> files) {
    return new IteratorCloseOps<>(files)
        .withClose(
            iterator ->
                Iterator.single(IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX()))
                    .$plus$plus(() -> iterator)
                    .$plus$plus(
                        () ->
                            Iterator.single(
                                IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX()))));
  }

  /**
   * This method computes the initial snapshot to read when Delta Source was initialized on a fresh
   * stream.
   *
   * @return an iterator of IndexedFiles for the initial snapshot
   */
  private ClosableIterator<IndexedFile> getSnapshotAt(long version) {
    if (initialSnapshot == null || version != initialSnapshotVersion) {
      cleanUpSnapshotResources();
      Snapshot snapshot = streamingBackend.getSnapshotAt(version);

      initialSnapshot = new DeltaSourceSnapshot(spark, snapshot, Seq$.MODULE$.empty());
      initialSnapshotVersion = version;

      // This handles a special case for schema tracking log when it's initialized but the initial
      // snapshot's schema has changed, suppose:
      // 1. The stream starts and looks at the initial snapshot to compute the starting offset, say
      //    at version 0 with schema <a>
      // 2. User renames a column, creates version 1 with schema <b>
      // 3. The read compatibility check fails during scanning version 1, initializes schema log
      //    using the initial snapshot's schema (<a>, because that's the safest thing to do as we
      //    have not served any data from initial snapshot yet) and exits stream.
      // 4. Stream restarts, since no starting offset was generated, it will retry loading the
      //    initial snapshot, which is now at version 1, but the tracked schema <a> is now different
      //    from the "new" initial snapshot schema! Worse, since schema tracking ignores any schema
      //    changes inside initial snapshot, we will then be reading the files using a wrong schema!
      // The below logic allows us to detect any discrepancies when reading initial snapshot using
      // a tracked schema, and reinitialize the log if needed.
      if (metadataEvolutionHandler.shouldTrackMetadataChange()
          && initialSnapshot.snapshot().version() >= readSnapshotDescriptor.version()) {
        metadataEvolutionHandler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
            snapshot.metadata(),
            snapshot.protocol(),
            initialSnapshot.snapshot().version(),
            // The new schema should replace the previous initialized schema for initial snapshot
            /* replace= */ true);
      }
    }

    // TODO: Add range-aware iteration to DeltaSourceSnapshot so V1 and V2 can filter the
    // already-processed prefix before decoding files.
    ClosableIterator<IndexedFile> snapshotFiles =
        new ClosableWrapper<>(
                initialSnapshot
                    .iterator()
                    .map(file -> IndexedFile.addFile(file.version(), file.index(), file.add())))
            .toClosable();
    return addBeginAndEndIndexOffsetsForVersion(version, snapshotFiles);
  }

  /**
   * Process a CDC commit into IndexedFiles. Validates metadata, collects CDC actions, and builds
   * IndexedFiles.
   *
   * <p>Package-private for testing.
   */
  ClosableIterator<IndexedFile> processCommitToIndexedFilesForCDC(
      CommitActions commit,
      long startVersion,
      Optional<DeltaSource.AdmissionLimits> limits,
      Optional<DeltaSourceOffset> endOffsetOpt,
      long lastProcessedVersion,
      long lastProcessedIndex) {
    try {
      long version = commit.getVersion();
      long timestamp = commit.getTimestamp();

      if (isEndOffsetAtBaseIndex(endOffsetOpt, version)) {
        return new ClosableWrapper<>(
                CollectionConverters.asScala(
                        Arrays.asList(
                            IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX()),
                            IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX())))
                    .iterator())
            .toClosable();
      }

      List<IndexedFile> result =
          collectAndBuildCDCIndexedFiles(commit, version, timestamp, startVersion, endOffsetOpt);

      // Pre-filter already-processed files from the resume commit BEFORE admission so they
      // don't consume budget.
      if (version == lastProcessedVersion) {
        result.removeIf(f -> !isAfterStartBoundary(f, lastProcessedVersion, lastProcessedIndex));
      }

      if (limits.isPresent()) {
        result = applyPerCommitCDCAdmission(result, limits.get(), version);
        if (result.isEmpty()) {
          return new ClosableWrapper<>(List$.MODULE$.<IndexedFile>empty().iterator()).toClosable();
        }
      } else {
        result.add(IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX()));
      }
      return new ClosableWrapper<>(CollectionConverters.asScala(result).iterator()).toClosable();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Utils.closeCloseables(commit);
    }
  }

  /**
   * Scans a commit's actions in a single pass: validates metadata, checks CDF is enabled, detects
   * no-op MERGEs, and collects CDC/Add/Remove files into IndexedFiles.
   *
   * <p>If schema tracking is on and this commit's metadata or protocol differs from source-init,
   * returns a single barrier sentinel instead of BASE + files + END. V1 splits this across
   * DeltaSourceCDCSupport.filterAndIndexDeltaLogs (barrier injection) and
   * IndexedChangeFileSeq.filterFiles (short-circuit); V2 collapses both here.
   */
  private List<IndexedFile> collectAndBuildCDCIndexedFiles(
      CommitActions commit,
      long version,
      long timestamp,
      long startVersion,
      Optional<DeltaSourceOffset> endOffsetOpt)
      throws IOException {
    boolean trackingMetadataChange = metadataEvolutionHandler.shouldTrackMetadataChange();

    // Phase 1: scan actions to validate metadata and collect CDC/Add/Remove files.
    List<CDCDataFile> explicitCdcFiles = new ArrayList<>();
    // Single ordered list for inferred CDC files, preserving delta-log action ordering.
    // DSv1 assigns IndexedFile.index via zipWithIndex on the original action sequence.
    List<CDCDataFile> inferredCdcFiles = new ArrayList<>();
    boolean shouldSkip = false;
    Metadata metadataAction = null;
    Protocol protocolAction = null;

    // TODO: Remove this adapter when CDC reads V1 actions through DeltaSingleCommit.
    try (ClosableIterator<ColumnarBatch> actionsIter =
        ScalaUtils.toClosableIterator(commit.getActions())) {
      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        for (int rowId = 0; rowId < batch.getSize(); rowId++) {
          // Metadata: check schema changes + CDF still enabled. Skip the read-compat check when
          // tracking — the barrier protocol below handles divergence.
          Optional<io.delta.kernel.internal.actions.Metadata> metadataOpt =
              StreamingHelper.getMetadata(batch, rowId);
          // TODO: Read CDC commits as V1 actions instead of Kernel batches to remove
          // this Metadata conversion.
          if (metadataOpt.isPresent()) {
            metadataAction =
                validateMetadata(
                    KernelActionUtils$.MODULE$.metadataFromKernel(metadataOpt.get()),
                    metadataAction,
                    version,
                    startVersion,
                    endOffsetOpt,
                    /* verifyMetadataAction= */ !trackingMetadataChange);
            if (!isCDFEnabled(metadataAction)) {
              throw new RuntimeException(
                  DeltaErrors.changeDataNotRecordedException(
                      version,
                      startVersion,
                      endOffsetOpt.map(DeltaSourceOffset::reservoirVersion).orElse(version)));
            }
          }

          // Protocol: captured so the barrier fires on protocol-only changes.
          Optional<io.delta.kernel.internal.actions.Protocol> protocolOpt =
              StreamingHelper.getProtocol(batch, rowId);
          if (protocolOpt.isPresent()) {
            Preconditions.checkArgument(
                protocolAction == null,
                "Should not encounter two protocol actions in the same commit of version %d",
                version);
            // TODO: Read CDC commits as V1 actions instead of Kernel batches to remove
            // this Protocol conversion.
            protocolAction = KernelActionUtils$.MODULE$.protocolFromKernel(protocolOpt.get());
          }

          // CommitInfo: detect no-op MERGEs where the operation rewrites files without
          // changing logical data. These produce file actions that cancel out; inferred CDC
          // actions will be discarded after the scan to avoid emitting spurious changes.
          Optional<io.delta.kernel.internal.actions.CommitInfo> commitInfoOpt =
              StreamingHelper.getCommitInfo(batch, rowId);
          if (commitInfoOpt.isPresent()) {
            // TODO: Read CDC commits as V1 actions instead of Kernel batches to remove
            // this CommitInfo conversion.
            shouldSkip =
                CDCReader$.MODULE$.shouldSkipFileActionsInCommit(
                    KernelActionUtils$.MODULE$.commitInfoFromKernel(commitInfoOpt.get()));
          }

          // Explicit CDC files
          Optional<CDCDataFile> cdcOpt = StreamingHelper.getCDCFile(batch, rowId, timestamp);
          if (cdcOpt.isPresent()) {
            explicitCdcFiles.add(cdcOpt.get());
            continue;
          }

          // RemoveFile(dataChange=true): collect for inferred CDC
          Optional<io.delta.kernel.internal.actions.RemoveFile> removeOpt =
              StreamingHelper.getDataChangeRemove(batch, rowId);
          if (removeOpt.isPresent()) {
            inferredCdcFiles.add(CDCDataFile.fromRemoveFile(removeOpt.get(), timestamp));
            continue;
          }

          // AddFile(dataChange=true)
          Optional<AddFile> addOpt = StreamingHelper.getAddFileWithDataChange(batch, rowId);
          if (addOpt.isPresent()) {
            inferredCdcFiles.add(CDCDataFile.fromAddFile(addOpt.get(), timestamp));
          }
        }
      }
    }

    // Schema-change barrier: emit only the barrier when this commit diverges from source-init.
    // V1 injects in DeltaSourceCDCSupport.filterAndIndexDeltaLogs and short-circuits in
    // IndexedChangeFileSeq.filterFiles; V2 does both here.
    try (ClosableIterator<IndexedFile> barrierIter =
        metadataEvolutionHandler.getMetadataOrProtocolChangeIndexedFileIterator(
            metadataAction, protocolAction, version)) {
      if (barrierIter.hasNext()) {
        List<IndexedFile> result = new ArrayList<>();
        result.add(barrierIter.next());
        return result;
      }
    }

    // No-op MERGE: discard inferred actions (DSv1's isValidIndexedFile / shouldSkip).
    // shouldSkip doesn't apply to explicit CDC files — only inferred Add/Remove.
    if (shouldSkip) {
      inferredCdcFiles.clear();
    }

    // Phase 2: build IndexedFiles from collected actions.
    List<IndexedFile> result = new ArrayList<>();
    result.add(IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX()));
    long fileIndex = 0;

    // Explicit CDC files and inferred files are mutually exclusive: if a commit contains any
    // AddCDCFile actions, only those are emitted and AddFile/RemoveFile are ignored.
    if (!explicitCdcFiles.isEmpty()) {
      for (CDCDataFile cdcFile : explicitCdcFiles) {
        result.add(IndexedFile.cdc(version, fileIndex++, cdcFile));
      }
    } else {
      // Bucket AddFile and RemoveFile by path to detect same-path pairs.
      // DV-based deletes reuse the same parquet file (marking rows in the DV instead of rewriting),
      // so both the Remove and Add reference the same path.
      Map<String, io.delta.kernel.internal.actions.AddFile> addByPath = new HashMap<>();
      Map<String, io.delta.kernel.internal.actions.RemoveFile> removeByPath = new HashMap<>();
      for (CDCDataFile cdc : inferredCdcFiles) {
        if (cdc.getAddFile() != null) {
          io.delta.kernel.internal.actions.AddFile prev =
              addByPath.put(cdc.getAddFile().getPath(), cdc.getAddFile());
          Preconditions.checkArgument(
              prev == null,
              "Unexpected duplicate AddFile at path %s in a single commit",
              cdc.getAddFile().getPath());
        } else if (cdc.getRemoveFile() != null) {
          io.delta.kernel.internal.actions.RemoveFile prev =
              removeByPath.put(cdc.getRemoveFile().getPath(), cdc.getRemoveFile());
          Preconditions.checkArgument(
              prev == null,
              "Unexpected duplicate RemoveFile at path %s in a single commit",
              cdc.getRemoveFile().getPath());
        }
      }

      Set<String> samePathKeys = new HashSet<>(addByPath.keySet());
      samePathKeys.retainAll(removeByPath.keySet());

      // Walk inferredCdcFiles in delta-log order. Non-pair files are emitted as-is. Same-path
      // pairs are emitted at the FIRST occurrence of the path (the counterpart, encountered
      // later in the loop, is skipped).
      Set<String> emittedPairs = new HashSet<>();
      for (CDCDataFile cdc : inferredCdcFiles) {
        String path = cdc.getPath();
        if (!samePathKeys.contains(path)) {
          // File with unique paths: emit as-is.
          result.add(IndexedFile.cdc(version, fileIndex++, cdc));
        } else if (emittedPairs.add(path)) {
          // First occurrence of a same-path pair: emit the consolidated pair entries.
          io.delta.kernel.internal.actions.AddFile add = addByPath.get(path);
          io.delta.kernel.internal.actions.RemoveFile remove = removeByPath.get(path);
          if (add.getDeletionVector().isPresent() || remove.getDeletionVector().isPresent()) {
            // DV-based delete/restore: compute the bitmap diff and emit one CDC file per diff
            // entry. Path.toString() avoids percent-encoding that breaks DV file resolution.
            String dataPath = snapshotAtSourceInit.getDataPath().toString();
            for (CDCDataFile f :
                CDCDeletionVectorHelper.generateDVDiffCDCFiles(
                    add, remove, timestamp, dataPath, hadoopConf)) {
              result.add(IndexedFile.cdc(version, fileIndex++, f));
            }
          } else {
            // Same-path Add+Remove without DV: emit plain insert + delete.
            result.add(
                IndexedFile.cdc(version, fileIndex++, CDCDataFile.fromAddFile(add, timestamp)));
            result.add(
                IndexedFile.cdc(
                    version, fileIndex++, CDCDataFile.fromRemoveFile(remove, timestamp)));
          }
        }
        // Else: second occurrence of an already-emitted same-path pair; skip.
      }
    }

    return result;
  }

  /**
   * Per-commit CDC admission control. Mirrors DSv1's IndexedChangeFileSeq.filterFiles.
   *
   * <p>Input is a single commit's IndexedFiles: either a schema-change barrier (passed through
   * unchanged), or an optional BASE sentinel followed by data files. Applies batch or per-file
   * admission and appends an END sentinel only when all data files are admitted. Returns empty list
   * on batch-reject so the offset doesn't advance.
   */
  @VisibleForTesting
  List<IndexedFile> applyPerCommitCDCAdmission(
      List<IndexedFile> commitFiles,
      DeltaSource.AdmissionLimits admissionLimits,
      long commitVersion) {
    // Schema-change barrier: pass through. collectAndBuildCDCIndexedFiles returns the barrier as a
    // singleton, so it can only appear as element 0.
    if (!commitFiles.isEmpty()
        && commitFiles.get(0).getIndex() == DeltaSourceOffset.METADATA_CHANGE_INDEX()) {
      return new ArrayList<>(commitFiles);
    }

    // Split out the optional BASE sentinel from data files.
    Optional<IndexedFile> baseSentinel = Optional.empty();
    List<IndexedFile> dataFiles = new ArrayList<>();
    for (IndexedFile file : commitFiles) {
      if (file.hasFileAction()) {
        dataFiles.add(file);
      } else {
        Preconditions.checkState(
            !baseSentinel.isPresent(),
            String.format(
                "Expected at most one BASE sentinel per commit, got multiple for version %s",
                commitVersion));
        baseSentinel = Optional.of(file);
      }
    }

    // Batch-admit all or nothing when we can't split the commit:
    //   - Explicit CDC: can't separate update_preimage/postimage
    //   - Inferred CDC with DVs: can't split add/remove pairs
    boolean requiresBatchAdmission =
        dataFiles.stream()
            .anyMatch(
                f ->
                    f.isAddCDCFile()
                        || (f.getCDCDataFile() != null && f.getCDCDataFile().hasDeletionVector()));

    List<IndexedFile> admittedData;
    if (requiresBatchAdmission) {
      if (admissionLimits.admit(toScalaAdmittableSeq(dataFiles))) {
        admittedData = dataFiles;
      } else {
        return Collections.emptyList();
      }
    } else {
      admittedData = new ArrayList<>();
      for (IndexedFile file : dataFiles) {
        if (admissionLimits.admit(file)) {
          admittedData.add(file);
        } else {
          break;
        }
      }
    }

    List<IndexedFile> result = new ArrayList<>();
    baseSentinel.ifPresent(result::add);
    result.addAll(admittedData);
    // Append END only if the entire commit was admitted. Partial admission leaves the offset
    // on the last admitted file so the next batch resumes from there.
    if (admittedData.size() >= dataFiles.size()) {
      result.add(IndexedFile.sentinel(commitVersion, DeltaSourceOffset.END_INDEX()));
    }
    return result;
  }

  /** Converts a list of IndexedFiles to a Scala Seq of AdmittableFile for batch admission. */
  private static Seq<AdmittableFile> toScalaAdmittableSeq(List<IndexedFile> files) {
    return CollectionConverters.asScala(
            files.stream().map(f -> (AdmittableFile) f).collect(Collectors.toList()))
        .toSeq();
  }

  private static boolean isCDFEnabled(Metadata metadata) {
    // Fall back to the deprecated delta.enableChangeDataCapture key when
    // delta.enableChangeDataFeed is not set.
    Map<String, String> config = CollectionConverters.asJava(metadata.configuration());
    String value = config.get("delta.enableChangeDataFeed");
    if (value == null) {
      value = config.get("delta.enableChangeDataCapture");
    }
    return value != null && value.equalsIgnoreCase("true");
  }

  /** Checks if the endOffset is at BASE_INDEX for the given version (early exit condition). */
  private static boolean isEndOffsetAtBaseIndex(
      Optional<DeltaSourceOffset> endOffsetOpt, long version) {
    return endOffsetOpt.isPresent()
        && endOffsetOpt.get().reservoirVersion() == version
        && endOffsetOpt.get().index() == DeltaSourceOffset.BASE_INDEX();
  }

  /**
   * Validates a metadata action: checks for duplicates and read-incompatible schema changes.
   *
   * @return the validated metadata (for assignment back to the caller's tracking variable)
   */
  private Metadata validateMetadata(
      Metadata metadata,
      Metadata existing,
      long version,
      long startVersion,
      Optional<DeltaSourceOffset> endOffsetOpt,
      boolean verifyMetadataAction) {
    Preconditions.checkArgument(
        existing == null,
        "Should not encounter two metadata actions in the same commit of version %d",
        version);
    // When schema tracking is enabled, verifyMetadataAction is false — the handler manages
    // schema changes through the barrier protocol instead.
    if (verifyMetadataAction) {
      Long batchEndVersion = endOffsetOpt.map(DeltaSourceOffset::reservoirVersion).orElse(null);
      checkReadIncompatibleSchemaChanges(
          metadata,
          version,
          startVersion,
          batchEndVersion,
          /* validatedDuringStreamStart= */ false);
    }
    return metadata;
  }
}
