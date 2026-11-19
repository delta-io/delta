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
package org.apache.spark.sql.delta.v2.read

import java.io.IOException
import java.io.UncheckedIOException
import java.nio.channels.ClosedByInterruptException
import java.time.ZoneId
import java.util._

import scala.collection.Iterator
import scala.collection.immutable.Seq
import scala.jdk.javaapi.CollectionConverters
import scala.util.Using
import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.DeltaSingleCommit
import org.apache.spark.sql.delta.NoMapping
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.SnapshotDescriptor
import org.apache.spark.sql.delta.TypeWidening
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.AddCDCFile
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.DeltaSourceSnapshot
import org.apache.spark.sql.delta.sources.DeltaSource
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog
import org.apache.spark.sql.delta.sources.DeltaSourceOffset
import org.apache.spark.sql.delta.sources.DeltaSourceOffsetLogic
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaStreamingBackend
import org.apache.spark.sql.delta.sources.DeltaStreamUtils
import org.apache.spark.sql.delta.sources.PersistedMetadata
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator.ClosableWrapper
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorFlatMapCloseOp
import org.apache.spark.sql.delta.v2.CDCDeletionVectorHelper
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext
import org.apache.spark.sql.delta.v2.read.DeltaV2MicroBatchStreamUtils._
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager
import org.apache.spark.sql.delta.v2.utils.PartitionUtils
import org.apache.spark.sql.delta.v2.utils.PartitionValueCaster
import org.apache.spark.sql.delta.v2.utils.ScalaUtils
import org.apache.spark.sql.delta.v2.utils.StreamingHelper
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

// TODO(#5318): Use DeltaErrors error framework for consistent error handling.
/**
 * Package-private micro-batch stream implementation for Delta's Spark DataSource V2 read path.
 *
 * <p>This class must remain package-private so callers outside {@code v2.read} depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
private[read] class DeltaV2MicroBatchStream(
      tableManager: DeltaV2TableManager,
      override protected val snapshotAtSourceInit: Snapshot,
      hadoopConf: Configuration,
      override protected val spark: SparkSession,
      override protected val options: DeltaOptions,
      inputTablePath: String,
      dataSchema: StructType,
      partitionSchema: StructType,
      readDataSchema: StructType,
      ddlOrderedReadOutputSchema: StructType,
      inputDataFilters: Array[Filter],
      scalaOptions: scala.collection.immutable.Map[String, String],
      metadataTrackingLog: Option[DeltaSourceMetadataTrackingLog],
      metadataPath: String,
      originalStreamingQueryContext: DeltaV2QueryContext)
    extends MicroBatchStream
        with SupportsAdmissionControl
        with SupportsTriggerAvailableNow
        with DeltaSourceOffsetLogic {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DeltaV2MicroBatchStream])

  // Spark can call initialOffset multiple times while constructing and executing the first batch.
  private var cachedInitialOffset: DeltaSourceOffset = null

  /**
   * Tracks whether this is the first batch for this stream (no checkpointed offset).
   *
   * <p>- First batch: initialOffset() -> latestOffset(Offset, ReadLimit) - Set `isFirstBatch` to
   * true in initialOffset() - in latestOffset(Offset, ReadLimit), use `isFirstBatch` to determine
   * whether to return null vs previousOffset (when no data is available) - keep `isFirstBatch` true
   * until a non-null offset is returned - Subsequent batches: latestOffset(Offset, ReadLimit)
   */
  private var isFirstBatch: Boolean = false

  /**
   * A global flag to mark whether we have done a per-stream start check for column mapping schema
   * changes (rename / drop).
   */
  @volatile private var hasCheckedReadIncompatibleSchemaChangesOnStreamStart: Boolean = false

  /** One-shot flag for {@link #validateCDFEnabledOnTable}. */
  @volatile private var cdfEnabledOnStreamStartValidated: Boolean = false

  /**
   * When AvailableNow is used, this offset will be the upper bound where this run of the query will
   * process up. We may run multiple micro batches, but the query will stop itself when it reaches
   * this offset.
   */
  private var lastOffsetForTriggerAvailableNow: Option[DeltaSourceOffset] = None

  private var isLastOffsetForTriggerAvailableNowInitialized: Boolean = false

  private var isTriggerAvailableNow: Boolean = false

  // Cached starting version to ensure idempotent behavior for "latest" starting version.
  // getStartingVersion() must return the same value across multiple calls.
  @volatile private var cachedStartingVersion: Optional[java.lang.Long] = null

  // A metadata snapshot when starting the query.
  private var initialSnapshot: DeltaSourceSnapshot = null
  private var initialSnapshotVersion: Long = -1L

  Objects.requireNonNull(tableManager, "tableManager is null")
  Objects.requireNonNull(originalStreamingQueryContext, "originalStreamingQueryContext is null")
  Objects.requireNonNull(snapshotAtSourceInit, "snapshotAtSourceInit is null")
  /** The Delta log operations that compute offsets. */
  private val streamingBackend: DeltaStreamingBackend =
      new DeltaV2StreamingBackend(
          tableManager, originalStreamingQueryContext, snapshotAtSourceInit.version)
  Objects.requireNonNull(hadoopConf, "hadoopConf is null")
  Objects.requireNonNull(spark, "spark is null")
  Objects.requireNonNull(options, "options is null")
  private val ignoreFileDeletion: Boolean = this.options.ignoreFileDeletion
  // Deprecated. Please use `skipChangeCommits` from now on.
  if (this.ignoreFileDeletion) {
    logger.warn(DeltaErrors.ignoreStreamingUpdatesAndDeletesWarning(this.spark))
  }
  private val skipChangeCommits: Boolean = this.options.skipChangeCommits
  // Normalize tablePath to ensure it ends with "/" for consistent path construction
  private val tablePath: String = {
    val normalizedTablePath = Objects.requireNonNull(inputTablePath, "tablePath is null")
    if (normalizedTablePath.endsWith("/")) normalizedTablePath else normalizedTablePath + "/"
  }
  Objects.requireNonNull(dataSchema, "dataSchema is null")
  Objects.requireNonNull(partitionSchema, "partitionSchema is null")
  Objects.requireNonNull(readDataSchema, "readDataSchema is null")
  Objects.requireNonNull(ddlOrderedReadOutputSchema, "ddlOrderedReadOutputSchema is null")
  private val dataFilters: Array[Filter] =
      Arrays.copyOf(
          Objects.requireNonNull(inputDataFilters, "dataFilters is null"),
          inputDataFilters.length)
  private val sqlConf: SQLConf = SQLConf.get
  Objects.requireNonNull(scalaOptions, "scalaOptions is null")

  private val tableId: String = this.snapshotAtSourceInit.metadata.id

  // The effective descriptor for reading, mirroring v1's readSnapshotDescriptor. When schema
  // tracking has a persisted entry, layer it onto the source-init descriptor so
  // the read schema/protocol/config reflect the evolved state. The merged entry has already
  // been written to the durable schema log during analysis (by DeltaV2Table's call to
  // getPersistedMetadataForMicroBatchStream with mergeConsecutiveSchemaChanges=true), so
  // reading it back from the log here yields the same value V1 obtains.
  private val readSnapshotDescriptor: SnapshotDescriptor = {
    val persistedMetadataAtSourceInit: Option[PersistedMetadata] =
        if (metadataTrackingLog.isDefined) metadataTrackingLog.get.getCurrentTrackedMetadata
        else Option.empty
    Objects.requireNonNull(
        if (persistedMetadataAtSourceInit.isDefined) {
          MetadataEvolutionHandler.buildReadSnapshotDescriptorFromPersistedMetadata(
              snapshotAtSourceInit, persistedMetadataAtSourceInit.get)
        } else snapshotAtSourceInit,
        "readSnapshotDescriptor is null")
  }
  private val readSchemaAtSourceInit: StructType =
      Objects.requireNonNull(readSnapshotDescriptor.schema, "readSchemaAtSourceInit is null")
  override protected val schema: StructType =
      DeltaSource.sourceSchema(spark, options, readSchemaAtSourceInit)
  private val readerSupportsDeletionVectors: Boolean =
      DeletionVectorUtils.deletionVectorsReadable(
          readSnapshotDescriptor.protocol, readSnapshotDescriptor.metadata)

  // V2 retains the partition layout from DataFrame analysis. Check that the read metadata still
  // matches that layout before the stream starts. V1 does not need this check because it loads
  // the layout when the source starts.
  locally {
    val shouldValidatePartitionSchemaOnRestart: Boolean =
        spark
            .sessionState
            .conf
            .getConf(DeltaSQLConf.STREAMING_PARTITION_SCHEMA_VALIDATION_ON_RESTART)
    if (shouldValidatePartitionSchemaOnRestart) {
      validatePartitionSchemaOnStartup(
          partitionSchema, readSnapshotDescriptor.metadata.partitionSchema, tableId)
    }
  }

  private val shouldValidateOffsets: Boolean =
      spark.sessionState.conf.getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION)
  private val excludeRegex: Option[Regex] = options.excludeRegex

  /**
   * Configuration options for handling schema changes behavior. Controls unsafe operations like
   * column mapping changes, partition column changes, nullability changes, and type widening.
   */
  override protected val schemaReadOptions: DeltaStreamUtils.SchemaReadOptions = {
    val isStreamingFromColumnMappingTable: Boolean =
        this.snapshotAtSourceInit.metadata.columnMappingMode != NoMapping
    val isTypeWideningSupportedInProtocol: Boolean =
        TypeWidening.isSupported(this.snapshotAtSourceInit.protocol)
    Objects.requireNonNull(
        DeltaStreamUtils.SchemaReadOptions.fromSparkSession(
            spark, isStreamingFromColumnMappingTable, isTypeWideningSupportedInProtocol),
        "schemaReadOptions is null")
  }
  /** Handler for non-additive schema evolution (rename/drop/type widening). */
  private val metadataEvolutionHandler: MetadataEvolutionHandler =
      new MetadataEvolutionHandler(
          spark,
          tableId,
          streamingBackend,
          tableManager,
          originalStreamingQueryContext,
          options,
          schemaReadOptions,
          metadataTrackingLog,
          readSnapshotDescriptor,
          metadataPath)

  override protected def trackingMetadataChange: Boolean =
      metadataEvolutionHandler.shouldTrackMetadataChange()

  locally {
    val shouldValidateSchemaOnRestart: Boolean =
        spark
            .sessionState
            .conf
            .getConf(DeltaSQLConf.STREAMING_SCHEMA_VALIDATION_ON_RESTART)
    // When schema tracking is enabled, the MetadataEvolutionHandler manages schema changes via
    // the schema log; the analysis-vs-snapshot mismatch this check guards against is expected on
    // restart and is surfaced via the schema-log evolution exception instead.
    if (shouldValidateSchemaOnRestart && !metadataEvolutionHandler.shouldTrackMetadataChange()) {
      validateSchemaCompatibilityOnStartup(dataSchema, partitionSchema, readSchemaAtSourceInit)
    }
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    logger.info("The streaming query reports to use Trigger.AvailableNow.")
    isTriggerAvailableNow = true
  }

  /**
   * initialize the internal states for AvailableNow if this method is called first time after
   * prepareForTriggerAvailableNow.
   */
  private def initForTriggerAvailableNowIfNeeded(startOffsetOpt: DeltaSourceOffset): Unit = {
    if (isTriggerAvailableNow && !isLastOffsetForTriggerAvailableNowInitialized) {
      isLastOffsetForTriggerAvailableNowInitialized = true
      initLastOffsetForTriggerAvailableNow(startOffsetOpt)
    }
  }

  private def initLastOffsetForTriggerAvailableNow(startOffsetOpt: DeltaSourceOffset): Unit = {
    // TODO(#6817): Explore unifying TAN offset computation into latestOffsetInternal.
    // Blocked: latestOffset per-batch shares the same conditions but must NOT bypass rate limiting.
    lastOffsetForTriggerAvailableNow =
        latestOffsetInternal(startOffsetOpt, ReadLimit.allAvailable())

    lastOffsetForTriggerAvailableNow.foreach(
        lastOffset =>
            logger.info("lastOffset for Trigger.AvailableNow has set to " + lastOffset.json()))
  }

  ////////////
  // offset //
  ////////////

  /**
   * Returns the initial offset for a streaming query to start reading from (if there's no
   * checkpointed offset).
   */
  override def initialOffset(): Offset = {
    if (cachedInitialOffset != null) {
      return cachedInitialOffset
    }

    val startingVersionOpt: Optional[java.lang.Long] = getStartingVersion()
    var version: Long = 0L
    var isInitialSnapshot: Boolean = false
    isFirstBatch = true

    if (startingVersionOpt.isPresent()) {
      version = startingVersionOpt.get()
      isInitialSnapshot = false
    } else {
      // No starting version specified in the options, use snapshot captured
      // at source initialization.
      version = snapshotAtSourceInit.version
      isInitialSnapshot = true
    }

    cachedInitialOffset =
        DeltaSourceOffset.apply(
            tableId, version, DeltaSourceOffset.BASE_INDEX, isInitialSnapshot)
    cachedInitialOffset
  }

  override def latestOffset(): Offset = {
    throw new IllegalStateException(
        "latestOffset() should not be called - use latestOffset(Offset, ReadLimit) instead")
  }

  /**
   * Get the latest offset with rate limiting (SupportsAdmissionControl).
   *
   * @param startOffset The starting offset
   * @param limit The read limit for rate limiting
   * @return The latest offset, or null if no data is available to read.
   */
  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    Objects.requireNonNull(startOffset, "startOffset should not be null for MicroBatchStream")
    Objects.requireNonNull(limit, "limit should not be null for MicroBatchStream")

    try {
      val deltaStartOffset: DeltaSourceOffset = DeltaSourceOffset.apply(tableId, startOffset)
      initForTriggerAvailableNowIfNeeded(deltaStartOffset)
      // Return null when no data is available for this batch.
      val endOffset: DeltaSourceOffset = latestOffsetInternal(deltaStartOffset, limit).orNull
      if (endOffset != null) {
        isFirstBatch = false
      }
      endOffset
    } catch { case e: Exception =>
      // Kernel's DefaultJsonHandler wraps ClosedByInterruptException (thrown by NIO
      // channels on thread interrupt) inside KernelEngineException (a RuntimeException).
      // Spark's StreamExecution.isInterruptionException recognizes
      // ClosedByInterruptException and UncheckedIOException but not
      // KernelEngineException. Re-wrap so Spark's isInterruptedByStop - which also
      // verifies state == TERMINATED - handles it as a clean stream shutdown.
      val interruptCause: Optional[ClosedByInterruptException] = findClosedByInterruptCause(e)
      if (interruptCause.isPresent()) {
        throw new UncheckedIOException(interruptCause.get())
      }
      // Translate Kernel's protocol-version error into Delta's InvalidProtocolVersionException.
      val protocolCause: Optional[UnsupportedProtocolVersionException] =
          findUnsupportedProtocolCause(e)
      if (protocolCause.isPresent()) {
        val p: UnsupportedProtocolVersionException = protocolCause.get()
        throw DeltaErrors.invalidProtocolVersionError(
                p.getTablePath(), p.getMinReaderVersion(), p.getMinWriterVersion())
      }
      throw e
    }
  }

  /**
   * Internal implementation of latestOffset using DeltaSourceOffset directly, without null checks
   * and state management.
   */
  private def latestOffsetInternal(
      deltaStartOffset: DeltaSourceOffset, limit: ReadLimit): Option[DeltaSourceOffset] = {
    val limits: Optional[DeltaSource.AdmissionLimits] =
        ScalaUtils.toJavaOptional(DeltaSource.AdmissionLimits.apply(options, limit))
    val endOffset: Option[DeltaSourceOffset] =
        getNextOffsetFromPreviousOffset(deltaStartOffset, limits, isFirstBatch)

    if (shouldValidateOffsets && endOffset.isDefined) {
      DeltaSourceOffset.validateOffsets(deltaStartOffset, endOffset.get)
    }

    endOffset
  }

  override def deserializeOffset(json: String): Offset = {
    DeltaSourceOffset.apply(tableId, json)
  }

  override def getDefaultReadLimit(): ReadLimit = {
    DeltaSource.AdmissionLimits.toReadLimit(options)
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
  private def getNextOffsetFromPreviousOffset(
      previousOffset: DeltaSourceOffset,
      limits: Optional[DeltaSource.AdmissionLimits],
      isFirstBatch: Boolean): Option[DeltaSourceOffset] = {
    // Initialize schema tracking log eagerly if possible (fresh stream, first batch).
    // This mirrors v1's getStartingOffsetFromSpecificDeltaVersion eager init path.
    if (isFirstBatch) {
      if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
        metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
            previousOffset.reservoirVersion,
            /* batchEndVersion= */ null,
            /* alwaysFailUponLogInitialized= */ false)
      }
    } else {
      var startVersionForMetadataLogInit: Long = 0L
      if (previousOffset.index == DeltaSourceOffset.BASE_INDEX) {
        startVersionForMetadataLogInit = previousOffset.reservoirVersion - 1
      } else {
        startVersionForMetadataLogInit = previousOffset.reservoirVersion
      }
      if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
        metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
            startVersionForMetadataLogInit,
            /* batchEndVersion= */ null,
            /* alwaysFailUponLogInitialized= */ false)
      }
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(
          startVersionForMetadataLogInit, batchEndVersion = None)
    }

    // Handle pending schema change offsets (two-barrier protocol).
    if (metadataEvolutionHandler.shouldTrackMetadataChange()) {
      val pendingOffset: Option[DeltaSourceOffset] =
          ScalaUtils.toScalaOption(
              metadataEvolutionHandler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(
                  previousOffset))
      if (pendingOffset.isDefined) {
        return pendingOffset
      }
    }

    val changes: ClosableIterator[IndexedFile] =
        getFileChangesWithRateLimit(
            previousOffset.reservoirVersion,
            previousOffset.index,
            previousOffset.isInitialSnapshot,
            limits)

    val lastFileChange: Option[IndexedFile] = DeltaSource.iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      // For the first batch, return empty to match DSv1's
      // getStartingOffsetFromSpecificDeltaVersion
      if (isFirstBatch) {
        return None
      }
      return Some(previousOffset)
    }
    // Block latestOffset() from generating an invalid offset by proactively
    // verifying incompatible schema changes under column mapping. See more details in the
    // method java doc.
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(
        previousOffset.reservoirVersion, batchEndVersion = None)
    val lastFile: IndexedFile = lastFileChange.get
    Some(
        DeltaSource.buildOffsetFromIndexedFile(
            tableId,
            lastFile.getVersion(),
            lastFile.getIndex(),
            previousOffset.reservoirVersion,
            previousOffset.isInitialSnapshot))
  }

  //////////
  // data //
  //////////

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset: DeltaSourceOffset = start.asInstanceOf[DeltaSourceOffset]
    val endOffset: DeltaSourceOffset = end.asInstanceOf[DeltaSourceOffset]

    val fromVersion: Long = startOffset.reservoirVersion
    val fromIndex: Long = startOffset.index
    val isInitialSnapshot: Boolean = startOffset.isInitialSnapshot

    // Validate and lazily initialize the metadata tracking log for planned batches during
    // stream (re)start. Mirrors v1's validateAndInitMetadataLogForPlannedBatchesDuringStreamStart.
    validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(fromVersion, endOffset)

    val partitionedFiles: List[PartitionedFile] = new ArrayList[PartitionedFile]()
    var totalBytesToRead: Long = 0
    val isCDC: Boolean = options.readChangeFeed
    val zoneId: ZoneId = DateTimeUtils.getZoneId(sqlConf.sessionLocalTimeZone)
    val caster: PartitionValueCaster = new PartitionValueCaster(partitionSchema, zoneId)

    try Using.resource(
        if (isCDC) {
            getFileChangesWithRateLimitForCDC(
                fromVersion,
                fromIndex,
                isInitialSnapshot,
                /* limits= */ Optional.empty(),
                Optional.of(endOffset))
        } else {
          getFileChanges(fromVersion, fromIndex, isInitialSnapshot, Optional.of(endOffset))
            .withClose[(IndexedFile, CDCDataFile)](_.map(file => (file, null)))
        }) {
      fileChanges =>
      while (fileChanges.hasNext) {
        val (indexedFile, cdcFile) = fileChanges.next()
        buildPartitionedFile(indexedFile, isCDC, cdcFile, caster).foreach { pf =>
          // Also check replay of an already planned batch, which skips latestOffset validation.
          checkDeletionVectorsReadable(indexedFile, cdcFile)
          totalBytesToRead += pf.fileSize
          partitionedFiles.add(pf)
        }
      }
    } catch { case e: IOException =>
      throw new UncheckedIOException(
          String.format(
              "Failed to get file changes for table %s from version %d index %d to offset %s",
              tablePath, Long.box(fromVersion), Long.box(fromIndex), endOffset),
          e)
    case e: RuntimeException =>
      // Same interrupt handling as latestOffset(): Kernel wraps ClosedByInterruptException
      // in KernelEngineException (a RuntimeException). Re-wrap as UncheckedIOException so
      // Spark's isInterruptedByStop recognizes it as a clean shutdown.
      val interruptCause: Optional[ClosedByInterruptException] = findClosedByInterruptCause(e)
      if (interruptCause.isPresent()) {
        throw new UncheckedIOException(interruptCause.get())
      }
      throw e
    }

    PartitionUtils.planInputPartitions(
        spark, partitionedFiles, totalBytesToRead, hadoopConf, sqlConf)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    PartitionUtils.createDeltaParquetReaderFactory(
        readSnapshotDescriptor,
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf,
        /* isWriteTimeCDCRead */ options.readChangeFeed)
  }

  private def checkDeletionVectorsReadable(
      file: IndexedFile, cdcFile: CDCDataFile): Unit = {
    if (readerSupportsDeletionVectors || !file.hasFileAction()) return
    var needsDeletionVectors: Boolean = false
    var path: String = null
    if (cdcFile != null) {
      needsDeletionVectors = cdcFile.hasDeletionVector()
      path = cdcFile.getPath()
    } else if (file.getAddFile() != null) {
      needsDeletionVectors = file.getAddFile().deletionVector != null
      path = file.getAddFile().path
    } else {
      return
    }
    if (!needsDeletionVectors) return
    if (!isExcludedPath(path)) {
      logger.warn(
          "Cannot read deletion vectors for table {} at version {} with source-init snapshot "
              + "version {} (readMode={})",
          tableId,
          Long.box(file.getVersion()),
          Long.box(snapshotAtSourceInit.version),
          if (options.readChangeFeed) "CDC" else "table")
      throw DeltaErrors.streamingDeletionVectorsNotReadableException(
          file.getVersion(), snapshotAtSourceInit.version)
    }
  }

  /**
   * Returns a {@link PartitionedFile} for the given {@link IndexedFile}, or {@code None} if the
   * entry is a sentinel or its path matches the excludeRegex.
   */
  private def buildPartitionedFile(
      indexedFile: IndexedFile,
      isCDC: Boolean,
      cdcFile: CDCDataFile,
      caster: PartitionValueCaster): Option[PartitionedFile] = {
    if (!indexedFile.hasFileAction()) return None
    if (isCDC) {
      if (cdcFile == null) {
        throw new IllegalStateException("Expected a non-null CDC payload for data files")
      }
      if (isExcludedPath(cdcFile.getPath())) return None
      return Some(
          PartitionUtils.buildCDCPartitionedFile(
              cdcFile, indexedFile.getVersion(), caster, tablePath))
    }
    if (indexedFile.getAddFile() != null) {
      val addFile: AddFile = indexedFile.getAddFile()
      if (isExcludedPath(addFile.path)) return None
      return Some(PartitionUtils.buildPartitionedFile(addFile, caster, tablePath))
    }
    None
  }

  ///////////////
  // lifecycle //
  ///////////////

  override def commit(end: Offset): Unit = {
    // IMPORTANT: for future developers, please place any work you would like to do in commit()
    // before updateMetadataTrackingLogAndFailTheStreamIfNeeded as it may throw an exception.
    val offset: DeltaSourceOffset = DeltaSourceOffset.apply(tableId, end)
    if (!offset.isInitialSnapshot) {
      // Release snapshot resources after initial snapshot processing has completed.
      cleanUpSnapshotResources()
    }
    metadataEvolutionHandler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(offset)
  }

  override def stop(): Unit = {
    cleanUpSnapshotResources()
  }

  private def cleanUpSnapshotResources(): Unit = {
    if (initialSnapshot != null) {
      initialSnapshot.close(
          /* unpersistSnapshot= */ initialSnapshotVersion < snapshotAtSourceInit.version)
      initialSnapshot = null
    }
  }

  private def isExcludedPath(path: String): Boolean = {
    excludeRegex.exists(_.findFirstIn(path).isDefined)
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
  private[read] def getStartingVersion(): Optional[java.lang.Long] = synchronized {
    if (cachedStartingVersion == null) {
      cachedStartingVersion =
          ScalaUtils.toJavaOptional(
                  DeltaSource.getStartingVersionFromOptions(
                      spark, options, streamingBackend, snapshotAtSourceInit))
              .map(version => java.lang.Long.valueOf(version))
    }
    cachedStartingVersion
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
  private[read] def getFileChangesWithRateLimit(
      fromVersion: Long,
      fromIndex: Long,
      isInitialSnapshot: Boolean,
      limits: Optional[DeltaSource.AdmissionLimits]): ClosableIterator[IndexedFile] = {
    // TODO: Move this validation into DeltaStreamingBackend when DSv2 shares offset
    // discovery with DSv1.
    if (options.readChangeFeed) {
      return getFileChangesWithRateLimitForCDC(
          fromVersion, fromIndex, isInitialSnapshot, limits, endOffset = Optional.empty())
        .withClose(_.map { case (file, cdcFile) =>
          checkDeletionVectorsReadable(file, cdcFile)
          file
        })
    }

    var changes: ClosableIterator[IndexedFile] =
        getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, endOffset = Optional.empty())

    // Take each change until we've seen the configured number of addFiles. Some changes don't
    // represent file additions; we retain them for offset tracking, but they don't count toward
    // the maxFilesPerTrigger conf.
    if (limits.isPresent()) {
      val admissionLimits: DeltaSource.AdmissionLimits = limits.get()
      changes =
          new IteratorCloseOps(changes)
              .withClose(iterator => iterator.takeWhile(file => admissionLimits.admit(file)))
    }

    // Stop before any schema change barrier if detected.
    // Mirrors v1's stopIndexedFileIteratorAtSchemaChangeBarrier call in
    // getFileChangesWithRateLimit.
    metadataEvolutionHandler.stopIndexedFileIteratorAtSchemaChangeBarrier(changes)
      .withClose(_.map { file =>
        checkDeletionVectorsReadable(file, cdcFile = null)
        file
      })
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
  private[read] def getFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isInitialSnapshot: Boolean,
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[IndexedFile] = {

    var result: ClosableIterator[IndexedFile] = null

    if (isInitialSnapshot) {
      // Read snapshot files, then delta logs from fromVersion + 1.
      // Create each iterator on demand.
      // filterDeltaLogs handles the case when no commits exist after fromVersion.
      result =
          new IteratorFlatMapCloseOp(CollectionConverters.asScala(Arrays.asList(1, 2).iterator()))
              .flatMapWithClose(
                  part => {
                    if (part == 1) {
                      getSnapshotAt(fromVersion)
                    } else {
                      filterDeltaLogs(fromVersion + 1, endOffset)
                    }
                  })
    } else {
      result = filterDeltaLogs(fromVersion, endOffset)
    }

    applyBoundaryFiltering(result, fromVersion, fromIndex, endOffset)
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
   * @return An iterator of IndexedFile paired with its CDCDataFile, or null for sentinels
   */
  private[read] def getFileChangesWithRateLimitForCDC(
      fromVersion: Long,
      fromIndex: Long,
      isInitialSnapshot: Boolean,
      limits: Optional[DeltaSource.AdmissionLimits],
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[(IndexedFile, CDCDataFile)] = {
    validateCDFEnabledOnTable(fromVersion)
    var result: ClosableIterator[(IndexedFile, CDCDataFile)] = null
    if (isInitialSnapshot) {
      result =
          new IteratorFlatMapCloseOp(CollectionConverters.asScala(Arrays.asList(1, 2).iterator()))
              .flatMapWithClose(
                  part => {
                    if (part == 1) {
                      val files = getSnapshotAt(fromVersion)
                      val commitTimestamp = initialSnapshot.snapshot.timestamp
                      var snapshotFiles = files.withClose(_.map { file =>
                        val cdcFile = if (file.getAddFile() != null) {
                          CDCDataFile.fromAddFile(file.getAddFile(), commitTimestamp)
                        } else null
                        (file, cdcFile)
                      })
                      // Filter already-processed files BEFORE admission so they don't consume
                      // budget.
                      snapshotFiles =
                          new IteratorCloseOps(snapshotFiles)
                              .withClose(
                                  iterator =>
                                      iterator.filter(file =>
                                        isAfterStartBoundary(file._1, fromVersion, fromIndex)))
                      // Delta-log admission shares the same limits so the budget is continuous.
                      if (limits.isPresent()) {
                        snapshotFiles =
                            new IteratorCloseOps(snapshotFiles)
                                .withClose(
                                    iterator =>
                                        iterator.takeWhile(file => limits.get().admit(file._1)))
                      }
                      snapshotFiles
                    } else {
                      filterDeltaLogsWithRateLimitForCDC(
                          fromVersion + 1, fromVersion, fromIndex, limits, endOffset)
                    }
                  })
    } else {
      result =
          filterDeltaLogsWithRateLimitForCDC(
              fromVersion, fromVersion, fromIndex, limits, endOffset)
    }

    // Start boundary already applied above (before admission). Only apply end boundary here.
    result = applyEndBoundaryFiltering(result, endOffset, _._1)

    // Stop before any schema change barrier if detected. V1's wrap is in
    // DeltaSource.getFileChangesWithRateLimit, shared by CDC and non-CDC; V2 applies it inside
    // this CDC-specific method because both planInputPartitions and the outer
    // getFileChangesWithRateLimit reach the CDC iterator through here.
    metadataEvolutionHandler.stopIndexedFileIteratorAtSchemaChangeBarrier(
      result, (file: (IndexedFile, CDCDataFile)) => file._1)
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
  private def applyBoundaryFiltering(
      result: ClosableIterator[IndexedFile],
      fromVersion: Long,
      fromIndex: Long,
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[IndexedFile] = {
    // Check start boundary (exclusive)
    val filtered =
        new IteratorCloseOps(result)
            .withClose(
                iterator =>
                    iterator.filter(file => isAfterStartBoundary(file, fromVersion, fromIndex)))

    applyEndBoundaryFiltering(filtered, endOffset)
  }

  /**
   * Applies end boundary filtering only. Used by CDC paths where start boundary filtering is
   * applied before admission to avoid wasting budget on already-processed files.
   */
  private def applyEndBoundaryFiltering(
      result: ClosableIterator[IndexedFile],
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[IndexedFile] = {
    applyEndBoundaryFiltering(result, endOffset, identity[IndexedFile])
  }

  private def applyEndBoundaryFiltering[T](
      result: ClosableIterator[T],
      endOffset: Optional[DeltaSourceOffset],
      indexedFile: T => IndexedFile): ClosableIterator[T] = {
    // If endOffset is provided, we are getting a batch on a constructed range so we should use
    // the endOffset as the limit.
    // Otherwise, we are looking for a new offset, so we try to use the latestOffset we found for
    // Trigger.availableNow() as limit. We know endOffset <= lastOffsetForTriggerAvailableNow.
    val lastOffsetForThisScan: Option[DeltaSourceOffset] =
        ScalaUtils.toScalaOption(endOffset).orElse(lastOffsetForTriggerAvailableNow)

    // Check end boundary (inclusive)
    if (lastOffsetForThisScan.isDefined) {
      val bound: DeltaSourceOffset = lastOffsetForThisScan.get
      return result.withClose(_.takeWhile { entry =>
        val file = indexedFile(entry)
        file.getVersion() < bound.reservoirVersion ||
          (file.getVersion() == bound.reservoirVersion && file.getIndex() <= bound.index)
      })
    }

    result
  }

  // Visible for testing.
  private[read] def validateCDFEnabledOnTable(startVersion: Long): Unit = {
    // Make sure CDF is enabled at startVersion during stream start.
    if (cdfEnabledOnStreamStartValidated) {
      return
    }
    var startSnapshot: Snapshot = null
    if (startVersion == snapshotAtSourceInit.version) {
      startSnapshot = snapshotAtSourceInit
    } else {
      try {
        startSnapshot = streamingBackend.getSnapshotAt(startVersion)
      } catch { case e: io.delta.kernel.exceptions.KernelException =>
        // startVersion may not yet exist (e.g. startingVersion=latest resolves to latest+1).
        // TODO(#6745): narrow this catch once kernel exposes a specific exception subclass
        // for "version not yet materialized".
        return
      }
    }
    if (!CDCReader.isCDCEnabledOnTable(startSnapshot.metadata, spark)) {
      throw new RuntimeException(
          DeltaErrors.changeDataNotRecordedException(startVersion, startVersion, startVersion))
    }
    cdfEnabledOnStreamStartValidated = true
  }

  private def filterDeltaLogs(
      startVersion: Long,
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[IndexedFile] = {
    // Schema tracking needs every metadata-change commit; failOnDataLoss=false may skip one.
    require(
      options.failOnDataLoss || !metadataEvolutionHandler.shouldTrackMetadataChange(),
      "Using schema from schema tracking log cannot tolerate missing commit files.")
    val commits: ClosableIterator[DeltaSingleCommit] =
        streamingBackend.getSingleCommitIterator(
            startVersion,
            options.failOnDataLoss,
            ScalaUtils.toScalaOption(endOffset))
    val files: ClosableIterator[IndexedFile] =
        new IteratorFlatMapCloseOp(commits)
            .flatMapWithClose(
                commit => processCommitToIndexedFiles(commit, startVersion, endOffset))
    wrapIteratorWithCommitIteratorClose(files, commits)
  }

  private def filterDeltaLogsWithRateLimitForCDC(
      startVersion: Long,
      lastProcessedVersion: Long,
      lastProcessedIndex: Long,
      limits: Optional[DeltaSource.AdmissionLimits],
      endOffset: Optional[DeltaSourceOffset]): ClosableIterator[(IndexedFile, CDCDataFile)] = {
    // Schema tracking needs every metadata-change commit; failOnDataLoss=false may skip one.
    require(
      options.failOnDataLoss || !metadataEvolutionHandler.shouldTrackMetadataChange(),
      "Using schema from schema tracking log cannot tolerate missing commit files.")
    var commits: ClosableIterator[DeltaSingleCommit] =
        streamingBackend.getSingleCommitIterator(
            startVersion,
            options.failOnDataLoss,
            ScalaUtils.toScalaOption(endOffset))

    // Stop processing further commits once the budget is spent.
    if (limits.isPresent()) {
      commits =
          new IteratorCloseOps(commits)
              .withClose(
                  iterator =>
                      iterator.takeWhile(_ => limits.get().hasCapacity))
    }

    val files: ClosableIterator[(IndexedFile, CDCDataFile)] =
        new IteratorFlatMapCloseOp(commits)
            .flatMapWithClose(
                commit =>
                    processCommitToIndexedFilesForCDC(
                        commit,
                        startVersion,
                        limits,
                        endOffset,
                        lastProcessedVersion,
                        lastProcessedIndex))
    wrapIteratorWithCommitIteratorClose(files, commits)
  }

  /**
   * Processes a single commit and returns an iterator of IndexedFiles wrapped with BEGIN/END
   * sentinels.
   */
  private def processCommitToIndexedFiles(
      commit: DeltaSingleCommit,
      startVersion: Long,
      endOffsetOpt: Optional[DeltaSourceOffset]): ClosableIterator[IndexedFile] = {
    try {
      val version: Long = commit.version

      // First pass: Validate the commit and decide whether to skip it.
      //
      // We must validate the ENTIRE commit before emitting ANY files. This is a correctness
      // requirement: commits could contain both AddFiles and RemoveFiles.
      // If we emitted AddFiles before discovering a RemoveFile (dataChange=true) later in the
      // commit, downstream would produce incorrect results.
      //
      // When schema tracking is enabled, skip per-commit metadata verification - the handler
      // manages schema changes through the barrier protocol instead.
      val trackingMetadataChange: Boolean = metadataEvolutionHandler.shouldTrackMetadataChange()
      val validationResult: CommitValidationResult =
          validateCommitAndDecideSkipping(
              commit,
              version,
              startVersion,
              snapshotAtSourceInit.dataPath.toString,
              endOffsetOpt,
              /* verifyMetadataAction= */ !trackingMetadataChange)

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
      // If metadata tracking is active, inject schema change barrier sentinel before file
      // actions. This mirrors v1's getMetadataOrProtocolChangeIndexedFileIterator concatenation
      // in filterAndGetIndexedFiles.
      val metadataBarrier: ClosableIterator[IndexedFile] =
          metadataEvolutionHandler.getMetadataOrProtocolChangeIndexedFileIterator(
              validationResult.metadataAction, validationResult.protocolAction, version)

      val fileActions: ClosableIterator[IndexedFile] =
          if (validationResult.shouldSkipCommit) {
              new ClosableWrapper[IndexedFile](Iterator.empty).toClosable
          } else getFilesFromCommit(commit, version)

      val inner: ClosableIterator[IndexedFile] =
          new IteratorCloseOps(fileActions)
              .withClose(
                  files =>
                      Iterator.single(IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX))
                          .++(metadataBarrier)
                          .++(files)
                          .++(
                              Iterator.single(
                                  IndexedFile.sentinel(
                                      version, DeltaSourceOffset.END_INDEX))))

      inner
    } catch { case e: Exception =>
      throw (if (e.isInstanceOf[RuntimeException]) e else new RuntimeException(e))
    }
  }

  private def getFilesFromCommit(
      commit: DeltaSingleCommit, version: Long): ClosableIterator[IndexedFile] = {
    val actions: ClosableIterator[Action] =
        commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L)
    // Assign each IndexedFile a unique index within the commit. We use a mutable array
    // because variables captured by a lambda must be effectively final (never reassigned).
    val fileIndex: Array[Long] = Array(0L)

    // Only include AddFiles with dataChange=true. Skip changes that optimize or reorganize
    // data without changing the logical content.
    new IteratorCloseOps(actions)
        .withClose(
            actionIterator =>
                actionIterator
                    .filter(
                        action =>
                            action.isInstanceOf[AddFile] && action.asInstanceOf[AddFile].dataChange)
                    .map(action => {
                      val index: Long = fileIndex(0)
                      fileIndex(0) += 1
                      IndexedFile.addFile(version, index, action.asInstanceOf[AddFile])
                    }))
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
  private def validateCommitAndDecideSkipping(
      commit: DeltaSingleCommit,
      version: Long,
      batchStartVersion: Long,
      tablePath: String,
      endOffsetOpt: Optional[DeltaSourceOffset],
      verifyMetadataAction: Boolean): CommitValidationResult = {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isPresent()) {
      val endOffset: DeltaSourceOffset = endOffsetOpt.get()
      if (endOffset.reservoirVersion == version
          && endOffset.index == DeltaSourceOffset.BASE_INDEX) {
        return new CommitValidationResult(false, null, null)
      }
    }

    // "Changes" are commits that contain both AddFile and RemoveFile actions (e.g. UPDATE,
    // MERGE that rewrites data files). Allowing changes means these commits won't cause failures.
    val shouldAllowChanges: Boolean =
        options.ignoreChanges || ignoreFileDeletion || skipChangeCommits
    // "Deletes" are commits that contain only RemoveFile actions without any AddFile actions
    // (e.g. DELETE that purely removes data). This is a subset of changes, so allowing changes
    // implies allowing deletes.
    val shouldAllowDeletes: Boolean = shouldAllowChanges || options.ignoreDeletes

    var hasFileAdd: Boolean = false
    var shouldSkipCommit: Boolean = false
    var metadataAction: Metadata = null
    var protocolAction: Protocol = null
    var removeFileActionPath: String = null
    var operation: String = null

    try Using.resource(
        commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L)) { actionsIter =>
      while (actionsIter.hasNext) {
        val action: Action = actionsIter.next()
        if (action.isInstanceOf[AddFile] && action.asInstanceOf[AddFile].dataChange) {
          hasFileAdd = true
        } else if (action.isInstanceOf[RemoveFile] && action.asInstanceOf[RemoveFile].dataChange) {
          // skip change commits include delete-only commits
          shouldSkipCommit = skipChangeCommits
          if (removeFileActionPath == null) {
            removeFileActionPath = action.asInstanceOf[RemoveFile].path
          }
        } else if (action.isInstanceOf[Metadata]) {
          metadataAction =
              validateMetadata(
                  action.asInstanceOf[Metadata],
                  metadataAction,
                  version,
                  batchStartVersion,
                  endOffsetOpt,
                  verifyMetadataAction)
        } else if (action.isInstanceOf[Protocol]) {
          Protocol.protocolRead(
              spark, action.asInstanceOf[Protocol], tablePath, readSnapshotDescriptor)
          assert(
            protocolAction == null,
            s"Should not encounter two protocol actions in the same commit of version $version")
          protocolAction = action.asInstanceOf[Protocol]
        } else if (action.isInstanceOf[CommitInfo]) {
          val commitInfo: CommitInfo = action.asInstanceOf[CommitInfo]
          operation =
              if (commitInfo.operation == null) {
                null
              } else String.format(
                  "%s (%s)",
                  commitInfo.operation,
                  CollectionConverters.asJava(commitInfo.operationParameters))
        }
      }
    } catch { case e: IOException =>
      throw new RuntimeException("Failed to process commit at version " + version, e)
    }

    if (removeFileActionPath != null) {
      if (hasFileAdd && !shouldAllowChanges) {
        // Commit contains data changes (adds + removes) and changes are disallowed.
        val changeInfo: String = if (operation != null) operation else removeFileActionPath
        throw DeltaErrors.deltaSourceIgnoreChangesError(version, changeInfo, tablePath)
      } else if (!hasFileAdd && !shouldAllowDeletes) {
        // Commit contains only removes (deletes) and deletes are disallowed.
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFileActionPath, tablePath)
      }
    }

    new CommitValidationResult(shouldSkipCommit, metadataAction, protocolAction)
  }

  /**
   * Validate and lazily initialize the metadata tracking log for planned batches during stream
   * (re)start. Mirrors v1's {@code validateAndInitMetadataLogForPlannedBatchesDuringStreamStart}.
   *
   * <p>Called from {@code planInputPartitions} (the v2 equivalent of v1's {@code getBatch}).
   */
  private def validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(
      startVersion: Long, endOffset: DeltaSourceOffset): Unit = {
    val endVersionForMetadataLogInit: Long =
        if (endOffset.index == DeltaSourceOffset.BASE_INDEX) {
            endOffset.reservoirVersion - 1
        } else endOffset.reservoirVersion

    // For eager initialization, we initialize the log right now.
    if (metadataEvolutionHandler.shouldInitializeMetadataTrackingEagerly()) {
      metadataEvolutionHandler.initializeMetadataTrackingAndExitStream(
          startVersion,
          endVersionForMetadataLogInit,
          /* alwaysFailUponLogInitialized= */ false)
    }

    // Check for column mapping + streaming incompatible schema changes
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(
        startVersion, Some(endVersionForMetadataLogInit))
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
  private def checkReadIncompatibleSchemaChangeOnStreamStartOnce(
      batchStartVersion: Long, batchEndVersion: Option[Long]): Unit = {
    // When schema tracking is enabled, the MetadataEvolutionHandler manages schema changes
    // through the barrier protocol - skip the traditional compatibility check.
    if (metadataEvolutionHandler.shouldTrackMetadataChange()) return

    if (hasCheckedReadIncompatibleSchemaChangesOnStreamStart) return

    var startVersionSnapshot: Snapshot = null
    var err: Exception = null
    try {
      startVersionSnapshot = streamingBackend.getSnapshotAt(batchStartVersion)
    } catch { case e: Exception =>
      err = e
    }

    // Cannot perfectly verify column mapping schema changes if we cannot compute a start snapshot.
    if (!schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges
        && schemaReadOptions.isStreamingFromColumnMappingTable
        && (err != null)) {
      throw DeltaErrors.failedToGetSnapshotDuringColumnMappingStreamingReadCheck(err)
          .asInstanceOf[RuntimeException]
    }

    // Perform schema check if we need to, considering all escape flags.
    if (!schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges
        || schemaReadOptions.typeWideningEnabled
        || !schemaReadOptions
            .forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {
      if (startVersionSnapshot != null) {
        checkReadIncompatibleSchemaChanges(
            startVersionSnapshot.metadata,
            startVersionSnapshot.version,
            batchStartVersion,
            batchEndVersion,
            /* validatedDuringStreamStart= */ true)
        // If end version is defined (i.e. we have a pending batch), let's also eagerly check all
        // intermediate schema changes against the stream read schema to capture corners cases such
        // as rename and rename back.
        if (batchEndVersion.isDefined) {
          for (entry <-
              CollectionConverters.asScala(StreamingHelper.collectMetadataActions(
                      batchStartVersion,
                      Optional.of(Long.box(batchEndVersion.get)),
                      tableManager,
                      originalStreamingQueryContext,
                      options.failOnDataLoss,
                      spark,
                      readSnapshotDescriptor)
                  .entrySet())) {
            val version: Long = entry.getKey()
            val metadata: Metadata = entry.getValue()
            checkReadIncompatibleSchemaChanges(
                metadata,
                version,
                batchStartVersion,
                batchEndVersion,
                /* validatedDuringStreamStart= */ true)
          }
        }
      }
    }

    // Mark as checked
    hasCheckedReadIncompatibleSchemaChangesOnStreamStart = true
  }

  /**
   * This method computes the initial snapshot to read when Delta Source was initialized on a fresh
   * stream.
   *
   * @return an iterator of IndexedFiles for the initial snapshot
   */
  private def getSnapshotAt(version: Long): ClosableIterator[IndexedFile] = {
    if (initialSnapshot == null || version != initialSnapshotVersion) {
      cleanUpSnapshotResources()
      val snapshot: Snapshot = streamingBackend.getSnapshotAt(version)

      initialSnapshot = new DeltaSourceSnapshot(spark, snapshot, Seq.empty)
      initialSnapshotVersion = version

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
          && initialSnapshot.snapshot.version >= readSnapshotDescriptor.version) {
        metadataEvolutionHandler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
            snapshot.metadata,
            snapshot.protocol,
            initialSnapshot.snapshot.version,
            // The new schema should replace the previous initialized schema for initial snapshot
            /* replace= */ true)
      }
    }

    // TODO: Add range-aware iteration to DeltaSourceSnapshot so V1 and V2 can filter the
    // already-processed prefix before decoding files.
    val snapshotFiles: ClosableIterator[IndexedFile] =
        new ClosableWrapper(
                initialSnapshot
                    .iterator()
                    .map(file => IndexedFile.addFile(file.version, file.index, file.add)))
            .toClosable
    addBeginAndEndIndexOffsetsForVersion(version, snapshotFiles)
  }

  /**
   * Process a CDC commit into IndexedFiles. Validates metadata, collects CDC actions, and builds
   * IndexedFiles.
   *
   * <p>Package-private for testing.
   */
  private[read] def processCommitToIndexedFilesForCDC(
      commit: DeltaSingleCommit,
      startVersion: Long,
      limits: Optional[DeltaSource.AdmissionLimits],
      endOffsetOpt: Optional[DeltaSourceOffset],
      lastProcessedVersion: Long,
      lastProcessedIndex: Long): ClosableIterator[(IndexedFile, CDCDataFile)] = {
    try {
      val version: Long = commit.version
      val timestamp: Long = commit.timestamp

      if (isEndOffsetAtBaseIndex(endOffsetOpt, version)) {
        return new ClosableWrapper(
                CollectionConverters.asScala(
                        Arrays.asList[(IndexedFile, CDCDataFile)](
                            (IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX), null),
                            (IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX), null)))
                    .iterator)
            .toClosable
      }

      var result: List[(IndexedFile, CDCDataFile)] =
          collectAndBuildCDCIndexedFiles(commit, version, timestamp, startVersion, endOffsetOpt)

      // Pre-filter already-processed files from the resume commit BEFORE admission so they
      // don't consume budget.
      if (version == lastProcessedVersion) {
        result.removeIf(f => !isAfterStartBoundary(f._1, lastProcessedVersion, lastProcessedIndex))
      }

      if (limits.isPresent()) {
        result = applyPerCommitCDCAdmission(result, limits.get(), version)
        if (result.isEmpty()) {
          return new ClosableWrapper[(IndexedFile, CDCDataFile)](Iterator.empty).toClosable
        }
      } else {
        result.add((IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX), null))
      }
      new ClosableWrapper(CollectionConverters.asScala(result).iterator).toClosable
    } catch { case e: IOException =>
      throw new RuntimeException(s"Failed to process CDC commit at version ${commit.version}", e)
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
  private def collectAndBuildCDCIndexedFiles(
      commit: DeltaSingleCommit,
      version: Long,
      timestamp: Long,
      startVersion: Long,
      endOffsetOpt: Optional[DeltaSourceOffset]): List[(IndexedFile, CDCDataFile)] = {
    val trackingMetadataChange: Boolean = metadataEvolutionHandler.shouldTrackMetadataChange()

    // Phase 1: scan actions to validate metadata and collect CDC/Add/Remove files.
    val explicitCdcFiles: List[CDCDataFile] = new ArrayList[CDCDataFile]()
    // Single ordered list for inferred CDC files, preserving delta-log action ordering.
    // DSv1 assigns IndexedFile.index via zipWithIndex on the original action sequence.
    val inferredCdcFiles: List[CDCDataFile] = new ArrayList[CDCDataFile]()
    var shouldSkip: Boolean = false
    var metadataAction: Metadata = null
    var protocolAction: Protocol = null

    Using.resource(commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L)) { actionsIter =>
      while (actionsIter.hasNext) {
        actionsIter.next() match {
          case metadata: Metadata =>
            // Metadata: check schema changes + CDF still enabled. Skip the read-compat check when
            // tracking - the barrier protocol below handles divergence.
            metadataAction =
                validateMetadata(
                    metadata,
                    metadataAction,
                    version,
                    startVersion,
                    endOffsetOpt,
                    /* verifyMetadataAction= */ !trackingMetadataChange)
            if (!CDCReader.isCDCEnabledOnTable(metadataAction, spark)) {
              throw new RuntimeException(
                  DeltaErrors.changeDataNotRecordedException(
                      version,
                      startVersion,
                      endOffsetOpt.map[java.lang.Long](_.reservoirVersion).orElse(version)))
            }
          case protocol: Protocol =>
            // TODO: Validate protocol read compatibility before processing CDC files.
            // Capture protocol-only changes so they also trigger a schema barrier.
            assert(
              protocolAction == null,
              s"Should not encounter two protocol actions in the same commit of version $version")
            protocolAction = protocol
          case commitInfo: CommitInfo =>
            // No-op MERGE commits rewrite files without changing logical data.
            shouldSkip = CDCReader.shouldSkipFileActionsInCommit(commitInfo)
          case cdcFile: AddCDCFile =>
            explicitCdcFiles.add(CDCDataFile.fromAddCDCFile(cdcFile, timestamp))
          case remove: RemoveFile if remove.dataChange =>
            // scalastyle:off removeFile // Wraps a RemoveFile; does not create one.
            inferredCdcFiles.add(CDCDataFile.fromRemoveFile(remove, timestamp))
            // scalastyle:on removeFile
          case add: AddFile if add.dataChange =>
            inferredCdcFiles.add(CDCDataFile.fromAddFile(add, timestamp))
          case _ =>
        }
      }
    }

    // Schema-change barrier: emit only the barrier when this commit diverges from source-init.
    // V1 injects in DeltaSourceCDCSupport.filterAndIndexDeltaLogs and short-circuits in
    // IndexedChangeFileSeq.filterFiles; V2 does both here.
    val barrierResult = Using.resource(
        metadataEvolutionHandler.getMetadataOrProtocolChangeIndexedFileIterator(
            metadataAction, protocolAction, version)) { barrierIter =>
      if (barrierIter.hasNext) {
        val result: List[(IndexedFile, CDCDataFile)] = new ArrayList[(IndexedFile, CDCDataFile)]()
        result.add((barrierIter.next(), null))
        Some(result)
      } else None
    }
    if (barrierResult.isDefined) {
      return barrierResult.get
    }

    // No-op MERGE: discard inferred actions (DSv1's isValidIndexedFile / shouldSkip).
    // shouldSkip doesn't apply to explicit CDC files - only inferred Add/Remove.
    if (shouldSkip) {
      inferredCdcFiles.clear()
    }

    // Phase 2: build IndexedFiles from collected actions.
    val result: List[(IndexedFile, CDCDataFile)] = new ArrayList[(IndexedFile, CDCDataFile)]()
    result.add((IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX), null))
    var fileIndex: Long = 0

    // Explicit CDC files and inferred files are mutually exclusive: if a commit contains any
    // AddCDCFile actions, only those are emitted and AddFile/RemoveFile are ignored.
    if (!explicitCdcFiles.isEmpty()) {
      for (cdcFile <- CollectionConverters.asScala(explicitCdcFiles)) {
        result.add(indexCDCFile(version, fileIndex, cdcFile))
        fileIndex += 1
      }
    } else {
      // Bucket AddFile and RemoveFile by path to detect same-path pairs.
      // DV-based deletes reuse the same parquet file (marking rows in the DV instead of rewriting),
      // so both the Remove and Add reference the same path.
      val addByPath: Map[String, AddFile] = new HashMap[String, AddFile]()
      val removeByPath: Map[String, RemoveFile] = new HashMap[String, RemoveFile]()
      for (cdc <- CollectionConverters.asScala(inferredCdcFiles)) {
        if (cdc.getAddFile() != null) {
          val prev: AddFile =
              addByPath.put(cdc.getAddFile().path, cdc.getAddFile())
          require(
            prev == null,
            s"Unexpected duplicate AddFile at path ${cdc.getAddFile().path} " +
              "in a single commit")
        // scalastyle:off removeFile // Reads RemoveFiles; does not create any.
        } else if (cdc.getRemoveFile() != null) {
          val prev: RemoveFile =
              removeByPath.put(cdc.getRemoveFile().path, cdc.getRemoveFile())
          require(
            prev == null,
            s"Unexpected duplicate RemoveFile at path ${cdc.getRemoveFile().path} " +
              "in a single commit")
        }
        // scalastyle:on removeFile
      }

      val samePathKeys: Set[String] = new HashSet[String](addByPath.keySet())
      samePathKeys.retainAll(removeByPath.keySet())

      // Walk inferredCdcFiles in delta-log order. Non-pair files are emitted as-is. Same-path
      // pairs are emitted at the FIRST occurrence of the path (the counterpart, encountered
      // later in the loop, is skipped).
      val emittedPairs: Set[String] = new HashSet[String]()
      for (cdc <- CollectionConverters.asScala(inferredCdcFiles)) {
        val path: String = cdc.getPath()
        if (!samePathKeys.contains(path)) {
          // File with unique paths: emit as-is.
          result.add(indexCDCFile(version, fileIndex, cdc))
          fileIndex += 1
        } else if (emittedPairs.add(path)) {
          // First occurrence of a same-path pair: emit the consolidated pair entries.
          val add: AddFile = addByPath.get(path)
          val remove: RemoveFile = removeByPath.get(path)
          if (add.deletionVector != null || remove.deletionVector != null) {
            // DV-based delete/restore: compute the bitmap diff and emit one CDC file per diff
            // entry. Path.toString() avoids percent-encoding that breaks DV file resolution.
            val dataPath: String = snapshotAtSourceInit.dataPath.toString()
            for (f <- CollectionConverters.asScala(
                CDCDeletionVectorHelper.generateDVDiffCDCFiles(
                    add, remove, timestamp, dataPath, hadoopConf))) {
              result.add(indexCDCFile(version, fileIndex, f))
              fileIndex += 1
            }
          } else {
            // Same-path Add+Remove without DV: emit plain insert + delete.
            result.add(
                indexCDCFile(version, fileIndex, CDCDataFile.fromAddFile(add, timestamp)))
            fileIndex += 1
            // scalastyle:off removeFile // Wraps a RemoveFile; does not create one.
            result.add(
                indexCDCFile(
                    version, fileIndex, CDCDataFile.fromRemoveFile(remove, timestamp)))
            // scalastyle:on removeFile
            fileIndex += 1
          }
        }
        // Else: second occurrence of an already-emitted same-path pair; skip.
      }
    }

    result
  }

  /**
   * Per-commit CDC admission control. Mirrors DSv1's IndexedChangeFileSeq.filterFiles.
   *
   * <p>Input is a single commit's IndexedFiles: either a schema-change barrier (passed through
   * unchanged), or an optional BASE sentinel followed by data files. Applies batch or per-file
   * admission and appends an END sentinel only when all data files are admitted. Returns empty list
   * on batch-reject so the offset doesn't advance.
   */
  // Visible for testing.
  private[read] def applyPerCommitCDCAdmission(
      commitFiles: List[(IndexedFile, CDCDataFile)],
      admissionLimits: DeltaSource.AdmissionLimits,
      commitVersion: Long): List[(IndexedFile, CDCDataFile)] = {
    // Schema-change barrier: pass through. collectAndBuildCDCIndexedFiles returns the barrier as a
    // singleton, so it can only appear as element 0.
    if (!commitFiles.isEmpty()
        && commitFiles.get(0)._1.getIndex() == DeltaSourceOffset.METADATA_CHANGE_INDEX) {
      return new ArrayList[(IndexedFile, CDCDataFile)](commitFiles)
    }

    // Split out the optional BASE sentinel from data files.
    var baseSentinel: Option[(IndexedFile, CDCDataFile)] = None
    val dataFiles: List[(IndexedFile, CDCDataFile)] = new ArrayList[(IndexedFile, CDCDataFile)]()
    for (file <- CollectionConverters.asScala(commitFiles)) {
      if (file._1.hasFileAction()) {
        dataFiles.add(file)
      } else {
        if (baseSentinel.nonEmpty) {
          throw new IllegalStateException(
            "Expected at most one BASE sentinel per commit, " +
              s"got multiple for version $commitVersion")
        }
        baseSentinel = Some(file)
      }
    }

    // Batch-admit all or nothing when we can't split the commit:
    //   - Explicit CDC: can't separate update_preimage/postimage
    //   - Inferred CDC with DVs: can't split add/remove pairs
    val requiresBatchAdmission: Boolean =
        dataFiles.stream().anyMatch(f =>
          f._2 != null && (f._2.isAddCDCFile() || f._2.hasDeletionVector()))

    var admittedData: List[(IndexedFile, CDCDataFile)] = null
    if (requiresBatchAdmission) {
      if (admissionLimits.admit(CollectionConverters.asScala(dataFiles).map(_._1).toList)) {
        admittedData = dataFiles
      } else {
        return Collections.emptyList()
      }
    } else {
      admittedData = new ArrayList[(IndexedFile, CDCDataFile)]()
      val dataFilesIter: java.util.Iterator[(IndexedFile, CDCDataFile)] = dataFiles.iterator()
      var admitting: Boolean = true
      while (admitting && dataFilesIter.hasNext()) {
        val file: (IndexedFile, CDCDataFile) = dataFilesIter.next()
        if (admissionLimits.admit(file._1)) {
          admittedData.add(file)
        } else {
          admitting = false
        }
      }
    }

    val result: List[(IndexedFile, CDCDataFile)] = new ArrayList[(IndexedFile, CDCDataFile)]()
    baseSentinel.foreach(file => result.add(file))
    result.addAll(admittedData)
    // Append END only if the entire commit was admitted. Partial admission leaves the offset
    // on the last admitted file so the next batch resumes from there.
    if (admittedData.size() >= dataFiles.size()) {
      result.add((IndexedFile.sentinel(commitVersion, DeltaSourceOffset.END_INDEX), null))
    }
    result
  }

  /**
   * Validates a metadata action: checks for duplicates and read-incompatible schema changes.
   *
   * @return the validated metadata (for assignment back to the caller's tracking variable)
   */
  private def validateMetadata(
      metadata: Metadata,
      existing: Metadata,
      version: Long,
      startVersion: Long,
      endOffsetOpt: Optional[DeltaSourceOffset],
      verifyMetadataAction: Boolean): Metadata = {
    assert(
      existing == null,
      s"Should not encounter two metadata actions in the same commit of version $version")
    // When schema tracking is enabled, verifyMetadataAction is false - the handler manages
    // schema changes through the barrier protocol instead.
    if (verifyMetadataAction) {
      val batchEndVersion: Option[Long] =
          ScalaUtils.toScalaOption(endOffsetOpt).map(_.reservoirVersion)
      checkReadIncompatibleSchemaChanges(
          metadata,
          version,
          startVersion,
          batchEndVersion,
          /* validatedDuringStreamStart= */ false)
    }
    metadata
  }
}
