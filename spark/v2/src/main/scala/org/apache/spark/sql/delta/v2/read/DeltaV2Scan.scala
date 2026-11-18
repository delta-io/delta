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

import java.time.ZoneId
import java.util.{Collections, Locale, Map => JMap, Objects, Optional, OptionalLong, TreeSet}

import scala.collection.immutable.{Map, Seq}
import scala.collection.mutable
import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.{DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog
import org.apache.spark.sql.delta.stats.{DataSize, DeltaScan}
import org.apache.spark.sql.delta.v2.DeltaV2Logging
import org.apache.spark.sql.delta.v2.interop.{DeltaV2QueryContext, DeltaV2Snapshot, DeltaV2SnapshotManager}
import org.apache.spark.sql.delta.v2.kernel.KernelEngineFactory
import org.apache.spark.sql.delta.v2.read.cdc.CDCSchemaContext
import org.apache.spark.sql.delta.v2.read.deletionvector.DeletionVectorSchemaContext
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager
import org.apache.spark.sql.delta.v2.utils.{PartitionUtils, PartitionValueCaster, ScalaUtils}
import org.apache.spark.sql.delta.v2.utils.ExpressionUtils.dsv2PredicateToCatalystExpression
import org.apache.spark.sql.delta.v2.utils.SchemaUtils
import org.apache.hadoop.conf.Configuration
import io.delta.kernel.expressions.{Predicate => KernelPredicate}
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshotImpl}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  InterpretedPredicate,
  Predicate => CatalystPredicate
}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{
  Batch,
  Scan,
  Statistics,
  SupportsReportStatistics,
  SupportsRuntimeV2Filtering
}
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.DeltaV2FilterTranslator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Package-private scan implementation for Delta's Spark DataSource V2 read path.
 *
 * This class must remain package-private so callers outside `v2.read` depend only on Spark's
 * public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
// TODO: Remove snapshotManager and use tableManager as the single source of truth.
private[read] class DeltaV2Scan(
    private val snapshotManager: DeltaV2SnapshotManager,
    private val tableManager: Option[DeltaV2TableManager],
    private val initialSnapshot: Snapshot,
    tableSchema: StructType,
    private val dataSchema: StructType,
    private val partitionSchema: StructType,
    private val readDataSchema: StructType,
    scanSupplier: () => DeltaScan,
    dataFiltersArg: Array[Expression],
    partitionFiltersArg: Array[Expression],
    private val catalogStats: Option[Statistics],
    private val options: CaseInsensitiveStringMap,
    // Empty means no limit pushdown from Spark.
    private val pushedLimit: Option[Int],
    private val originalQueryContext: DeltaV2QueryContext)
    extends DeltaV2Logging
    with Scan
    with SupportsReportStatistics
    with DeltaV2ScanStatistics
    with SupportsRuntimeV2Filtering
    {
  import DeltaV2Scan._

  Objects.requireNonNull(snapshotManager, "snapshotManager is null")
  Objects.requireNonNull(originalQueryContext, "originalQueryContext is null")
  Objects.requireNonNull(tableManager, "tableManager is null")
  Objects.requireNonNull(initialSnapshot, "initialSnapshot is null")
  Objects.requireNonNull(dataSchema, "dataSchema is null")
  Objects.requireNonNull(partitionSchema, "partitionSchema is null")
  Objects.requireNonNull(readDataSchema, "readDataSchema is null")
  // Produces the V1 DeltaScan via DeltaV2Snapshot.filesForScan. Kept lazy because streaming scans
  // never consume batch-selected files and must not run the batch data-skipping path during
  // ScanBuilder.build().
  private val deltaScanSupplier =
    Objects.requireNonNull(scanSupplier, "deltaScanSupplier is null")
  // Pushed data filters (min/max skipping), as Catalyst expressions, for explain and scan equality.
  private val dataFilters: Array[Expression] =
    if (dataFiltersArg == null) new Array[Expression](0) else dataFiltersArg.clone()
  // Pushed partition filters (row-exact), as Catalyst expressions, for explain and scan equality.
  private val partitionFilters: Array[Expression] =
    if (partitionFiltersArg == null) new Array[Expression](0) else partitionFiltersArg.clone()
  private val partitionFiltersSet: mutable.HashSet[Expression] = canonicalizedSet(partitionFilters)
  // Derived Sets used only for equals/hashCode: filters are AND-ed at evaluation time,
  // so list order has no semantic meaning and two scans with the same filter set in
  // different orders should compare equal.
  // Canonicalize so equals/hashCode ignore exprId/ordering: two scans of the same table with the
  // same logical filters compare equal even when the filter expressions carry different exprIds.
  private val dataFiltersSet: mutable.HashSet[Expression] = canonicalizedSet(dataFilters)
  Objects.requireNonNull(catalogStats, "catalogStats is null")
  Objects.requireNonNull(options, "options is null")
  Objects.requireNonNull(pushedLimit, "pushedLimit is null")
  private val scalaOptions: Map[String, String] = ScalaUtils.toScalaMap(options)
  private val hadoopConf: Configuration =
    SparkSession.active.sessionState.newHadoopConfWithOptions(scalaOptions)
  private val sqlConf: SQLConf = SQLConf.get
  private val deltaOptions: DeltaOptions = new DeltaOptions(scalaOptions, sqlConf)
  private[this] val cdcReadEnabled: Boolean = deltaOptions.readChangeFeed
  private val zoneId: ZoneId = DateTimeUtils.getZoneId(sqlConf.sessionLocalTimeZone)
  private val ddlOrderedReadOutputSchema: StructType = {
    val ddlOrdered = SchemaUtils.ddlOrderedOutputSchema(
      tableSchema, readDataSchema, partitionSchema)
    if (cdcReadEnabled) CDCSchemaContext.appendCDCColumns(ddlOrdered) else ddlOrdered
  }

  // Initialized lazily and shared with batches. Runtime pruning, when supported, replaces this
  // reference without modifying an existing selection.
  private var currentDeltaScan: DeltaScan = null
  @volatile private var planned: Boolean = false

  // Runtime predicates applied after planning (using Set for order-independent comparison)
  private val appliedRuntimePredicates = new mutable.HashSet[V2Predicate]

  /**
   * Returns the Kernel snapshot wrapped by [[initialSnapshot]] for operations that have not
   * migrated to the shared snapshot facade yet.
   */
  private def kernelSnapshot(): KernelSnapshotImpl =
    DeltaV2Snapshot.getKernelSnapshot(initialSnapshot)

  /** Read schema for the scan, in the table's DDL column order. */
  override def readSchema(): StructType = ddlOrderedReadOutputSchema

  /**
   * Override columnarSupportMode to explicitly declare whether this scan supports columnar
   * (vectorized) reading. Without this override, the default `PARTITION_DEFINED` mode causes
   * Spark to eagerly call `planInputPartitions()` during query planning to check
   * per-partition columnar support, triggering unnecessary early file enumeration.
   *
   * Since columnar support is uniform across all partitions (determined by schema compatibility
   * and table features, not by individual files), we can declare it at the scan level to avoid this
   * overhead.
   *
   * This must stay consistent with the vectorized reader decision in
   * [[PartitionUtils.createDeltaParquetReaderFactory]]. In particular, deletion-vector-enabled
   * tables augment the read schema with internal columns (e.g., `__delta_internal_is_row_deleted`),
   * which changes the schema passed to the vectorized reader check. We replicate that logic here to
   * ensure the scan-level declaration matches the per-partition reader behavior.
   */
  override def columnarSupportMode(): Scan.ColumnarSupportMode = {
    val metadataColumnRequested =
      readDataSchema.fields.exists(field => FileFormat.METADATA_NAME.equals(field.name))
    if (metadataColumnRequested) {
      return Scan.ColumnarSupportMode.UNSUPPORTED
    }
    // Mirror the schema augmentation chain in PartitionUtils.createDeltaParquetReaderFactory
    // (CDC then DV, in that order) so the batch-read check sees the same final schema the
    // parquet reader will. If you reorder or add augmentations there, update this in lockstep.
    var schemaForBatchCheck = readDataSchema
    if (cdcReadEnabled) {
      schemaForBatchCheck = CDCSchemaContext.appendCDCColumns(schemaForBatchCheck)
    }
    if (PartitionUtils.tableSupportsDeletionVectors(kernelSnapshot())) {
      schemaForBatchCheck =
        new DeletionVectorSchemaContext(schemaForBatchCheck, partitionSchema).getSchemaWithDvColumn
    }
    if (ParquetUtils.isBatchReadSupportedForSchema(sqlConf, schemaForBatchCheck)) {
      Scan.ColumnarSupportMode.SUPPORTED
    } else {
      Scan.ColumnarSupportMode.UNSUPPORTED
    }
  }

  override def toBatch(): Batch =
    recordFrameProfileValue("batchScan.toBatch", () => createBatch())

  /** Constructs the fallback batch from the lazily selected scan. */
  private def createBatch(): Batch = {
    if (cdcReadEnabled) {
      throw new UnsupportedOperationException(
        "Batch reads with CDC (readChangeFeed / readChangeData) are not supported in the V2 " +
          "connector. Either remove the CDC read option or use a streaming read.")
    }
    val deltaScan = preparedScan()
    // File selection is done by V1 data skipping, so no kernel predicates are
    // pushed to the batch. Keep two distinct filter sets: partition + data filters distinguish
    // batches that select different files under otherwise-equal state, while only data filters may
    // reach the Parquet reader. Partition columns are materialized from the file path rather than
    // stored in Parquet, so passing a partition predicate to Parquet can incorrectly reject every
    // row. Order-insensitive equality is handled by DeltaV2Batch.
    val pushedCatalystFilters = partitionFilters ++ dataFilters
    val batchPushedFilters = DeltaV2FilterTranslator.translate(pushedCatalystFilters)
    val batchDataFilters = DeltaV2FilterTranslator.translate(dataFilters)
    new DeltaV2Batch(
      initialSnapshot = initialSnapshot,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      readDataSchema = readDataSchema,
      ddlOrderedReadOutputSchema = ddlOrderedReadOutputSchema,
      deltaScan = deltaScan,
      tablePath = getTablePath(),
      zoneId = zoneId,
      kernelPushedFilters = new Array[KernelPredicate](0),
      dataFilters = batchDataFilters,
      allFilters = batchPushedFilters,
      readerOptions = scalaOptions,
      hadoopConf = hadoopConf
    )
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val streamTableManager = tableManager.getOrElse {
      throw new UnsupportedOperationException("Streaming reads require a table manager")
    }
    // Loads a fresh snapshot as the baseline for schema change detection and table identity
    // checks. DeltaV2Scan's initialSnapshot is from analysis time and may be stale by stream
    // start/restart.
    // Matches V1's DeltaDataSource.createSource() behavior.
    val latestSnapshot = snapshotManager.loadLatestSnapshot(originalQueryContext)
    val spark = SparkSession.active
    // Create metadata tracking log for non-additive schema evolution support.
    // Mirrors V1's DeltaDataSource.getMetadataTrackingLogForDeltaSource(). At execution time the
    // merger is gated off (mergeConsecutiveSchemaChanges=false) -- that fold only runs at analysis.
    val metadataTrackingLog: Option[DeltaSourceMetadataTrackingLog] =
      MetadataEvolutionHandler.getMetadataTrackingLogForMicroBatchStream(
        spark,
        latestSnapshot,
        options,
        snapshotManager,
        KernelEngineFactory.createDefaultEngine(hadoopConf),
        Option(checkpointLocation),
        /* mergeConsecutiveSchemaChanges= */ false,
        originalQueryContext
      )
    new DeltaV2MicroBatchStream(
      streamTableManager,
      latestSnapshot,
      hadoopConf,
      spark,
      deltaOptions,
      getTablePath(),
      dataSchema,
      partitionSchema,
      readDataSchema,
      ddlOrderedReadOutputSchema,
      new Array[Filter](0),
      if (scalaOptions != null) scalaOptions else Map.empty[String, String],
      metadataTrackingLog,
      checkpointLocation,
      originalQueryContext
    )
  }

  override def description(): String = {
    val pushed = partitionFilters.iterator.map(_.toString).mkString(", ")
    val data = dataFilters.iterator.map(_.toString).mkString(", ")
    val description = new StringBuilder(
      String.format(Locale.ROOT, "PushedFilters: [%s], DataFilters: [%s]", pushed, data)
    )
    pushedLimit.foreach(
      limit => description.append(String.format(Locale.ROOT, ", PushedLimit: %d", Int.box(limit)))
    )
    description.toString
  }

  /**
   * Returns the catalog size when available, otherwise a file-size estimate without constructing
   * row or column statistics. Before planning the estimate uses snapshot metadata; afterwards it
   * uses the selected files.
   */
  override def estimateSizeInBytes(): OptionalLong = estimateSizeInBytesInternal().toJavaPrimitive

  private[read] def estimateSizeInBytesInternal(): Option[Long] = {
    if (isZeroLimit()) {
      return Some(0L)
    }
    if (catalogStats.isDefined) {
      return catalogStats.get.sizeInBytes().toScala
    }
    estimateSelectedFileSizeInBytes()
  }

  override def estimateStatistics(): Statistics = {
    if (isZeroLimit()) {
      return statistics(Some(0L), Some(0L), Collections.emptyMap())
    }
    if (catalogHasNumRows()) {
      val stats = catalogStats.get
      return statistics(None, stats.numRows().toScala, stats.columnStats())
    }
    statistics(estimateSelectedFileSizeInBytes(), None, Collections.emptyMap())
  }

  private def estimateSelectedFileSizeInBytes(): Option[Long] = synchronized {
    // TODO: Revisit this shortcut after coordinated scan preparation lands.
    // An early unfiltered optimizer request does not need file selection: the snapshot checksum is
    // already the exact size of every selected file. Filtered, limit, and CDC scans still plan here
    // so Spark receives their accurate post-pruning size. If checksum bytes are unavailable, fall
    // through to planning rather than replacing an accurate statistic with an unknown value.
    if (!planned && canUseSnapshotSizeForUnfilteredEstimate()) {
      val tableSizeInBytes = snapshotSizeInBytesIfKnown()
      if (tableSizeInBytes.isDefined) {
        return tableSizeInBytes
      }
    }

    val deltaScan = preparedScan()
    val totalBytes = deltaScan.scanned.bytesCompressed
      .getOrElse(selectedFileSizeInBytes(deltaScan))
    // Do not scale the selected-file bytes by readSchema. Delta returns false from
    // reflectsFullyPushedDownFilters(), so Spark re-adds fully pushed filters when adjusting
    // statistics. Delta's scan builder retains filter-only columns in readSchema; when that makes
    // the scan output wider than the query output, Spark also leaves the Project that restores the
    // query output above the relation. The Filter and Project in Spark's logical plan, rather than
    // this scan, own those statistics adjustments.
    Some(totalBytes)
  }

  private def isZeroLimit(): Boolean = pushedLimit.isDefined && pushedLimit.get == 0

  private def canUseSnapshotSizeForUnfilteredEstimate(): Boolean =
    !cdcReadEnabled && dataFilters.length == 0 && partitionFilters.length == 0 &&
    pushedLimit.isEmpty

  private def snapshotSizeInBytesIfKnown(): Option[Long] =
    initialSnapshot.sizeInBytesIfKnown

  private def statistics(
      sizeInBytesArg: Option[Long],
      numRowsArg: Option[Long],
      columnStatsArg: JMap[NamedReference, ColumnStatistics]
  ): Statistics = {
    val reportedSizeInBytes = sizeInBytesArg.toJavaPrimitive
    val reportedNumRows = numRowsArg.toJavaPrimitive
    new Statistics {
      override def sizeInBytes(): OptionalLong = reportedSizeInBytes
      override def numRows(): OptionalLong = reportedNumRows
      override def columnStats(): JMap[NamedReference, ColumnStatistics] = columnStatsArg
    }
  }

  /**
   * Delta requires Spark to retain fully pushed predicates when adjusting scan statistics. In
   * particular, catalog statistics describe the unfiltered table and must not be treated as if
   * every connector-pushed predicate were already reflected.
   */
  override def reflectsFullyPushedDownFilters(): Boolean =
    reflectsFullyPushedDownFiltersInternal()

  private[read] def reflectsFullyPushedDownFiltersInternal(): Boolean = false

  /**
   * Get the table path from the scan state.
   *
   * @return the table path with trailing slash
   */
  def getTablePath(): String = {
    // PartitionUtils passes the resolved path to SparkPath.fromUrlString, so the table root must be
    // URL-encoded (for example, spaces and literal '%' characters).
    val tableRoot = initialSnapshot.dataPath.toUri.toString
    if (tableRoot.endsWith("/")) tableRoot else tableRoot + "/"
  }

  /** Select files lazily and reuse the statistics computed by V1 file selection. */
  private def planScanFiles(): Unit = {
    // Select files lazily via V1 data skipping over the Kernel-backed snapshot, which
    // DataSkippingDeltaV2SnapshotSuite validates against the V1 oracle. Keeping this work behind
    // batch planning avoids running an unused batch scan for MicroBatchStream. When a limit is
    // pushed the supplier routes selection through V1's limit-aware filesForScan.
    val deltaScan = recordFrameProfileValue(
      "scan.awaitFileSelection",
      () => Objects.requireNonNull(deltaScanSupplier(), "deltaScanSupplier returned null"))
    val materializeFiles: Runnable = () => materializeSelectedFiles(deltaScan)
    recordFrameProfileAction("scan.materializeSelectedFiles", materializeFiles)
  }

  /** Retains scan metadata without constructing physical input files. */
  private def materializeSelectedFiles(deltaScan: DeltaScan): Unit = {
    Objects.requireNonNull(deltaScan.scannedSnapshot, "deltaScan.scannedSnapshot returned null")
    // Preserve V1's aggregates, including unknown values. Stats access must not traverse AddFiles
    // to recompute bytes or per-file row counts.
    currentDeltaScan = deltaScan
  }

  /**
   * Returns the lazily selected scan without constructing physical input files. Initialization is
   * guarded by this scan's monitor; planned is published only after success.
   */
  private[read] def preparedScan(): DeltaScan = synchronized {
    if (!planned) {
      recordFrameProfileAction("scan.planFiles", () => planScanFiles())
      planned = true
    }
    currentDeltaScan
  }

  def getDataSchema(): StructType = dataSchema

  def getPartitionSchema(): StructType = partitionSchema

  def getReadDataSchema(): StructType = readDataSchema

  def getOptions(): CaseInsensitiveStringMap = options

  def getConfiguration(): Configuration = hadoopConf

  /** Returns the limit pushed down from Spark, if any. Package-private for testing. */
  private[read] def getPushedLimit(): Option[Int] = pushedLimit

  private def applyRuntimePredicates(
      runtimePredicates: mutable.ArrayBuffer[RuntimePredicate]): Unit = synchronized {
    preparedScan()
    // Record the applied predicates for equals/hashCode comparison
    val predicateIterator = runtimePredicates.iterator
    while (predicateIterator.hasNext) {
      appliedRuntimePredicates.add(predicateIterator.next().predicate)
    }
    val runtimeFilteredAddFiles = new mutable.ArrayBuffer[AddFile]
    var newTotalBytes = 0L
    var newRowCountKnown = currentDeltaScan.scanned.rows.isDefined
    var newTotalRows = 0L
    var originalFileCount = 0L
    val caster = new PartitionValueCaster(partitionSchema, zoneId)
    val fileIterator = currentDeltaScan.files.iterator
    while (fileIterator.hasNext) {
      val addFile = fileIterator.next()
      originalFileCount += 1
      val partitionValues =
        PartitionUtils.getPartitionRow(addFile.partitionValues, caster)
      val allMatch =
        runtimePredicates.forall(predicate => predicate.evaluator.eval(partitionValues))
      if (newRowCountKnown) {
        val numRecords = addFile.numPhysicalRecords
        if (numRecords.isEmpty) {
          newRowCountKnown = false
        } else if (allMatch) {
          newTotalRows += numRecords.get
        }
      }
      if (allMatch) {
        runtimeFilteredAddFiles.append(addFile)
        newTotalBytes += addFile.size
      }
    }
    // A no-op filter preserves aggregate-only statistics. If files were pruned, only complete
    // per-file counts can describe the survivors (limit selection can omit those counts).
    if (runtimeFilteredAddFiles.size.toLong < originalFileCount) {
      val scanned = new DataSize(
        bytesCompressed = Option(newTotalBytes),
        rows = if (newRowCountKnown) Option(newTotalRows) else Option.empty,
        files = Option(runtimeFilteredAddFiles.size.toLong),
        logicalRows = Option.empty
      )
      currentDeltaScan = withFilesAndStats(
        currentDeltaScan,
        Some(runtimeFilteredAddFiles.toList),
        scanned
      )
    }
  }

  override def filterAttributes(): Array[NamedReference] =
    partitionSchema.fields.map(field => FieldReference.column(field.name))

  override def filter(predicates: Array[V2Predicate]): Unit = {
    // Spark currently invokes this method for DynamicPruningExpression predicates. These normally
    // come from join dynamic partition pruning, but group-based row-level operations can also use
    // them to select the file groups that must be rewritten. Delta advertises only partition
    // columns through filterAttributes(), so accepted predicates prune the already-planned file
    // list by partition value.
    //
    // Runtime pruning happens after planScanFiles(), so combining it with a pushed limit would be
    // unsafe: limit planning could stop before files that survive the runtime predicate, and this
    // method can only remove planned files. Current Spark plan shapes prevent that combination.
    // Join DPP places a Join between the Limit and the scan. Group-based row-level operations do
    // not place a direct Limit over the scan, so neither shape matches Spark's
    // PhysicalOperation(_, Nil, scanBuilderHolder) limit pushdown gate.

    // Try to convert runtime predicates to catalyst expressions, then create predicate evaluators
    // Only track predicates that successfully convert to evaluators
    val runtimePredicates = new mutable.ArrayBuffer[RuntimePredicate]
    for (predicate <- predicates) {
      // only the predicates on partition columns will be converted
      val catalystExpr =
        dsv2PredicateToCatalystExpression(predicate, partitionSchema).toScala
      if (catalystExpr.isDefined) {
        val predicateEvaluator = CatalystPredicate.createInterpreted(catalystExpr.get)
        runtimePredicates.append(new RuntimePredicate(predicate, predicateEvaluator))
      }
    }
    if (!runtimePredicates.isEmpty) {
      // Apply runtime predicates while holding this scan's monitor
      applyRuntimePredicates(runtimePredicates)
    }
  }

  /** Returns whether the catalog-provided statistics include a numRows value. */
  private def catalogHasNumRows(): Boolean =
    catalogStats.isDefined && catalogStats.get.numRows().isPresent


  override def equals(o: Any): Boolean = {
    if (this.eq(o.asInstanceOf[AnyRef])) {
      return true
    }
    if (o == null || getClass != o.getClass) {
      return false
    }
    val that = o.asInstanceOf[DeltaV2Scan]
    Objects.equals(
      originalQueryContext.catalogTableOpt, that.originalQueryContext.catalogTableOpt) &&
    Objects.equals(initialSnapshot.path, that.initialSnapshot.path) &&
    initialSnapshot.version == that.initialSnapshot.version &&
    Objects.equals(dataSchema, that.dataSchema) &&
    Objects.equals(partitionSchema, that.partitionSchema) &&
    Objects.equals(readDataSchema, that.readDataSchema) &&
    Objects.equals(partitionFiltersSet, that.partitionFiltersSet) &&
    Objects.equals(dataFiltersSet, that.dataFiltersSet) &&
    Objects.equals(options, that.options) &&
    Objects.equals(pushedLimit, that.pushedLimit) &&
    Objects.equals(appliedRuntimePredicates, that.appliedRuntimePredicates) &&
    Objects.equals(catalogStats, that.catalogStats)
  }

  override def hashCode(): Int = {
    var result = Objects.hash(
      originalQueryContext.catalogTableOpt,
      catalogStats.orNull,
      initialSnapshot.path,
      Long.box(initialSnapshot.version),
      dataSchema,
      partitionSchema,
      readDataSchema,
      options,
      Int.box(pushedLimit.getOrElse(0)),
      appliedRuntimePredicates,
      dataFiltersSet
    )
    // Fold in the partition-filter set so scans that differ only by pushed partition filters hash
    // differently, matching equals()'s partitionFiltersSet check.
    result = 31 * result + partitionFiltersSet.hashCode()
    result
  }
}

private[read] object DeltaV2Scan {
  /**
   * Sum of the selected files' on-disk sizes. Used as the scanned-bytes fallback when the DeltaScan
   * carries no `scanned.bytesCompressed` aggregate (for example, a table without file
   * statistics). `AddFile.size()` is always populated, so this reads only lightweight file
   * metadata and never constructs physical input files.
   */
  private def selectedFileSizeInBytes(deltaScan: DeltaScan): Long = {
    var totalBytes = 0L
    val iterator = deltaScan.files.iterator
    while (iterator.hasNext) {
      totalBytes += iterator.next().size
    }
    totalBytes
  }


  /** Copies stats and optionally files, preserving snapshot and filter metadata. */
  private def withFilesAndStats(
      deltaScan: DeltaScan,
      replacementFiles: Option[Seq[AddFile]],
      scanned: DataSize): DeltaScan = {
    deltaScan.copy(
      version = deltaScan.version,
      files = if (replacementFiles.isDefined) replacementFiles.get else deltaScan.files,
      total = deltaScan.total,
      partition = deltaScan.partition,
      scanned = scanned)(
      scannedSnapshot = deltaScan.scannedSnapshot,
      partitionFilters = deltaScan.partitionFilters,
      dataFilters = deltaScan.dataFilters,
      partitionLikeDataFilters = deltaScan.partitionLikeDataFilters,
      rewrittenPartitionLikeDataFilters = deltaScan.rewrittenPartitionLikeDataFilters,
      unusedFilters = deltaScan.unusedFilters,
      scanDurationMs = deltaScan.scanDurationMs,
      scanDurationNs = deltaScan.scanDurationNs,
      dataSkippingType = deltaScan.dataSkippingType)
  }

  /** Set of exprId-normalized filter expressions (exprId- and order-insensitive) for equals. */
  private def canonicalizedSet(filters: Array[Expression]): mutable.HashSet[Expression] = {
    val set = new mutable.HashSet[Expression]
    for (f <- filters) {
      set.add(DeltaV2ScanBuilder.normalizeForEquality(f))
    }
    set
  }

  /**
   * Holds a runtime predicate from [[DeltaV2Scan.filter]] along with its compiled evaluator.
   *
   * Only created for predicates that can be successfully converted to Catalyst expressions
   * (typically predicates on partition columns) and compiled into InterpretedPredicate evaluators.
   * Predicates that cannot be converted are not instantiated as RuntimePredicate objects.
   */
  private class RuntimePredicate(val predicate: V2Predicate, val evaluator: InterpretedPredicate)
}

/**
 * Declares statistics methods that are absent from [[SupportsReportStatistics]] in some supported
 * Spark versions. This gives the scan's `override def` implementations a declaration to override
 * when compiling against those versions. Keep this trait while those versions are supported.
 */
private[read] trait DeltaV2ScanStatistics {
  def estimateSizeInBytes(): OptionalLong
  def reflectsFullyPushedDownFilters(): Boolean
}
