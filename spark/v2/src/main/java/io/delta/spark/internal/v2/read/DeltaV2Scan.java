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

import static io.delta.spark.internal.v2.utils.ExpressionUtils.dsv2PredicateToCatalystExpression;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.internal.v2.DeltaV2JavaLogging;
import io.delta.spark.internal.v2.kernel.KernelEngineFactory;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorSchemaContext;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.InterpretedPredicate;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.stats.DeltaScan;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.v2.DeltaV2FilterTranslator;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

/**
 * Package-private scan implementation for Delta's Spark DataSource V2 read path.
 *
 * <p>This class must remain package-private so callers outside {@code v2.read} depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
class DeltaV2Scan extends DeltaV2JavaLogging
    implements Scan, SupportsReportStatistics, SupportsRuntimeV2Filtering {

  private final DeltaV2SnapshotManager snapshotManager;
  private final io.delta.kernel.Snapshot initialSnapshot;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final StructType ddlOrderedReadOutputSchema;
  // Produces the V1 DeltaScan via DeltaV2Snapshot.filesForScan. Kept lazy because streaming scans
  // never consume batch-selected files and must not run the batch data-skipping path during
  // ScanBuilder.build().
  private final Supplier<DeltaScan> deltaScanSupplier;
  // Pushed data filters (min/max skipping), as Catalyst expressions, for explain and scan equality.
  private final Expression[] dataFilters;
  // Pushed partition filters (row-exact), as Catalyst expressions, for explain and scan equality.
  private final Expression[] partitionFilters;
  private final Set<Expression> partitionFiltersSet;
  // Derived Sets used only for equals/hashCode: filters are AND-ed at evaluation time,
  // so list order has no semantic meaning and two scans with the same filter set in
  // different orders should compare equal.
  private final Set<Expression> dataFiltersSet;
  private final Optional<Statistics> catalogStats;
  private final Configuration hadoopConf;
  private final boolean isCDCRead;
  private final CaseInsensitiveStringMap options;
  // Empty means no limit pushdown from Spark.
  private final OptionalInt pushedLimit;
  private final scala.collection.immutable.Map<String, String> scalaOptions;
  private final SQLConf sqlConf;
  private final DeltaOptions deltaOptions;
  private final ZoneId zoneId;

  // Planned input files and the corresponding selected AddFile actions.
  private List<PartitionedFile> partitionedFiles = new ArrayList<>();
  // Per-file row counts, parallel to partitionedFiles. Populated only while rowCountKnown is
  // true; cleared if any AddFile lacks numRecords. Retained so totalRows can be recomputed
  // after runtime partition filtering prunes files, instead of invalidating the count.
  private List<Long> perFileRowCounts = new ArrayList<>();
  private List<DeltaScanFile> selectedFiles = new ArrayList<>();
  private long totalBytes = 0L;
  private long totalRows = 0L;
  // true iff every AddFile in the scan had numRecords in its stats JSON.
  private boolean rowCountKnown = false;
  private org.apache.spark.sql.delta.Snapshot plannedSnapshot = null;
  private volatile boolean planned = false;

  // Runtime predicates applied after planning (using Set for order-independent comparison)
  private final Set<org.apache.spark.sql.connector.expressions.filter.Predicate>
      appliedRuntimePredicates = new HashSet<>();

  // TODO(#6743): bundle scan-level schemas into a single ScanSchemaContext.
  public DeltaV2Scan(
      DeltaV2SnapshotManager snapshotManager,
      io.delta.kernel.Snapshot initialSnapshot,
      StructType tableSchema,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Supplier<DeltaScan> scanSupplier,
      Expression[] dataFilters,
      Expression[] partitionFilters,
      Optional<Statistics> catalogStats,
      CaseInsensitiveStringMap options,
      OptionalInt pushedLimit) {

    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.initialSnapshot = Objects.requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.deltaScanSupplier = Objects.requireNonNull(scanSupplier, "deltaScanSupplier is null");
    this.dataFilters = dataFilters == null ? new Expression[0] : dataFilters.clone();
    this.partitionFilters = partitionFilters == null ? new Expression[0] : partitionFilters.clone();
    this.partitionFiltersSet = canonicalizedSet(this.partitionFilters);
    // Canonicalize so equals/hashCode ignore exprId/ordering: two scans of the same table with the
    // same logical filters compare equal even when the filter expressions carry different exprIds.
    this.dataFiltersSet = canonicalizedSet(this.dataFilters);
    this.catalogStats = Objects.requireNonNull(catalogStats, "catalogStats is null");
    this.options = Objects.requireNonNull(options, "options is null");
    this.pushedLimit = Objects.requireNonNull(pushedLimit, "pushedLimit is null");
    this.scalaOptions = ScalaUtils.toScalaMap(options);
    this.hadoopConf = SparkSession.active().sessionState().newHadoopConfWithOptions(scalaOptions);
    this.sqlConf = SQLConf.get();
    this.deltaOptions = new DeltaOptions(scalaOptions, sqlConf);
    this.isCDCRead = deltaOptions.readChangeFeed();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
    StructType ddlOrdered =
        SchemaUtils.ddlOrderedOutputSchema(tableSchema, readDataSchema, partitionSchema);
    this.ddlOrderedReadOutputSchema =
        isCDCRead ? CDCSchemaContext.appendCDCColumns(ddlOrdered) : ddlOrdered;
  }

  /** Read schema for the scan, in the table's DDL column order. */
  @Override
  public StructType readSchema() {
    return ddlOrderedReadOutputSchema;
  }

  /**
   * Override columnarSupportMode to explicitly declare whether this scan supports columnar
   * (vectorized) reading. Without this override, the default {@code PARTITION_DEFINED} mode causes
   * Spark to eagerly call {@code planInputPartitions()} during query planning to check
   * per-partition columnar support, triggering unnecessary early file enumeration.
   *
   * <p>Since columnar support is uniform across all partitions (determined by schema compatibility
   * and table features, not by individual files), we can declare it at the scan level to avoid this
   * overhead.
   *
   * <p>This must stay consistent with the vectorized reader decision in {@link
   * PartitionUtils#createDeltaParquetReaderFactory}. In particular, deletion-vector-enabled tables
   * augment the read schema with internal columns (e.g., {@code __delta_internal_is_row_deleted}),
   * which changes the schema passed to the vectorized reader check. We replicate that logic here to
   * ensure the scan-level declaration matches the per-partition reader behavior.
   */
  @Override
  public Scan.ColumnarSupportMode columnarSupportMode() {
    boolean metadataColumnRequested =
        Arrays.stream(readDataSchema.fields())
            .anyMatch(field -> FileFormat$.MODULE$.METADATA_NAME().equals(field.name()));
    if (metadataColumnRequested) {
      return Scan.ColumnarSupportMode.UNSUPPORTED;
    }

    // Mirror the schema augmentation chain in PartitionUtils.createDeltaParquetReaderFactory
    // (CDC then DV, in that order) so the batch-read check sees the same final schema the
    // parquet reader will. If you reorder or add augmentations there, update this in lockstep.
    StructType schemaForBatchCheck = readDataSchema;
    if (isCDCRead) {
      schemaForBatchCheck = CDCSchemaContext.appendCDCColumns(schemaForBatchCheck);
    }
    if (PartitionUtils.tableSupportsDeletionVectors(initialSnapshot)) {
      schemaForBatchCheck =
          new DeletionVectorSchemaContext(schemaForBatchCheck, partitionSchema)
              .getSchemaWithDvColumn();
    }

    return ParquetUtils.isBatchReadSupportedForSchema(sqlConf, schemaForBatchCheck)
        ? Scan.ColumnarSupportMode.SUPPORTED
        : Scan.ColumnarSupportMode.UNSUPPORTED;
  }

  // TODO: Drop toBatch (and the filter translation it needs) once this scan implements the
  // file-source Scan interface. DataSourceV2Strategy already routes a Scan that implements
  // org.apache.spark.sql.connector.read.FileScan through FileScanBridge to FileSourceScanExec,
  // ahead of the plain DataSourceV2ScanRelation case, so toBatch stops being reachable at that
  // point and the Batch-only bookkeeping below goes away with it.
  @Override
  public Batch toBatch() {
    return recordFrameProfileValue("batchScan.toBatch", this::createBatch);
  }

  /** Constructs the fallback batch without putting its multi-step body inside a Java lambda. */
  private Batch createBatch() {
    if (isCDCRead) {
      throw new UnsupportedOperationException(
          "Batch reads with CDC (readChangeFeed / readChangeData) are not supported in the V2 "
              + "connector. Either remove the CDC read option or use a streaming read.");
    }
    ensurePlanned();
    // File selection is done by V1 data skipping (partitionedFiles), so no kernel predicates are
    // pushed to the batch. Keep two distinct filter sets: partition + data filters distinguish
    // batches that select different files under otherwise-equal state, while only data filters may
    // reach the Parquet reader. Partition columns are materialized from the file path rather than
    // stored in Parquet, so passing a partition predicate to Parquet can incorrectly reject every
    // row. Order-insensitive equality is handled by DeltaV2Batch.
    final Expression[] pushedCatalystFilters =
        new Expression[partitionFilters.length + dataFilters.length];
    System.arraycopy(partitionFilters, 0, pushedCatalystFilters, 0, partitionFilters.length);
    System.arraycopy(
        dataFilters, 0, pushedCatalystFilters, partitionFilters.length, dataFilters.length);
    final Filter[] batchPushedFilters = DeltaV2FilterTranslator.translate(pushedCatalystFilters);
    final Filter[] batchDataFilters = DeltaV2FilterTranslator.translate(dataFilters);
    return new DeltaV2Batch(
        initialSnapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        partitionedFiles,
        new Predicate[0],
        batchDataFilters,
        batchPushedFilters,
        totalBytes,
        scalaOptions,
        hadoopConf);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    // Loads a fresh snapshot as the baseline for schema change detection and table identity
    // checks. DeltaV2Scan's initialSnapshot is from analysis time and may be stale by stream
    // start/restart.
    // Matches V1's DeltaDataSource.createSource() behavior.
    Snapshot latestSnapshot = snapshotManager.loadLatestSnapshot();
    SparkSession spark = SparkSession.active();

    // Create metadata tracking log for non-additive schema evolution support.
    // Mirrors V1's DeltaDataSource.getMetadataTrackingLogForDeltaSource(). At execution time the
    // merger is gated off (mergeConsecutiveSchemaChanges=false) — that fold only runs at analysis.
    Option<DeltaSourceMetadataTrackingLog> metadataTrackingLog =
        MetadataEvolutionHandler.getMetadataTrackingLogForMicroBatchStream(
            spark,
            latestSnapshot,
            options,
            snapshotManager,
            KernelEngineFactory.createDefaultEngine(hadoopConf),
            Option.apply(checkpointLocation),
            /* mergeConsecutiveSchemaChanges= */ false);

    return new DeltaV2MicroBatchStream(
        snapshotManager,
        latestSnapshot,
        hadoopConf,
        spark,
        deltaOptions,
        getTablePath(),
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        new Filter[0],
        scalaOptions != null ? scalaOptions : scala.collection.immutable.Map$.MODULE$.empty(),
        metadataTrackingLog,
        checkpointLocation);
  }

  @Override
  public String description() {
    final String pushed =
        Arrays.stream(partitionFilters).map(Object::toString).collect(Collectors.joining(", "));
    final String data =
        Arrays.stream(dataFilters).map(Object::toString).collect(Collectors.joining(", "));
    final StringBuilder description =
        new StringBuilder(
            String.format(Locale.ROOT, "PushedFilters: [%s], DataFilters: [%s]", pushed, data));
    pushedLimit.ifPresent(
        limit -> description.append(String.format(Locale.ROOT, ", PushedLimit: %d", limit)));
    return description.toString();
  }

  /**
   * Returns the catalog size when available, or the compression-adjusted post-pruning file size,
   * without constructing row or column statistics.
   *
   * <p>Intentionally no {@code @Override}: Delta also compiles this source against supported Spark
   * versions whose {@link SupportsReportStatistics} interface predates this optional method. The
   * method still overrides the interface default when Delta is built against a Spark version that
   * provides it.
   */
  public OptionalLong estimateSizeInBytes() {
    return estimateSizeInBytesInternal();
  }

  /** Package-private entry point for Scala wrappers compiled together with this Java source. */
  OptionalLong estimateSizeInBytesInternal() {
    if (isZeroLimit()) {
      return OptionalLong.of(0L);
    }
    if (catalogStats.isPresent()) {
      return catalogStats.get().sizeInBytes();
    }
    return estimateSelectedFileSizeInBytes();
  }

  @Override
  public Statistics estimateStatistics() {
    if (isZeroLimit()) {
      return statistics(OptionalLong.of(0L), OptionalLong.of(0L), Collections.emptyMap());
    }
    if (catalogHasNumRows()) {
      final Statistics stats = catalogStats.get();
      return statistics(OptionalLong.empty(), stats.numRows(), stats.columnStats());
    }
    return statistics(
        estimateSelectedFileSizeInBytes(), OptionalLong.empty(), Collections.emptyMap());
  }

  private OptionalLong estimateSelectedFileSizeInBytes() {
    ensurePlanned();
    // Do not scale the selected-file bytes by readSchema. Delta returns false from
    // reflectsFullyPushedDownFilters(), so Spark re-adds fully pushed filters when adjusting
    // statistics. Delta's scan builder retains filter-only columns in readSchema; when that makes
    // the scan output wider than the query output, Spark also leaves the Project that restores the
    // query output above the relation. The Filter and Project in Spark's logical plan, rather than
    // this scan, own those statistics adjustments.
    return OptionalLong.of(totalBytes);
  }

  private boolean isZeroLimit() {
    return pushedLimit.isPresent() && pushedLimit.getAsInt() == 0;
  }

  private Statistics statistics(
      OptionalLong sizeInBytes,
      OptionalLong numRows,
      Map<NamedReference, ColumnStatistics> columnStats) {
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return sizeInBytes;
      }

      @Override
      public OptionalLong numRows() {
        return numRows;
      }

      @Override
      public Map<NamedReference, ColumnStatistics> columnStats() {
        return columnStats;
      }
    };
  }

  /**
   * Delta requires Spark to retain fully pushed predicates when adjusting scan statistics. In
   * particular, catalog statistics describe the unfiltered table and must not be treated as if
   * every connector-pushed predicate were already reflected.
   *
   * <p>Intentionally no {@code @Override}; see {@link #estimateSizeInBytes()}.
   */
  public boolean reflectsFullyPushedDownFilters() {
    return reflectsFullyPushedDownFiltersInternal();
  }

  /** Package-private entry point for Scala wrappers compiled together with this Java source. */
  boolean reflectsFullyPushedDownFiltersInternal() {
    return false;
  }

  /**
   * Get the table path from the scan state.
   *
   * @return the table path with trailing slash
   */
  public String getTablePath() {
    // PartitionUtils passes the resolved path to SparkPath.fromUrlString, so the table root must be
    // URL-encoded (for example, spaces and literal '%' characters).
    final String tableRoot =
        new Path(((SnapshotImpl) initialSnapshot).getDataPath().toString()).toUri().toString();
    return tableRoot.endsWith("/") ? tableRoot : tableRoot + "/";
  }

  /**
   * Plan the files to scan by materializing {@link PartitionedFile}s and aggregating size stats.
   * Ensures all iterators are closed to avoid resource leaks.
   *
   * <p>When a limit is pushed (via {@link SupportsPushDownLimit}), per-file {@code numRecords}
   * (minus deletion-vector cardinality) are used to stop adding files once enough logical rows have
   * been accumulated to satisfy the limit. Files that lack statistics are added but do not count
   * toward the limit.
   */
  private void planScanFiles() {
    final String tablePath = getTablePath();
    // Select files lazily via V1 data skipping over the Kernel-backed snapshot, which
    // DataSkippingDeltaV2SnapshotSuite validates against the V1 oracle. Keeping this work behind
    // batch planning avoids running an unused batch scan for MicroBatchStream. When a limit is
    // pushed the supplier routes selection through V1's limit-aware filesForScan. Here we
    // materialize the selected V1 AddFiles into PartitionedFiles and aggregate stats.
    //
    // Two mutually exclusive row-count sources, mirroring which one V1 populates:
    //   - No limit: the builder passes keepNumRecords (the same plan-stats condition as
    //     arePlanStatsEnabled), so each AddFile carries numRecords and we sum per file below.
    //     Per-file counts also let runtime partition filtering re-derive the post-prune count.
    //   - Limit pushed: V1's limit-aware filesForScan takes no keepNumRecords and always nulls
    //     out AddFile.stats (DataSkippingReader.getFilesAndNumRecords defaults keepStats=false),
    //     but it does report the aggregate in DeltaScan.scanned.rows -- the very field V1's
    //     PreparedDeltaFileIndex.getNumOfRows reads. The aggregate fallback below picks it up.
    final Supplier<DeltaScan> selectFiles =
        () -> Objects.requireNonNull(deltaScanSupplier.get(), "deltaScanSupplier returned null");
    final DeltaScan deltaScan = recordFrameProfileValue("scan.awaitFileSelection", selectFiles);
    final Runnable materializeFiles = () -> materializeSelectedFiles(deltaScan, tablePath);
    recordFrameProfileAction("scan.materializeSelectedFiles", materializeFiles);
  }

  /**
   * Converts the files selected by {@code deltaScan} to connector scan objects and aggregates scan
   * size statistics.
   */
  private void materializeSelectedFiles(DeltaScan deltaScan, String tablePath) {
    plannedSnapshot =
        Objects.requireNonNull(
            deltaScan.scannedSnapshot(), "deltaScan.scannedSnapshot returned null");
    rowCountKnown = arePlanStatsEnabled();
    final List<org.apache.spark.sql.delta.actions.AddFile> scanFiles =
        scala.jdk.javaapi.CollectionConverters.asJava(deltaScan.files());

    for (org.apache.spark.sql.delta.actions.AddFile addFile : scanFiles) {
      partitionedFiles.add(
          PartitionUtils.buildPartitionedFile(addFile, partitionSchema, tablePath, zoneId));
      // Track the selected file descriptor in parallel with partitionedFiles for the row-level
      // ReplaceData write path (getSelectedFiles) and runtime-filter bookkeeping below.
      selectedFiles.add(DeltaScanFile.fromV1AddFile(addFile));
      totalBytes += addFile.size();

      if (rowCountKnown) {
        scala.Option<Object> numRecords = addFile.numPhysicalRecords();
        if (numRecords.isDefined()) {
          long n = ((Number) numRecords.get()).longValue();
          totalRows += n;
          perFileRowCounts.add(n);
        } else {
          // This file has no numRecords -- row count is unknowable for the whole scan.
          // Clear partial state and stop accumulating for all subsequent files.
          rowCountKnown = false;
          totalRows = 0;
          perFileRowCounts.clear();
        }
      }
    }

    // Fall back to the scan-level aggregate when per-file counts were unavailable. This is the
    // normal case for a pushed limit (V1 nulls per-file stats but fills scanned.rows) and also
    // recovers the count for any other filesForScan path that reports only the aggregate.
    // perFileRowCounts stays empty, so runtime partition filtering below invalidates the count
    // rather than pretending a scan-level total still applies to a pruned file set.
    if (!rowCountKnown && arePlanStatsEnabled()) {
      final scala.Option<Object> scannedRows = deltaScan.scanned().rows();
      if (scannedRows.isDefined()) {
        rowCountKnown = true;
        totalRows = ((Number) scannedRows.get()).longValue();
      }
    }
  }

  /**
   * Ensure the scan is planned exactly once in a thread-safe manner, optionally applying runtime
   * filters.
   */
  private synchronized void ensurePlanned(List<RuntimePredicate> runtimePredicates) {
    // First, ensure planning is done
    if (!planned) {
      recordFrameProfileAction("scan.planFiles", this::planScanFiles);
      planned = true;
    }

    // Then apply runtime predicates if provided
    if (runtimePredicates != null && !runtimePredicates.isEmpty()) {
      // Record the applied predicates for equals/hashCode comparison
      for (RuntimePredicate filter : runtimePredicates) {
        appliedRuntimePredicates.add(filter.predicate);
      }

      List<PartitionedFile> runtimeFilteredPartitionedFiles = new ArrayList<>();
      List<DeltaScanFile> runtimeFilteredFiles = new ArrayList<>();
      // Per-file counts exist only when planScanFiles summed them file by file. When the count came
      // from the scan-level aggregate instead (pushed limit), there is nothing to re-derive from,
      // so the count cannot survive pruning -- see the invalidation below.
      final boolean perFileCountsAvailable =
          rowCountKnown && this.perFileRowCounts.size() == this.partitionedFiles.size();
      // Parallel to runtimeFilteredPartitionedFiles; only used when per-file counts are available.
      List<Long> filteredRowCounts = perFileCountsAvailable ? new ArrayList<>() : null;
      long newTotalRows = 0L;
      for (int i = 0; i < this.partitionedFiles.size(); i++) {
        PartitionedFile pf = this.partitionedFiles.get(i);
        InternalRow partitionValues = pf.partitionValues();
        boolean allMatch =
            runtimePredicates.stream()
                .allMatch(predicate -> predicate.evaluator.eval(partitionValues));
        if (allMatch) {
          runtimeFilteredPartitionedFiles.add(pf);
          runtimeFilteredFiles.add(this.selectedFiles.get(i));
          if (perFileCountsAvailable) {
            long rc = this.perFileRowCounts.get(i);
            filteredRowCounts.add(rc);
            newTotalRows += rc;
          }
        }
      }

      // Update the filtered file set and totalBytes; recompute totalRows only when per-file counts
      // are available.
      if (runtimeFilteredPartitionedFiles.size() < this.partitionedFiles.size()) {
        this.partitionedFiles = runtimeFilteredPartitionedFiles;
        this.selectedFiles = runtimeFilteredFiles;
        this.totalBytes =
            runtimeFilteredPartitionedFiles.stream().mapToLong(PartitionedFile::fileSize).sum();
        if (perFileCountsAvailable) {
          // Recompute totalRows from per-file counts of files that survived pruning so
          // numRows() reports the post-prune count rather than a stale pre-filter value.
          this.perFileRowCounts = filteredRowCounts;
          this.totalRows = newTotalRows;
        } else if (rowCountKnown) {
          // The count came from the scan-level aggregate, which describes the pre-prune file set.
          // With no per-file breakdown it cannot be adjusted, so report unknown rather than a
          // count that overstates what will actually be read.
          this.rowCountKnown = false;
          this.totalRows = 0L;
        }
      }
    }
  }

  /** Ensure the scan is planned exactly once in a thread-safe manner. */
  private void ensurePlanned() {
    // Pass null to indicate no runtime predicate should be applied - just perform the scan planning
    ensurePlanned(null);
  }

  org.apache.spark.sql.delta.Snapshot plannedSnapshot() {
    ensurePlanned();
    return Objects.requireNonNull(plannedSnapshot, "plannedSnapshot is null after planning");
  }

  public StructType getDataSchema() {
    return dataSchema;
  }

  /**
   * Returns the Delta files selected by this scan after pushdown and runtime filtering.
   *
   * <p>The returned descriptors preserve only the metadata needed by row-level ReplaceData commits
   * to construct matching RemoveFile actions.
   *
   * @apiNote Internal API for the DSv2 DML write path (see {@code DeltaReplaceDataBatchWrite}).
   */
  public List<DeltaScanFile> getSelectedFiles() {
    ensurePlanned();
    return Collections.unmodifiableList(selectedFiles);
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public StructType getReadDataSchema() {
    return readDataSchema;
  }

  public CaseInsensitiveStringMap getOptions() {
    return options;
  }

  public Configuration getConfiguration() {
    return hadoopConf;
  }

  /** Returns the limit pushed down from Spark, if any. Package-private for testing. */
  OptionalInt getPushedLimit() {
    return pushedLimit;
  }

  @Override
  public NamedReference[] filterAttributes() {
    return Arrays.stream(partitionSchema.fields())
        .map(field -> FieldReference.column(field.name()))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(org.apache.spark.sql.connector.expressions.filter.Predicate[] predicates) {
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
    List<RuntimePredicate> runtimePredicates = new ArrayList<>();
    for (org.apache.spark.sql.connector.expressions.filter.Predicate predicate : predicates) {
      // only the predicates on partition columns will be converted
      Optional<Expression> catalystExpr =
          dsv2PredicateToCatalystExpression(predicate, partitionSchema);
      if (catalystExpr.isPresent()) {
        InterpretedPredicate predicateEvaluator =
            org.apache.spark.sql.catalyst.expressions.Predicate.createInterpreted(
                catalystExpr.get());
        runtimePredicates.add(new RuntimePredicate(predicate, predicateEvaluator));
      }
    }

    if (!runtimePredicates.isEmpty()) {
      // Apply runtime predicates within the synchronized ensurePlanned method
      ensurePlanned(runtimePredicates);
    }
  }

  /**
   * Returns whether Delta should collect per-file row counts for scan metadata. The scan builder
   * evaluates the same condition when deciding whether to ask {@code filesForScan} to retain record
   * counts; the two must stay in sync.
   *
   * <p>This does not gate {@link #estimateStatistics()}. Spark's {@code
   * DataSourceV2ScanRelation.computeStats()} chooses between the full-statistics and size-only APIs
   * based on CBO and plan-statistics settings. Once Spark calls either API, Delta returns that
   * API's connector statistics independently of the planner-side gate.
   */
  private boolean arePlanStatsEnabled() {
    return sqlConf.cboEnabled() || sqlConf.planStatsEnabled();
  }
  /** Returns whether the catalog-provided statistics include a numRows value. */
  private boolean catalogHasNumRows() {
    return catalogStats.isPresent() && catalogStats.get().numRows().isPresent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeltaV2Scan that = (DeltaV2Scan) o;
    return Objects.equals(initialSnapshot.getPath(), that.initialSnapshot.getPath())
        && initialSnapshot.getVersion() == that.initialSnapshot.getVersion()
        && Objects.equals(dataSchema, that.dataSchema)
        && Objects.equals(partitionSchema, that.partitionSchema)
        && Objects.equals(readDataSchema, that.readDataSchema)
        && Objects.equals(partitionFiltersSet, that.partitionFiltersSet)
        && Objects.equals(dataFiltersSet, that.dataFiltersSet)
        && Objects.equals(options, that.options)
        && Objects.equals(pushedLimit, that.pushedLimit)
        && Objects.equals(appliedRuntimePredicates, that.appliedRuntimePredicates)
        && Objects.equals(catalogStats, that.catalogStats);
  }

  /** Set of exprId-normalized filter expressions (exprId- and order-insensitive) for equals. */
  private static Set<Expression> canonicalizedSet(Expression[] filters) {
    Set<Expression> set = new HashSet<>();
    for (Expression f : filters) {
      set.add(DeltaV2ScanBuilder.normalizeForEquality(f));
    }
    return set;
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            catalogStats,
            initialSnapshot.getPath(),
            initialSnapshot.getVersion(),
            dataSchema,
            partitionSchema,
            readDataSchema,
            options,
            pushedLimit,
            appliedRuntimePredicates,
            dataFiltersSet);
    // Fold in the partition-filter set so scans that differ only by pushed partition filters hash
    // differently, matching equals()'s partitionFiltersSet check.
    result = 31 * result + Objects.hashCode(partitionFiltersSet);
    return result;
  }

  /**
   * Holds a runtime predicate from {@link #filter(Predicate[])} along with its compiled evaluator.
   *
   * <p>Only created for predicates that can be successfully converted to Catalyst expressions
   * (typically predicates on partition columns) and compiled into InterpretedPredicate evaluators.
   * Predicates that cannot be converted are not instantiated as RuntimePredicate objects.
   */
  private static class RuntimePredicate {
    final org.apache.spark.sql.connector.expressions.filter.Predicate predicate;
    final InterpretedPredicate evaluator;

    RuntimePredicate(
        org.apache.spark.sql.connector.expressions.filter.Predicate predicate,
        InterpretedPredicate evaluator) {
      this.predicate = predicate;
      this.evaluator = evaluator;
    }
  }
}
