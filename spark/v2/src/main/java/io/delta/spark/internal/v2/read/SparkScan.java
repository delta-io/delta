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

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorSchemaContext;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark DSV2 Scan implementation backed by Delta Kernel. */
public class SparkScan implements Scan, SupportsReportStatistics, SupportsRuntimeV2Filtering {

  /** Supported streaming options for the V2 connector. */
  private static final List<String> SUPPORTED_STREAMING_OPTIONS =
      Collections.unmodifiableList(
          Arrays.asList(
              DeltaOptions.STARTING_VERSION_OPTION(),
              DeltaOptions.STARTING_TIMESTAMP_OPTION(),
              DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION(),
              DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION(),
              DeltaOptions.IGNORE_FILE_DELETION_OPTION(),
              DeltaOptions.IGNORE_CHANGES_OPTION(),
              DeltaOptions.IGNORE_DELETES_OPTION(),
              DeltaOptions.SKIP_CHANGE_COMMITS_OPTION(),
              DeltaOptions.EXCLUDE_REGEX_OPTION(),
              DeltaOptions.FAIL_ON_DATA_LOSS_OPTION()));

  /**
   * Block list of DeltaOptions that are not supported for streaming in V2 connector. Only
   * startingVersion, startingTimestamp, maxFilesPerTrigger, maxBytesPerTrigger, ignoreFileDeletion,
   * ignoreChanges, ignoreDeletes, skipChangeCommits, excludeRegex, and failOnDataLoss are
   * supported. User-defined custom options (not in DeltaOptions) are allowed to pass through.
   */
  private static final Set<String> UNSUPPORTED_STREAMING_OPTIONS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  DeltaOptions.CDC_READ_OPTION().toLowerCase(),
                  DeltaOptions.CDC_READ_OPTION_LEGACY().toLowerCase(),
                  DeltaOptions.CDC_END_VERSION().toLowerCase(),
                  DeltaOptions.CDC_END_TIMESTAMP().toLowerCase(),
                  DeltaOptions.SCHEMA_TRACKING_LOCATION().toLowerCase(),
                  DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS().toLowerCase(),
                  DeltaOptions.STREAMING_SOURCE_TRACKING_ID().toLowerCase(),
                  DeltaOptions.ALLOW_SOURCE_COLUMN_DROP().toLowerCase(),
                  DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME().toLowerCase(),
                  DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE().toLowerCase())));

  private final DeltaSnapshotManager snapshotManager;
  private final Snapshot initialSnapshot;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  // Derived Sets used only for equals/hashCode: filters are AND-ed at evaluation time,
  // so list order has no semantic meaning and two scans with the same filter set in
  // different orders should compare equal.
  private final Set<Predicate> pushedToKernelFiltersSet;
  private final Set<Filter> dataFiltersSet;
  private final io.delta.kernel.Scan kernelScan;
  private final Optional<Statistics> catalogStats;
  private final Configuration hadoopConf;
  private final boolean isCDCRead;
  private final CaseInsensitiveStringMap options;
  private final scala.collection.immutable.Map<String, String> scalaOptions;
  private final SQLConf sqlConf;
  private final DeltaOptions deltaOptions;
  private final ZoneId zoneId;

  // Planned input files and stats
  private List<PartitionedFile> partitionedFiles = new ArrayList<>();
  private long totalBytes = 0L;
  private long totalRows = 0L;
  // true iff every AddFile in the scan had numRecords in its stats JSON AND no runtime
  // partition filter has been applied (runtime filtering invalidates the cached totalRows).
  private boolean rowCountKnown = false;
  // Estimated size in bytes accounting for column projection, used for query optimizer cost
  // estimation
  private long estimatedSizeInBytes = 0L;
  private volatile boolean planned = false;

  // Runtime predicates applied after planning (using Set for order-independent comparison)
  private final Set<org.apache.spark.sql.connector.expressions.filter.Predicate>
      appliedRuntimePredicates = new HashSet<>();

  public SparkScan(
      DeltaSnapshotManager snapshotManager,
      Snapshot initialSnapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      io.delta.kernel.Scan kernelScan,
      Optional<Statistics> catalogStats,
      CaseInsensitiveStringMap options) {

    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.initialSnapshot = Objects.requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.pushedToKernelFilters =
        pushedToKernelFilters == null ? new Predicate[0] : pushedToKernelFilters.clone();
    this.dataFilters = dataFilters == null ? new Filter[0] : dataFilters.clone();
    this.pushedToKernelFiltersSet = Set.copyOf(Arrays.asList(this.pushedToKernelFilters));
    this.dataFiltersSet = Set.copyOf(Arrays.asList(this.dataFilters));
    this.kernelScan = Objects.requireNonNull(kernelScan, "kernelScan is null");
    this.catalogStats = Objects.requireNonNull(catalogStats, "catalogStats is null");
    this.options = Objects.requireNonNull(options, "options is null");
    this.scalaOptions = ScalaUtils.toScalaMap(options);
    this.hadoopConf = SparkSession.active().sessionState().newHadoopConfWithOptions(scalaOptions);
    this.sqlConf = SQLConf.get();
    this.deltaOptions = new DeltaOptions(scalaOptions, sqlConf);
    this.isCDCRead = deltaOptions.readChangeFeed();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
  }

  /**
   * Read schema for the scan: data columns followed by partition columns, with CDC columns appended
   * for CDC reads.
   */
  @Override
  public StructType readSchema() {
    final List<StructField> fields =
        new ArrayList<>(readDataSchema.fields().length + partitionSchema.fields().length);
    Collections.addAll(fields, readDataSchema.fields());
    Collections.addAll(fields, partitionSchema.fields());
    StructType schema = new StructType(fields.toArray(new StructField[0]));
    return isCDCRead ? CDCSchemaContext.appendCDCColumns(schema) : schema;
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

  @Override
  public Batch toBatch() {
    if (isCDCRead) {
      throw new UnsupportedOperationException(
          "Batch reads with CDC (readChangeFeed / readChangeData) are not supported in the V2 "
              + "connector. Either remove the CDC read option or use a streaming read.");
    }
    ensurePlanned();
    return new SparkBatch(
        initialSnapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        partitionedFiles,
        pushedToKernelFilters,
        dataFilters,
        totalBytes,
        scalaOptions,
        hadoopConf);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    validateStreamingOptions(deltaOptions);
    return new SparkMicroBatchStream(
        snapshotManager,
        // Loads a fresh snapshot as the baseline for schema change detection and table identity
        // checks. SparkScan's initialSnapshot is from analysis time and may be stale by stream
        // start/restart.
        // Matches V1's DeltaDataSource.createSource() behavior.
        snapshotManager.loadLatestSnapshot(),
        hadoopConf,
        SparkSession.active(),
        deltaOptions,
        getTablePath(),
        dataSchema,
        partitionSchema,
        readDataSchema,
        dataFilters != null ? dataFilters : new Filter[0],
        scalaOptions != null ? scalaOptions : scala.collection.immutable.Map$.MODULE$.empty());
  }

  @Override
  public String description() {
    final String pushed =
        Arrays.stream(pushedToKernelFilters)
            .map(Object::toString)
            .collect(Collectors.joining(", "));
    final String data =
        Arrays.stream(dataFilters).map(Object::toString).collect(Collectors.joining(", "));
    return String.format(Locale.ROOT, "PushedFilters: [%s], DataFilters: [%s]", pushed, data);
  }

  @Override
  public Statistics estimateStatistics() {
    ensurePlanned();
    // Capture mutable scan state as final locals so the returned Statistics object reflects a
    // consistent snapshot. A subsequent filter() call mutates rowCountKnown and totalRows
    // (see ensurePlanned(runtimePredicates)), and we don't want those mutations to leak into a
    // Statistics instance the caller is still holding.
    final long plannedBytes = estimatedSizeInBytes;
    final boolean rowCountKnownSnapshot = rowCountKnown;
    final long totalRowsSnapshot = totalRows;

    // When catalog stats are available and CBO is enabled, combine table-level stats
    // (for columnStats) with planned file stats (for sizeInBytes and numRows).
    // This mirrors V1's LogicalRelation.computeStats() which gates column stats on
    // conf.cboEnabled || conf.planStatsEnabled.
    if (arePlanStatsEnabled() && catalogStats.isPresent()) {
      final Statistics stats = catalogStats.get();
      return new Statistics() {
        @Override
        public OptionalLong sizeInBytes() {
          // Planned file size is authoritative (even if 0 for an empty table)
          return OptionalLong.of(plannedBytes);
        }

        @Override
        public OptionalLong numRows() {
          // Prefer catalog stats when available: ANALYZE TABLE counts are typically fresh
          // enough for the optimizer, and using them lets us skip per-file stats JSON parsing
          // during planning. Fall back to per-file row count only when the catalog lacks it.
          if (stats.numRows().isPresent()) {
            return stats.numRows();
          }
          return rowCountKnownSnapshot ? OptionalLong.of(totalRowsSnapshot) : OptionalLong.empty();
        }

        @Override
        public Map<NamedReference, ColumnStatistics> columnStats() {
          // TODO: After partition pruning, column stats (e.g. min, max, nullCount,
          //  distinctCount) could be tightened based on the pruned file-level stats.
          return stats.columnStats();
        }
      };
    }

    // No catalog stats available or CBO disabled — return stats from planned files only
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.of(plannedBytes);
      }

      @Override
      public OptionalLong numRows() {
        return rowCountKnownSnapshot ? OptionalLong.of(totalRowsSnapshot) : OptionalLong.empty();
      }
    };
  }

  /**
   * Computes the estimated size in bytes accounting for column projection.
   *
   * <p>This mirrors what {@code SizeInBytesOnlyStatsPlanVisitor.visitUnaryNode} (from Spark code)
   * would compute for a {@code Project} over a {@code LogicalRelation}: {@code sizeInBytes =
   * childSizeInBytes * outputRowSize / childRowSize}
   *
   * <p>Where:
   *
   * <ul>
   *   <li><b>childRowSize</b> = {@code ROW_OVERHEAD + dataSchema + partitionSchema} (equivalent to
   *       LogicalRelation output)
   *   <li><b>outputRowSize</b> = {@code ROW_OVERHEAD + readDataSchema + partitionSchema}
   *       (equivalent to Project output)
   * </ul>
   *
   * <p>When catalog column stats are available, uses per-column {@code avgLen} instead of {@code
   * defaultSize()} for more accurate size estimation, mirroring {@code
   * EstimationUtils.getSizePerRow()} behavior.
   *
   * <p>This provides consistent statistics with the v1 code path (LogicalRelation + visitUnaryNode
   * from Spark code directory).
   *
   * @param totalBytes the total size in bytes of the planned files (raw physical size)
   * @return the estimated size in bytes after accounting for column projection
   */
  private long computeEstimatedSizeWithColumnProjection(long totalBytes) {
    if (totalBytes <= 0) {
      return totalBytes;
    }

    // Row overhead constant, matching EstimationUtils.getSizePerRow (from Spark)
    final int ROW_OVERHEAD = 8;

    // Use avgLen from catalog column stats when available for more accurate estimation
    Map<String, OptionalLong> avgLenByColumn = getAvgLenByColumn();

    final long fullSchemaRowSize =
        ROW_OVERHEAD
            + getSchemaSize(dataSchema, avgLenByColumn)
            + getSchemaSize(partitionSchema, avgLenByColumn);
    final long outputRowSize = ROW_OVERHEAD + getSchemaSize(readSchema(), avgLenByColumn);

    long estimatedBytes = (totalBytes * outputRowSize) / fullSchemaRowSize;

    return Math.max(1L, estimatedBytes);
  }

  /**
   * Returns a map of column name to avgLen from catalog column stats, used to improve size
   * estimation accuracy when catalog stats are available.
   */
  private Map<String, OptionalLong> getAvgLenByColumn() {
    if (!catalogStats.isPresent()) {
      return Collections.emptyMap();
    }
    Map<NamedReference, ColumnStatistics> colStats = catalogStats.get().columnStats();
    if (colStats.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, OptionalLong> result = new HashMap<>();
    for (Map.Entry<NamedReference, ColumnStatistics> entry : colStats.entrySet()) {
      result.put(entry.getKey().fieldNames()[0], entry.getValue().avgLen());
    }
    return result;
  }

  /**
   * Computes the estimated in-memory size for a schema, using avgLen from catalog stats when
   * available, falling back to defaultSize(). Mirrors EstimationUtils.getSizePerRow(). For
   * StringType columns with avgLen, adds UTF8String overhead (base + offset + numBytes = 12 bytes).
   */
  private static long getSchemaSize(StructType schema, Map<String, OptionalLong> avgLenByColumn) {
    long size = 0;
    for (StructField field : schema.fields()) {
      OptionalLong avgLen = avgLenByColumn.getOrDefault(field.name(), OptionalLong.empty());
      if (avgLen.isPresent()) {
        if (field.dataType() instanceof StringType) {
          // UTF8String: base + offset + numBytes (matching EstimationUtils.getSizePerRow)
          size += avgLen.getAsLong() + 8 + 4;
        } else {
          size += avgLen.getAsLong();
        }
      } else {
        size += field.dataType().defaultSize();
      }
    }
    return size;
  }

  /**
   * Get the table path from the scan state.
   *
   * @return the table path with trailing slash
   */
  public String getTablePath() {
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final Row scanState = kernelScan.getScanState(tableEngine);
    final String tableRoot = ScanStateRow.getTableRoot(scanState).toUri().toString();
    return tableRoot.endsWith("/") ? tableRoot : tableRoot + "/";
  }

  /**
   * Plan the files to scan by materializing {@link PartitionedFile}s and aggregating size stats.
   * Ensures all iterators are closed to avoid resource leaks.
   */
  private void planScanFiles() {
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final String tablePath = getTablePath();
    // TODO: Promote getScanFiles(Engine, boolean includeStats) to the public Scan interface to
    // avoid coupling to the kernel-internal ScanImpl class. Until that API is available, this
    // instanceof check is the only way to request per-file statistics from the kernel.
    //
    // Only parse stats JSON when all of:
    //  - the optimizer will use numRows (CBO or planStats enabled), matching V1's behavior
    //    (LogicalRelation.computeStats())
    //  - the catalog does not already provide numRows (otherwise catalog value is preferred in
    //    estimateStatistics(), so per-file parsing would be wasted work)
    //  - the kernel scan is ScanImpl (the only path that supports includeStats)
    final boolean includeStats =
        kernelScan instanceof ScanImpl && arePlanStatsEnabled() && !catalogHasNumRows();
    final Iterator<FilteredColumnarBatch> scanFileBatches;
    if (includeStats) {
      scanFileBatches = ((ScanImpl) kernelScan).getScanFiles(tableEngine, true /* includeStats */);
      rowCountKnown = true; // assume all files have stats; set to false on first miss
    } else {
      rowCountKnown = false;
      scanFileBatches = kernelScan.getScanFiles(tableEngine);
    }

    final String[] locations = new String[0];
    final scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        scala.collection.immutable.Map$.MODULE$.empty();

    while (scanFileBatches.hasNext()) {
      final FilteredColumnarBatch batch = scanFileBatches.next();

      try (CloseableIterator<Row> addFileRowIter = batch.getRows()) {
        while (addFileRowIter.hasNext()) {
          final Row row = addFileRowIter.next();
          final AddFile addFile = new AddFile(row.getStruct(0));

          final PartitionedFile partitionedFile =
              PartitionUtils.buildPartitionedFile(addFile, partitionSchema, tablePath, zoneId);

          totalBytes += addFile.getSize();
          partitionedFiles.add(partitionedFile);

          if (rowCountKnown) {
            Optional<Long> numRecords = addFile.getNumRecords();
            if (numRecords.isPresent()) {
              totalRows += numRecords.get();
            } else {
              // This file has no numRecords — row count is unknowable for the whole scan.
              // Clear partial state and stop accumulating for all subsequent files.
              rowCountKnown = false;
              totalRows = 0;
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Pre-compute estimated size accounting for column projection
    estimatedSizeInBytes = computeEstimatedSizeWithColumnProjection(totalBytes);
  }

  /**
   * Ensure the scan is planned exactly once in a thread-safe manner, optionally applying runtime
   * filters.
   */
  private synchronized void ensurePlanned(List<RuntimePredicate> runtimePredicates) {
    // First, ensure planning is done
    if (!planned) {
      planScanFiles();
      planned = true;
    }

    // Then apply runtime predicates if provided
    if (runtimePredicates != null && !runtimePredicates.isEmpty()) {
      // Record the applied predicates for equals/hashCode comparison
      for (RuntimePredicate filter : runtimePredicates) {
        appliedRuntimePredicates.add(filter.predicate);
      }

      List<PartitionedFile> runtimeFilteredPartitionedFiles = new ArrayList<>();
      for (PartitionedFile pf : this.partitionedFiles) {
        InternalRow partitionValues = pf.partitionValues();
        boolean allMatch =
            runtimePredicates.stream()
                .allMatch(predicate -> predicate.evaluator.eval(partitionValues));
        if (allMatch) {
          runtimeFilteredPartitionedFiles.add(pf);
        }
      }

      // Update partitionedFiles, totalBytes, totalRows, and estimatedSizeInBytes if any partition
      // is filtered out
      if (runtimeFilteredPartitionedFiles.size() < this.partitionedFiles.size()) {
        this.partitionedFiles = runtimeFilteredPartitionedFiles;
        this.totalBytes =
            runtimeFilteredPartitionedFiles.stream().mapToLong(PartitionedFile::fileSize).sum();
        this.estimatedSizeInBytes = computeEstimatedSizeWithColumnProjection(this.totalBytes);
        // Per-file row counts are not retained, so we cannot recompute totalRows over the
        // filtered subset. Invalidate rowCountKnown so numRows() returns OptionalLong.empty()
        // rather than a stale pre-filter count. Retaining per-file counts just for this case
        // would cost O(n) memory on every scan; the trade-off is losing numRows after runtime
        // partition filtering fires.
        rowCountKnown = false;
        totalRows = 0L;
      }
    }
  }

  /** Ensure the scan is planned exactly once in a thread-safe manner. */
  private void ensurePlanned() {
    // Pass null to indicate no runtime predicate should be applied - just perform the scan planning
    ensurePlanned(null);
  }

  public StructType getDataSchema() {
    return dataSchema;
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

  @Override
  public NamedReference[] filterAttributes() {
    return Arrays.stream(partitionSchema.fields())
        .map(field -> FieldReference.column(field.name()))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(org.apache.spark.sql.connector.expressions.filter.Predicate[] predicates) {

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
   * Returns whether plan-time statistics should be collected and reported. Matches V1 behavior:
   * {@code LogicalRelation.computeStats()} only surfaces stats when {@code spark.sql.cbo.enabled}
   * or {@code spark.sql.cbo.planStats.enabled} is true.
   *
   * <p>Despite the row-count-flavored framing in V1, this gate controls multiple things in the V2
   * scan path:
   *
   * <ul>
   *   <li>whether {@code numRows()} is reported in {@link #estimateStatistics()}
   *   <li>whether the catalog-stats branch is entered (which also governs {@code columnStats}
   *       propagation from the catalog)
   *   <li>whether per-file stats JSON is parsed in {@link #planScanFiles()} (gated together with
   *       {@code !catalogHasNumRows()} to avoid wasted parsing when the catalog already has it)
   * </ul>
   *
   * Future readers touching any of these paths should treat this helper as load-bearing.
   */
  private boolean arePlanStatsEnabled() {
    return sqlConf.cboEnabled() || sqlConf.planStatsEnabled();
  }

  /** Returns whether the catalog-provided statistics include a numRows value. */
  private boolean catalogHasNumRows() {
    return catalogStats.isPresent() && catalogStats.get().numRows().isPresent();
  }

  /**
   * Validates that unsupported streaming options are not used. Uses a block list approach - only
   * blocks known DeltaOptions that are unsupported, allowing user-defined custom options to pass
   * through.
   *
   * <p>Note: DeltaOptions internally uses CaseInsensitiveMap, which preserves the original key
   * casing but performs case-insensitive lookups.
   *
   * @param deltaOptions the DeltaOptions to validate
   * @throws UnsupportedOperationException if unsupported options are found
   */
  static void validateStreamingOptions(DeltaOptions deltaOptions) {
    List<String> unsupportedOptions = new ArrayList<>();
    scala.collection.Iterator<String> keysIterator = deltaOptions.options().keysIterator();

    while (keysIterator.hasNext()) {
      String key = keysIterator.next();
      // DeltaOptions uses CaseInsensitiveMap with keys already lowercased.
      if (UNSUPPORTED_STREAMING_OPTIONS.contains(key)) {
        unsupportedOptions.add(key);
      }
    }

    if (!unsupportedOptions.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "The following streaming options are not supported: [%s]. "
                  + "Supported options are: [%s].",
              String.join(", ", unsupportedOptions),
              String.join(", ", SUPPORTED_STREAMING_OPTIONS)));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkScan that = (SparkScan) o;
    return Objects.equals(initialSnapshot.getPath(), that.initialSnapshot.getPath())
        && initialSnapshot.getVersion() == that.initialSnapshot.getVersion()
        && Objects.equals(dataSchema, that.dataSchema)
        && Objects.equals(partitionSchema, that.partitionSchema)
        && Objects.equals(readDataSchema, that.readDataSchema)
        && Objects.equals(pushedToKernelFiltersSet, that.pushedToKernelFiltersSet)
        && Objects.equals(dataFiltersSet, that.dataFiltersSet)
        // ignoring kernelScan because it is derived from Snapshot which is created from tablePath,
        // with pushed down filters that are also recorded in `pushedToKernelFilters`
        && Objects.equals(options, that.options)
        && Objects.equals(appliedRuntimePredicates, that.appliedRuntimePredicates)
        && Objects.equals(catalogStats, that.catalogStats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        catalogStats,
        initialSnapshot.getPath(),
        initialSnapshot.getVersion(),
        dataSchema,
        partitionSchema,
        readDataSchema,
        options,
        appliedRuntimePredicates,
        pushedToKernelFiltersSet,
        dataFiltersSet);
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
