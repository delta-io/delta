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
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
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
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
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
              DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION(),
              DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION()));

  /**
   * Block list of DeltaOptions that are not supported for streaming in V2 connector. Only
   * startingVersion, maxFilesPerTrigger, and maxBytesPerTrigger are supported. User-defined custom
   * options (not in DeltaOptions) are allowed to pass through.
   */
  private static final Set<String> UNSUPPORTED_STREAMING_OPTIONS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  DeltaOptions.EXCLUDE_REGEX_OPTION().toLowerCase(),
                  DeltaOptions.IGNORE_FILE_DELETION_OPTION().toLowerCase(),
                  DeltaOptions.IGNORE_CHANGES_OPTION().toLowerCase(),
                  DeltaOptions.IGNORE_DELETES_OPTION().toLowerCase(),
                  DeltaOptions.SKIP_CHANGE_COMMITS_OPTION().toLowerCase(),
                  DeltaOptions.FAIL_ON_DATA_LOSS_OPTION().toLowerCase(),
                  DeltaOptions.STARTING_TIMESTAMP_OPTION().toLowerCase(),
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
  private final io.delta.kernel.Scan kernelScan;
  private final Configuration hadoopConf;
  private final CaseInsensitiveStringMap options;
  private final scala.collection.immutable.Map<String, String> scalaOptions;
  private final SQLConf sqlConf;
  private final ZoneId zoneId;

  // Planned input files and stats
  private List<PartitionedFile> partitionedFiles = new ArrayList<>();
  private long totalBytes = 0L;
  private volatile boolean planned = false;

  public SparkScan(
      DeltaSnapshotManager snapshotManager,
      Snapshot initialSnapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      io.delta.kernel.Scan kernelScan,
      CaseInsensitiveStringMap options) {

    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.initialSnapshot = Objects.requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.pushedToKernelFilters =
        pushedToKernelFilters == null ? new Predicate[0] : pushedToKernelFilters.clone();
    this.dataFilters = dataFilters == null ? new Filter[0] : dataFilters.clone();
    this.kernelScan = Objects.requireNonNull(kernelScan, "kernelScan is null");
    this.options = Objects.requireNonNull(options, "options is null");
    this.scalaOptions = ScalaUtils.toScalaMap(options);
    this.hadoopConf = SparkSession.active().sessionState().newHadoopConfWithOptions(scalaOptions);
    this.sqlConf = SQLConf.get();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
  }

  /**
   * Read schema for the scan, which is the projection of data columns followed by partition
   * columns.
   */
  @Override
  public StructType readSchema() {
    final List<StructField> fields =
        new ArrayList<>(readDataSchema.fields().length + partitionSchema.fields().length);
    Collections.addAll(fields, readDataSchema.fields());
    Collections.addAll(fields, partitionSchema.fields());
    return new StructType(fields.toArray(new StructField[0]));
  }

  @Override
  public Batch toBatch() {
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
    DeltaOptions deltaOptions = new DeltaOptions(scalaOptions, sqlConf);
    // Validate streaming options immediately after constructing DeltaOptions
    validateStreamingOptions(deltaOptions);
    return new SparkMicroBatchStream(
        snapshotManager,
        initialSnapshot,
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
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.of(totalBytes);
      }

      @Override
      public OptionalLong numRows() {
        // Row count is unknown at planning time.
        return OptionalLong.empty();
      }
    };
  }

  /**
   * Get the table path from the scan state.
   *
   * @return the table path with trailing slash
   */
  private String getTablePath() {
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final Row scanState = kernelScan.getScanState(tableEngine);
    final String tableRoot = ScanStateRow.getTableRoot(scanState).toUri().toString();
    return tableRoot.endsWith("/") ? tableRoot : tableRoot + "/";
  }

  /**
   * Plan the files to scan by materializing {@link PartitionedFile}s and aggregating size stats.
   * Uses DataFrame-based distributed log replay (V1 algorithm).
   */
  private void planScanFiles() {
    SparkSession spark = SparkSession.active();
    final String tablePath = getTablePath();

    // Get number of partitions from config
    int numPartitions =
        spark
            .conf()
            .getOption("spark.databricks.delta.v2.distributedLogReplay.numPartitions")
            .map(Integer::parseInt)
            .getOrElse(() -> 50);

    // Step 1: Distributed log replay to get all files as DataFrame
    org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> allFiles =
        DistributedLogReplayHelper.stateReconstructionV2(spark, initialSnapshot, numPartitions);

    // Step 2: Collect to driver
    java.util.List<org.apache.spark.sql.Row> fileRows = allFiles.collectAsList();

    // Step 3: Convert DataFrame Rows to PartitionedFiles
    // Each row has schema: add (struct) containing all AddFile fields
    for (org.apache.spark.sql.Row row : fileRows) {
      // Extract add struct from row (V1: stateDS.select(col("add")))
      org.apache.spark.sql.Row addStruct = row.getStruct(0);

      // Extract fields from add struct
      String path = addStruct.getAs("path");
      scala.collection.immutable.Map<String, String> partitionValues =
          addStruct.getAs("partitionValues");
      long size = addStruct.getAs("size");
      long modificationTime = addStruct.getAs("modificationTime");

      // Build partition row from partition values
      InternalRow partitionRow = buildPartitionRowFromMap(partitionValues, partitionSchema);

      // Create PartitionedFile
      final PartitionedFile partitionedFile =
          new PartitionedFile(
              partitionRow,
              org.apache.spark.paths.SparkPath.fromUrlString(
                  new org.apache.hadoop.fs.Path(tablePath, path).toString()),
              /* start= */ 0L,
              /* length= */ size,
              /* locations= */ new String[0],
              modificationTime,
              /* fileSize= */ size,
              scala.collection.immutable.Map$.MODULE$.empty());

      totalBytes += size;
      partitionedFiles.add(partitionedFile);
    }
  }

  /**
   * Build InternalRow from Scala Map<String, String> partition values. Similar to
   * PartitionUtils.getPartitionRow but works with Spark SQL Map.
   */
  private InternalRow buildPartitionRowFromMap(
      scala.collection.immutable.Map<String, String> partitionValues, StructType partitionSchema) {

    final int numPartCols = partitionSchema.fields().length;
    final Object[] values = new Object[numPartCols];

    // Build physical name -> index map
    final Map<String, Integer> nameToIndex = new HashMap<>(numPartCols);
    for (int i = 0; i < numPartCols; i++) {
      StructField field = partitionSchema.fields()[i];
      String physicalName = DeltaColumnMapping.getPhysicalName(field);
      nameToIndex.put(physicalName, i);
      values[i] = null;
    }

    // Convert Scala map to Java map for easier iteration
    if (partitionValues != null) {
      scala.collection.Iterator<scala.Tuple2<String, String>> iter = partitionValues.iterator();
      while (iter.hasNext()) {
        scala.Tuple2<String, String> entry = iter.next();
        String key = entry._1();
        String strVal = entry._2();
        Integer pos = nameToIndex.get(key);
        if (pos != null) {
          StructField field = partitionSchema.fields()[pos];
          values[pos] =
              (strVal == null)
                  ? null
                  : PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
        }
      }
    }

    return InternalRow.fromSeq(
        scala.jdk.javaapi.CollectionConverters.asScala(Arrays.asList(values).iterator()).toSeq());
  }

  /** Ensure the scan is planned exactly once in a thread\-safe manner. */
  private synchronized void ensurePlanned() {
    if (!planned) {
      planScanFiles();
      planned = true;
    }
  }

  StructType getDataSchema() {
    return dataSchema;
  }

  StructType getPartitionSchema() {
    return partitionSchema;
  }

  StructType getReadDataSchema() {
    return readDataSchema;
  }

  CaseInsensitiveStringMap getOptions() {
    return options;
  }

  Configuration getConfiguration() {
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
    List<InterpretedPredicate> evaluators = new ArrayList<>();
    for (org.apache.spark.sql.connector.expressions.filter.Predicate predicate : predicates) {
      // only the predicates on partition columns will be converted
      Optional<Expression> catalystExpr =
          dsv2PredicateToCatalystExpression(predicate, partitionSchema);
      if (catalystExpr.isPresent()) {
        InterpretedPredicate predicateEvaluator =
            org.apache.spark.sql.catalyst.expressions.Predicate.createInterpreted(
                catalystExpr.get());
        evaluators.add(predicateEvaluator);
      }
    }
    if (evaluators.isEmpty()) {
      return;
    }

    // Filter existing partitionedFiles with runtime filter evaluators
    ensurePlanned();
    List<PartitionedFile> runtimeFilteredPartitionedFiles = new ArrayList<>();
    for (PartitionedFile pf : this.partitionedFiles) {
      InternalRow partitionValues = pf.partitionValues();
      boolean allMatch = evaluators.stream().allMatch(evaluator -> evaluator.eval(partitionValues));
      if (allMatch) {
        runtimeFilteredPartitionedFiles.add(pf);
      }
    }

    // Update partitionedFiles and totalBytes, if any partition is filtered out
    if (runtimeFilteredPartitionedFiles.size() < this.partitionedFiles.size()) {
      this.partitionedFiles = runtimeFilteredPartitionedFiles;
      this.totalBytes = this.partitionedFiles.stream().mapToLong(PartitionedFile::fileSize).sum();
    }
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
        && Arrays.equals(pushedToKernelFilters, that.pushedToKernelFilters)
        && Arrays.equals(dataFilters, that.dataFilters)
        // ignoring kernelScan because it is derived from Snapshot which is created from tablePath,
        // with pushed down filters that are also recorded in `pushedToKernelFilters`
        && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            initialSnapshot.getPath(),
            initialSnapshot.getVersion(),
            dataSchema,
            partitionSchema,
            readDataSchema,
            options);
    result = 31 * result + Arrays.hashCode(pushedToKernelFilters);
    result = 31 * result + Arrays.hashCode(dataFilters);
    return result;
  }
}
