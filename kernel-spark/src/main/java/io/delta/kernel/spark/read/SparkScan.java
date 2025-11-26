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

import static io.delta.kernel.spark.utils.ExpressionUtils.dsv2PredicateToCatalystExpression;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.InterpretedPredicate;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

/** Spark DSV2 Scan implementation backed by Delta Kernel. */
public class SparkScan implements Scan, SupportsReportStatistics, SupportsRuntimeV2Filtering {

  private final DeltaSnapshotManager snapshotManager;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final io.delta.kernel.Scan kernelScan;
  private final Snapshot initialSnapshot;
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
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      io.delta.kernel.Scan kernelScan,
      Snapshot initialSnapshot,
      CaseInsensitiveStringMap options) {

    this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager is null");
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.pushedToKernelFilters =
        pushedToKernelFilters == null ? new Predicate[0] : pushedToKernelFilters.clone();
    this.dataFilters = dataFilters == null ? new Filter[0] : dataFilters.clone();
    this.kernelScan = Objects.requireNonNull(kernelScan, "kernelScan is null");
    this.initialSnapshot = Objects.requireNonNull(initialSnapshot, "initialSnapshot is null");
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
        getTablePath(),
        dataSchema,
        partitionSchema,
        readDataSchema,
        partitionedFiles,
        pushedToKernelFilters,
        dataFilters,
        totalBytes,
        scalaOptions,
        hadoopConf,
        initialSnapshot);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    DeltaOptions deltaOptions = new DeltaOptions(scalaOptions, sqlConf);
    return new SparkMicroBatchStream(
        snapshotManager, hadoopConf, SparkSession.active(), deltaOptions);
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
   * Build the partition {@link InternalRow} from kernel partition values by casting them to the
   * desired Spark types using the session time zone for temporal types.
   */
  private InternalRow getPartitionRow(MapValue partitionValues) {
    final int numPartCols = partitionSchema.fields().length;
    assert partitionValues.getSize() == numPartCols
        : String.format(
            Locale.ROOT,
            "Partition values size from add file %d != partition columns size %d",
            partitionValues.getSize(),
            numPartCols);

    final Object[] values = new Object[numPartCols];

    // Build field name -> index map once
    final Map<String, Integer> fieldIndex = new HashMap<>(numPartCols);
    for (int i = 0; i < numPartCols; i++) {
      fieldIndex.put(partitionSchema.fields()[i].name(), i);
      values[i] = null;
    }

    // Fill values in a single pass over partitionValues
    for (int idx = 0; idx < partitionValues.getSize(); idx++) {
      final String key = partitionValues.getKeys().getString(idx);
      final String strVal = partitionValues.getValues().getString(idx);
      final Integer pos = fieldIndex.get(key);
      if (pos != null) {
        final StructField field = partitionSchema.fields()[pos];
        values[pos] =
            (strVal == null)
                ? null
                : PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
      }
    }
    return InternalRow.fromSeq(
        JavaConverters.asScalaIterator(Arrays.asList(values).iterator()).toSeq());
  }

  /**
   * Plan the files to scan by materializing {@link PartitionedFile}s and aggregating size stats.
   * Ensures all iterators are closed to avoid resource leaks.
   */
  private void planScanFiles() {
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final String tablePath = getTablePath();
    final Iterator<FilteredColumnarBatch> scanFileBatches = kernelScan.getScanFiles(tableEngine);

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
              new PartitionedFile(
                  getPartitionRow(addFile.getPartitionValues()),
                  SparkPath.fromUrlString(tablePath + addFile.getPath()),
                  0L,
                  addFile.getSize(),
                  locations,
                  addFile.getModificationTime(),
                  addFile.getSize(),
                  otherConstantMetadataColumnValues);

          totalBytes += addFile.getSize();
          partitionedFiles.add(partitionedFile);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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
}
