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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.spark.internal.v2.read.ColumnReorderReadFunction;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.read.SparkReaderFactory;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorReadFunction;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorSchemaContext;
import io.delta.spark.internal.v2.read.rowtracking.RowTrackingReadFunction;
import io.delta.spark.internal.v2.read.rowtracking.RowTrackingSchemaContext;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;

/** Utility class for partition-related operations shared across Delta Kernel Spark components. */
public class PartitionUtils {

  private PartitionUtils() {}

  /**
   * Returns whether the given snapshot's table supports deletion vectors. A table supports DVs when
   * its protocol includes the {@link TableFeatures#DELETION_VECTORS_RW_FEATURE} and the table
   * format is Parquet.
   */
  public static boolean tableSupportsDeletionVectors(Snapshot snapshot) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    Protocol protocol = snapshotImpl.getProtocol();
    Metadata metadata = snapshotImpl.getMetadata();
    return protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
        && "parquet".equalsIgnoreCase(metadata.getFormat().getProvider());
  }

  /**
   * Calculate the maximum split bytes for file partitioning, considering total bytes and file
   * count. This is used for optimal file splitting in both batch and streaming read.
   */
  public static long calculateMaxSplitBytes(
      SparkSession sparkSession, long totalBytes, int fileCount, SQLConf sqlConf) {
    long defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes();
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    Option<Object> minPartitionNumOption = sqlConf.filesMinPartitionNum();

    int minPartitionNum =
        minPartitionNumOption.isDefined()
            ? ((Number) minPartitionNumOption.get()).intValue()
            : sqlConf
                .getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM())
                .getOrElse(() -> sparkSession.sparkContext().defaultParallelism());
    if (minPartitionNum <= 0) {
      minPartitionNum = 1;
    }

    long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
    long bytesPerCore = calculatedTotalBytes / minPartitionNum;

    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }

  /**
   * Build the partition {@link InternalRow} from kernel partition values by casting them to the
   * desired Spark types using the session time zone for temporal types.
   *
   * <p>Note: Partition values in AddFile use physical column names as keys when column mapping is
   * enabled. This method uses DeltaColumnMapping.getPhysicalName to map from logical schema fields
   * to physical partition value keys.
   *
   * @implNote The returned {@link InternalRow} is a {@code GenericInternalRow} (via {@code
   *     InternalRow.fromSeq}), which has value-based {@code equals()}/{@code hashCode()}. Callers
   *     such as {@code SparkBatch.planPartitionedInputPartitions} rely on this for grouping files
   *     by partition key. Changing the return type to a different InternalRow subtype (e.g. {@code
   *     UnsafeRow}) may break that contract.
   */
  public static InternalRow getPartitionRow(
      MapValue partitionValues, StructType partitionSchema, ZoneId zoneId) {
    final int numPartCols = partitionSchema.fields().length;
    assert partitionValues.getSize() == numPartCols
        : String.format(
            java.util.Locale.ROOT,
            "Partition values size from add file %d != partition columns size %d",
            partitionValues.getSize(),
            numPartCols);

    final Object[] values = new Object[numPartCols];

    // Build physical name -> index map once
    // Partition values use physical names as keys when column mapping is enabled
    final Map<String, Integer> physicalNameToIndex = new HashMap<>(numPartCols);
    for (int i = 0; i < numPartCols; i++) {
      StructField field = partitionSchema.fields()[i];
      String physicalName = DeltaColumnMapping.getPhysicalName(field);
      physicalNameToIndex.put(physicalName, i);
      values[i] = null;
    }

    // Fill values in a single pass over partitionValues
    for (int idx = 0; idx < partitionValues.getSize(); idx++) {
      final String key = partitionValues.getKeys().getString(idx);
      final String strVal = partitionValues.getValues().getString(idx);
      final Integer pos = physicalNameToIndex.get(key);
      if (pos != null) {
        final StructField field = partitionSchema.fields()[pos];
        values[pos] =
            (strVal == null)
                ? null
                : PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
      }
    }
    return InternalRow.fromSeq(
        CollectionConverters.asScala(Arrays.asList(values).iterator()).toSeq());
  }

  /**
   * Build a PartitionedFile from an AddFile with the given partition schema and table path.
   *
   * @param addFile The AddFile to convert
   * @param partitionSchema The partition schema for parsing partition values
   * @param tablePath The table path
   * @param zoneId The timezone for temporal partition values
   * @return A PartitionedFile ready for Spark execution
   */
  public static PartitionedFile buildPartitionedFile(
      AddFile addFile, StructType partitionSchema, String tablePath, ZoneId zoneId) {
    InternalRow partitionRow =
        getPartitionRow(addFile.getPartitionValues(), partitionSchema, zoneId);

    // Preferred node locations are not used.
    String[] preferredLocations = new String[0];

    // Build metadata map with DV info if present
    scala.collection.immutable.Map<String, Object> deletionVectorMetadata =
        buildDvMetadata(addFile.getDeletionVector());
    scala.collection.immutable.Map<String, Object> rowTrackingMetadata =
        buildRowTrackingMetadata(addFile.getBaseRowId(), addFile.getDefaultRowCommitVersion());
    scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        deletionVectorMetadata;
    for (Map.Entry<String, Object> entry :
        CollectionConverters.asJava(rowTrackingMetadata).entrySet()) {
      otherConstantMetadataColumnValues =
          otherConstantMetadataColumnValues.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }

    return new PartitionedFile(
        partitionRow,
        SparkPath.fromUrlString(new Path(tablePath, addFile.getPath()).toString()),
        /* start= */ 0L,
        /* length= */ addFile.getSize(),
        preferredLocations,
        addFile.getModificationTime(),
        /* fileSize= */ addFile.getSize(),
        otherConstantMetadataColumnValues);
  }

  /**
   * Create a PartitionReaderFactory for reading Parquet files with Delta-specific features.
   *
   * <p>Uses DeltaParquetFileFormatV2 which supports column mapping, deletion vectors, and other
   * Delta features through the ProtocolMetadataAdapterV2.
   *
   * <p>For tables with deletion vectors enabled, this method:
   *
   * <ol>
   *   <li>Adds __delta_internal_is_row_deleted column to read schema
   *   <li>Creates a reader that generates the is_row_deleted column using DV bitmap
   *   <li>Wraps the reader to filter out deleted rows and remove internal columns
   * </ol>
   *
   * @param snapshot The Delta table snapshot containing protocol, metadata, and table path
   */
  public static PartitionReaderFactory createDeltaParquetReaderFactory(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    // Use Path.toString() instead of toUri().toString() to avoid URL encoding issues.
    // toUri().toString() encodes special characters (e.g., space -> %20), which causes
    // DV file path resolution failures.
    String tablePath = snapshotImpl.getDataPath().toString();

    // Preserve the caller-provided readDataSchema (pre-DV/RT augmentation) for the final
    // column-reorder wrapper below. DV/RT inject + strip internal columns; from the outside,
    // the reader chain still produces one StructField per column in (readDataSchema,partitions).
    final StructType originalReadDataSchema = readDataSchema;

    boolean metadataColumnRequested =
        Arrays.stream(readDataSchema.fields())
            .anyMatch(field -> FileFormat$.MODULE$.METADATA_NAME().equals(field.name()));
    Optional<RowTrackingSchemaContext> rowTrackingSchemaContext = Optional.empty();
    if (metadataColumnRequested) {
      RowTrackingSchemaContext context =
          new RowTrackingSchemaContext(readDataSchema, snapshotImpl.getMetadata(), partitionSchema);
      rowTrackingSchemaContext = Optional.of(context);
      readDataSchema = context.getSchemaWithRowTrackingColumns();
    }

    // Create DV schema context if table supports deletion vectors
    Optional<DeletionVectorSchemaContext> dvSchemaContext =
        tableSupportsDeletionVectors(snapshot)
            ? Optional.of(new DeletionVectorSchemaContext(readDataSchema, partitionSchema))
            : Optional.empty();
    if (dvSchemaContext.isPresent()) {
      readDataSchema = dvSchemaContext.get().getSchemaWithDvColumn();
    }

    boolean enableVectorizedReader =
        // Disabled because RowTrackingReadFunction operates on individual InternalRows to compute
        // row_id from _tmp_metadata_row_index and coalesce with materialized columns.
        !rowTrackingSchemaContext.isPresent()
            && ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithVectorizedReading =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    // TODO(https://github.com/delta-io/delta/issues/5859): Enable file splitting for DV tables
    boolean optimizationsEnabled = !dvSchemaContext.isPresent();

    // TODO(https://github.com/delta-io/delta/issues/5859): Support _metadata.row_index for DV
    Option<Boolean> useMetadataRowIndex =
        dvSchemaContext.isPresent() ? Option.apply(Boolean.FALSE) : Option.empty();
    DeltaParquetFileFormatV2 deltaFormat =
        createDeltaParquetFileFormat(
            snapshot, tablePath, optimizationsEnabled, useMetadataRowIndex);

    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        deltaFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            dataSchema,
            partitionSchema,
            readDataSchema,
            CollectionConverters.asScala(Arrays.asList(dataFilters)).toSeq(),
            optionsWithVectorizedReading,
            hadoopConf);

    // Wrap reader to filter deleted rows and remove internal columns if DV is enabled.
    // DV must be the inner wrapper so it sees the raw reader output with the DV column
    // at its expected index, before row tracking changes the column layout.
    if (dvSchemaContext.isPresent()) {
      readFunc =
          DeletionVectorReadFunction.wrap(readFunc, dvSchemaContext.get(), enableVectorizedReader);
    }

    // Wrap reader to add rowTracking metadata.
    // RT is the outer wrapper: _tmp_metadata_row_index values are per-row physical positions
    // generated by the Parquet reader, so they remain correct after DV filtering.
    if (rowTrackingSchemaContext.isPresent()) {
      readFunc = RowTrackingReadFunction.wrap(readFunc, rowTrackingSchemaContext.get());
    }

    // Reorder output columns from (data ++ partitions) to the table's DDL order. Required
    // because Spark's streaming plan binds output attributes to SparkTable.schema() (DDL
    // order); without this reorder a downstream ordinal-based consumer can read a
    // type-mismatched ColumnVector and NPE. No-op when partitions are already at the end.
    // Uses the ORIGINAL readDataSchema (pre-DV/RT augmentation): DV strips its internal
    // column and RT inserts _metadata between data and partitions, so what reaches this
    // wrapper is logically (originalReadDataSchema ++ partitionSchema).
    readFunc =
        ColumnReorderReadFunction.wrap(
            readFunc,
            enableVectorizedReader,
            readerOutputSchema(originalReadDataSchema, partitionSchema),
            ddlOrderedOutputSchema(snapshot, originalReadDataSchema, partitionSchema));

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }

  /**
   * The output column layout that the underlying reader (before the reorder wrapper) produces: data
   * columns followed by partition columns.
   */
  private static StructType readerOutputSchema(
      StructType readDataSchema, StructType partitionSchema) {
    List<StructField> fields =
        new ArrayList<>(readDataSchema.fields().length + partitionSchema.fields().length);
    for (StructField f : readDataSchema.fields()) {
      fields.add(f);
    }
    for (StructField f : partitionSchema.fields()) {
      fields.add(f);
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  /**
   * The projection of {@code readDataSchema ∪ partitionSchema} in the table's DDL order (partition
   * columns interleaved at their declared positions). Any field in {@code readDataSchema} that is
   * not part of the table's persisted schema (e.g. Spark's synthetic {@code _metadata} column) is
   * appended at the end in insertion order.
   */
  public static StructType ddlOrderedOutputSchema(
      Snapshot snapshot, StructType readDataSchema, StructType partitionSchema) {
    if (partitionSchema.fields().length == 0) {
      // No partition columns — reader output already matches DDL order for included fields.
      return readDataSchema;
    }
    StructType rawSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
            snapshot.getSchema());
    LinkedHashMap<String, StructField> fieldsByName =
        new LinkedHashMap<>(readDataSchema.fields().length + partitionSchema.fields().length);
    for (StructField f : readDataSchema.fields()) {
      fieldsByName.put(f.name(), f);
    }
    for (StructField f : partitionSchema.fields()) {
      fieldsByName.put(f.name(), f);
    }
    List<StructField> ordered = new ArrayList<>(fieldsByName.size());
    for (StructField f : rawSchema.fields()) {
      StructField projected = fieldsByName.remove(f.name());
      if (projected != null) {
        ordered.add(projected);
      }
    }
    // Append leftover fields (not in the persisted schema) preserving insertion order.
    ordered.addAll(fieldsByName.values());
    return new StructType(ordered.toArray(new StructField[0]));
  }

  /**
   * Creates a {@link DeltaParquetFileFormatV2} from a snapshot. Shared between the read path
   * ({@link #createDeltaParquetReaderFactory}) and the write path (BatchWrite).
   *
   * @param snapshot the Delta table snapshot (must be a SnapshotImpl)
   * @param tablePath the table root path
   * @param optimizationsEnabled whether to enable file splitting and predicate pushdown
   * @param useMetadataRowIndex explicit control over _metadata.row_index for DV filtering
   */
  public static DeltaParquetFileFormatV2 createDeltaParquetFileFormat(
      Snapshot snapshot,
      String tablePath,
      boolean optimizationsEnabled,
      Option<Boolean> useMetadataRowIndex) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    return new DeltaParquetFileFormatV2(
        snapshotImpl.getProtocol(),
        snapshotImpl.getMetadata(),
        /* nullableRowTrackingConstantFields */ false,
        /* nullableRowTrackingGeneratedFields */ false,
        optimizationsEnabled,
        Option.apply(tablePath),
        /* isCDCRead */ false,
        useMetadataRowIndex);
  }

  /**
   * Build metadata map for PartitionedFile containing DV descriptor if present.
   *
   * <p>The metadata is used by DeltaParquetFileFormat to generate the is_row_deleted column.
   */
  private static scala.collection.immutable.Map<String, Object> buildDvMetadata(
      Optional<DeletionVectorDescriptor> dvOpt) {
    Map<String, Object> metadata = new HashMap<>();
    if (dvOpt.isPresent()) {
      metadata.put(
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED(),
          dvOpt.get().serializeToBase64());
      metadata.put(
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE(), RowIndexFilterType.IF_CONTAINED);
    }
    return scala.collection.immutable.Map$.MODULE$.from(CollectionConverters.asScala(metadata));
  }

  /**
   * Build metadata map for PartitionedFile containing row tracking descriptor if present.
   *
   * <p>The metadata is used by DeltaParquetFileFormat to generate row-tracking values.
   *
   * <p>Both keys must be present together (or both absent) because they represent one row-tracking
   * descriptor contract for a file.
   *
   * @throws IllegalStateException if only one of the two row-tracking constants is provided
   */
  @SuppressWarnings("unchecked")
  private static scala.collection.immutable.Map<String, Object> buildRowTrackingMetadata(
      Optional<Long> baseRowId, Optional<Long> defaultRowCommitVersion) {
    scala.collection.immutable.Map<String, Object> result =
        (scala.collection.immutable.Map<String, Object>)
            (scala.collection.immutable.Map<?, ?>) scala.collection.immutable.Map$.MODULE$.empty();
    if (baseRowId.isPresent() != defaultRowCommitVersion.isPresent()) {
      throw new IllegalStateException(
          "Expected row tracking metadata keys base_row_id and default_row_commit_version to "
              + "be either both set or both unset");
    }
    if (baseRowId.isPresent() && defaultRowCommitVersion.isPresent()) {
      result = result.$plus(new Tuple2<>(RowId$.MODULE$.BASE_ROW_ID(), baseRowId.get()));
      result =
          result.$plus(
              new Tuple2<>(
                  DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(),
                  defaultRowCommitVersion.get()));
    }
    return result;
  }
}
