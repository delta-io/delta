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
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.read.SparkReaderFactory;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorReadFunction;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorUtils;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
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
    scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        DeletionVectorUtils.buildMetadata(addFile.getDeletionVector());

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
   *   <li>Augments the read schema to include __delta_internal_is_row_deleted column
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
    Protocol protocol = snapshotImpl.getProtocol();
    Metadata metadata = snapshotImpl.getMetadata();
    String tablePath = snapshotImpl.getDataPath().toUri().toString();

    // Check if table supports deletion vectors
    boolean tableSupportsDV = DeletionVectorUtils.isReadable(protocol, metadata);

    // Augment schema with DV column if needed
    StructType augmentedReadDataSchema = readDataSchema;
    if (tableSupportsDV) {
      augmentedReadDataSchema = DeletionVectorUtils.augmentSchema(readDataSchema);
    }

    // Phase 1: Disable vectorized reader when DV is enabled to simplify implementation
    // Phase 2 will add vectorized reader support with ColumnarBatch filtering
    boolean enableVectorizedReader =
        !tableSupportsDV && ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithVectorizedReading =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    // Phase 1: Disable optimizations when DV is enabled (no file splitting)
    // This means DeltaParquetFileFormat uses internal row index counter
    // Phase 3 will set optimizationsEnabled = true for _metadata.row_index support
    boolean optimizationsEnabled = !tableSupportsDV;

    // Use DeltaParquetFileFormatV2 to support column mapping and other Delta features
    // For Phase 1: explicitly set useMetadataRowIndex = false (no _metadata.row_index support)
    Option<Boolean> useMetadataRowIndex =
        tableSupportsDV ? Option.apply(Boolean.FALSE) : Option.empty();
    DeltaParquetFileFormatV2 deltaFormat =
        new DeltaParquetFileFormatV2(
            protocol,
            metadata,
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            optimizationsEnabled,
            Option.apply(tablePath),
            /* isCDCRead */ false,
            /* useMetadataRowIndexOpt */ useMetadataRowIndex);

    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        deltaFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            dataSchema,
            partitionSchema,
            augmentedReadDataSchema,
            CollectionConverters.asScala(Arrays.asList(dataFilters)).toSeq(),
            optionsWithVectorizedReading,
            hadoopConf);

    // Wrap reader to filter deleted rows and remove internal columns if DV is enabled
    if (tableSupportsDV) {
      int dvColumnIndex =
          augmentedReadDataSchema.fieldIndex(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME());
      // Full input schema: data columns (with DV column) + partition columns
      StructType fullInputSchema = augmentedReadDataSchema.merge(partitionSchema);
      readFunc = DeletionVectorReadFunction.wrap(readFunc, dvColumnIndex, fullInputSchema);
    }

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }
}
