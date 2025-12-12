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
package io.delta.kernel.spark.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.spark.read.DeltaParquetFileFormatV2;
import io.delta.kernel.spark.read.SparkReaderFactory;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
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
        buildDVMetadata(addFile.getDeletionVector());

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
   * Build metadata map containing DV descriptor if present.
   *
   * @param dvOpt Optional DeletionVectorDescriptor from Kernel
   * @return Immutable Scala map with DV metadata, or empty map if no DV
   */
  private static scala.collection.immutable.Map<String, Object> buildDVMetadata(
      Optional<DeletionVectorDescriptor> dvOpt) {
    if (!dvOpt.isPresent()) {
      return scala.collection.immutable.Map$.MODULE$.empty();
    }

    DeletionVectorDescriptor kernelDv = dvOpt.get();

    // Convert Kernel DV to Spark DV and serialize to base64
    org.apache.spark.sql.delta.actions.DeletionVectorDescriptor sparkDv =
        convertToSparkDV(kernelDv);
    String dvBase64 = sparkDv.serializeToBase64();

    // Build immutable map with DV metadata using the constants from DeltaParquetFileFormat
    List<Tuple2<String, Object>> entries = new ArrayList<>();
    entries.add(new Tuple2<>(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED(), dvBase64));
    entries.add(
        new Tuple2<>(
            DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE(), RowIndexFilterType.IF_CONTAINED));

    @SuppressWarnings("unchecked")
    scala.collection.immutable.Map<String, Object> dvMetadata =
        (scala.collection.immutable.Map<String, Object>)
            scala.collection.immutable.Map$.MODULE$.apply(
                scala.collection.immutable.Seq$.MODULE$.apply(
                    CollectionConverters.asScala(entries).toSeq()));
    return dvMetadata;
  }

  /**
   * Convert Kernel's DeletionVectorDescriptor to Spark's DeletionVectorDescriptor.
   *
   * @param kernelDv The Kernel DeletionVectorDescriptor
   * @return The equivalent Spark DeletionVectorDescriptor
   */
  private static org.apache.spark.sql.delta.actions.DeletionVectorDescriptor convertToSparkDV(
      DeletionVectorDescriptor kernelDv) {
    scala.Option<Object> offset =
        kernelDv.getOffset().isPresent()
            ? scala.Option.apply((Object) kernelDv.getOffset().get())
            : scala.Option.empty();
    scala.Option<Object> maxRowIndex = scala.Option.empty();

    return new org.apache.spark.sql.delta.actions.DeletionVectorDescriptor(
        kernelDv.getStorageType(),
        kernelDv.getPathOrInlineDv(),
        offset,
        kernelDv.getSizeInBytes(),
        kernelDv.getCardinality(),
        maxRowIndex);
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
    boolean tableSupportsDV = isDeletionVectorReadable(protocol, metadata);

    // Augment schema with DV column if needed
    StructType augmentedReadDataSchema = readDataSchema;
    if (tableSupportsDV) {
      augmentedReadDataSchema = addIsRowDeletedColumn(readDataSchema);
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

    // Phase 1: Always use optimizationsEnabled = false when DV is enabled
    // This means DeltaParquetFileFormat uses internal row index counter (no file splitting)
    // Phase 2/3 will set optimizationsEnabled = true for _metadata.row_index support
    boolean optimizationsEnabled = !tableSupportsDV;

    // Use DeltaParquetFileFormatV2 to support column mapping and other Delta features
    // For Phase 1: explicitly set useMetadataRowIndex = false (no _metadata.row_index support)
    Option<Boolean> useMetadataRowIndex = tableSupportsDV ? Option.apply(false) : Option.empty();
    DeltaParquetFileFormatV2 deltaFormat =
        new DeltaParquetFileFormatV2(
            protocol,
            metadata,
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            optimizationsEnabled,
            Option.apply(tablePath),
            /* isCDCRead */ false,
            useMetadataRowIndex);

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
      int dvColumnIndex = augmentedReadDataSchema.fieldIndex(IS_ROW_DELETED_COLUMN_NAME);
      int totalColumns = augmentedReadDataSchema.fields().length + partitionSchema.fields().length;
      readFunc = wrapReaderForDVFiltering(readFunc, dvColumnIndex, totalColumns);
    }

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }

  // DV column name constant - must match DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
  private static final String IS_ROW_DELETED_COLUMN_NAME = "__delta_internal_is_row_deleted";

  /**
   * Check if table supports reading deletion vectors.
   *
   * @param protocol The table protocol
   * @param metadata The table metadata
   * @return true if deletion vectors can be read
   */
  private static boolean isDeletionVectorReadable(Protocol protocol, Metadata metadata) {
    return protocol.supportsFeature(
            io.delta.kernel.internal.tablefeatures.TableFeatures.DELETION_VECTORS_RW_FEATURE)
        && "parquet".equalsIgnoreCase(metadata.getFormat().getProvider());
  }

  /**
   * Add the __delta_internal_is_row_deleted column to the schema.
   *
   * @param schema The original schema
   * @return Schema with IS_ROW_DELETED column appended
   */
  private static StructType addIsRowDeletedColumn(StructType schema) {
    List<StructField> fields = new ArrayList<>(Arrays.asList(schema.fields()));
    fields.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    return new StructType(fields.toArray(new StructField[0]));
  }

  /**
   * Wrap the base reader function to filter out deleted rows and remove the DV column.
   *
   * <p>This implements a simple row-by-row filtering approach for Phase 1. For each row: 1. Check
   * the is_row_deleted column (0 = keep, non-0 = delete) 2. Filter out deleted rows 3. Project out
   * the is_row_deleted column from the output
   *
   * @param baseReadFunc The original reader function
   * @param dvColumnIndex Index of the __delta_internal_is_row_deleted column
   * @param totalColumns Total columns including partition columns
   * @return Wrapped reader function with DV filtering
   */
  private static Function1<PartitionedFile, Iterator<InternalRow>> wrapReaderForDVFiltering(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      int dvColumnIndex,
      int totalColumns) {
    return new DVFilteringReadFunc(baseReadFunc, dvColumnIndex, totalColumns);
  }

  /**
   * Serializable wrapper function that applies DV filtering to the base reader.
   *
   * <p>Must be a static class that implements Serializable to allow Spark task serialization.
   */
  private static class DVFilteringReadFunc
      extends scala.runtime.AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
      implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
    private final int dvColumnIndex;
    private final int totalColumns;

    DVFilteringReadFunc(
        Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
        int dvColumnIndex,
        int totalColumns) {
      this.baseReadFunc = baseReadFunc;
      this.dvColumnIndex = dvColumnIndex;
      this.totalColumns = totalColumns;
    }

    @Override
    public Iterator<InternalRow> apply(PartitionedFile file) {
      Iterator<InternalRow> baseIter = baseReadFunc.apply(file);
      return new DVFilteringIterator(baseIter, dvColumnIndex, totalColumns);
    }
  }

  /**
   * Iterator that filters deleted rows and removes the DV column from output.
   *
   * <p>Phase 1: Only handles row-based (InternalRow) output since vectorized reader is disabled.
   */
  private static class DVFilteringIterator implements Iterator<InternalRow> {
    private final Iterator<InternalRow> baseIter;
    private final int dvColumnIndex;
    private final int outputNumColumns;
    private InternalRow nextRow = null;

    DVFilteringIterator(Iterator<InternalRow> baseIter, int dvColumnIndex, int totalColumns) {
      this.baseIter = baseIter;
      this.dvColumnIndex = dvColumnIndex;
      this.outputNumColumns = totalColumns - 1; // Exclude DV column
    }

    @Override
    public boolean hasNext() {
      while (nextRow == null && baseIter.hasNext()) {
        InternalRow row = baseIter.next();
        // Check if row is deleted (0 = keep, non-0 = delete)
        byte isDeleted = row.getByte(dvColumnIndex);
        if (isDeleted == 0) {
          nextRow = projectRow(row);
          return true;
        }
      }
      return nextRow != null;
    }

    @Override
    public InternalRow next() {
      if (nextRow == null && !hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      InternalRow result = nextRow;
      nextRow = null;
      return result;
    }

    /** Project out the DV column from the row. */
    private InternalRow projectRow(InternalRow row) {
      Object[] values = new Object[outputNumColumns];
      int outIdx = 0;
      for (int i = 0; i < row.numFields(); i++) {
        if (i != dvColumnIndex) {
          values[outIdx++] = row.get(i, null);
        }
      }
      return new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values);
    }
  }
}
