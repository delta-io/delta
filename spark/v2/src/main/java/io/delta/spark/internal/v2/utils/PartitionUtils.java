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
import io.delta.spark.internal.v2.read.cdc.CDCReadFunction;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorReadFunction;
import io.delta.spark.internal.v2.read.deletionvector.DeletionVectorSchemaContext;
import io.delta.spark.internal.v2.read.metadata.MetadataStructReadFunction;
import io.delta.spark.internal.v2.read.metadata.MetadataStructSchemaContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FilePartition$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
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
   * Plan input partitions by bin-packing a list of {@link PartitionedFile}s into {@link
   * FilePartition}s.
   */
  public static InputPartition[] planInputPartitions(
      SparkSession sparkSession,
      List<PartitionedFile> partitionedFiles,
      long totalBytes,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    long maxSplitBytes =
        calculateMaxSplitBytes(sparkSession, totalBytes, partitionedFiles.size(), sqlConf);
    scala.collection.Seq<FilePartition> filePartitions =
        FilePartition$.MODULE$.getFilePartitions(
            sparkSession, JavaConverters.asScalaBuffer(partitionedFiles).toSeq(), maxSplitBytes);

    return JavaConverters.seqAsJavaList(filePartitions).toArray(new InputPartition[0]);
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
        if (strVal == null) {
          values[pos] = null;
        } else if (field.dataType() instanceof StringType) {
          values[pos] = UTF8String.fromString(strVal);
        } else {
          values[pos] =
              PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
        }
      }
    }
    return new GenericInternalRow(values);
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
    scala.collection.immutable.Map<String, Object> metadata =
        mergeIntoScalaMap(
            buildDvMetadataScala(addFile.getDeletionVector()),
            buildRowTrackingMetadata(addFile.getBaseRowId(), addFile.getDefaultRowCommitVersion()));
    return makePartitionedFile(
        resolveTableRelativePath(tablePath, addFile.getPath()),
        addFile.getSize(),
        addFile.getModificationTime(),
        getPartitionRow(addFile.getPartitionValues(), partitionSchema, zoneId),
        metadata);
  }

  /**
   * Build a PartitionedFile from a CDCDataFile with CDC constants in
   * otherConstantMetadataColumnValues for the {@link CDCReadFunction} to null-coalesce.
   *
   * <p>Row tracking metadata is intentionally NOT included: per Delta protocol "Reader Requirements
   * for Row Tracking", readers must not expose row IDs / row commit versions while reading change
   * data files from {@code cdc} actions.
   */
  public static PartitionedFile buildCDCPartitionedFile(
      io.delta.spark.internal.v2.read.CDCDataFile cdcFile,
      long commitVersion,
      StructType partitionSchema,
      String tablePath,
      ZoneId zoneId) {
    Map<String, Object> cdcConstants = new HashMap<>();
    cdcConstants.put(CDCSchemaContext.CDC_COMMIT_VERSION, commitVersion);
    cdcConstants.put(
        CDCSchemaContext.CDC_COMMIT_TIMESTAMP,
        TimeUnit.MILLISECONDS.toMicros(cdcFile.getCommitTimestamp()));
    if (!cdcFile.isAddCDCFile()) {
      cdcConstants.put(CDCSchemaContext.CDC_TYPE_COLUMN, cdcFile.getChangeType());
    }
    Optional<String> effectiveDv = cdcFile.getEffectiveDv();
    scala.collection.immutable.Map<String, Object> metadata =
        effectiveDv.isPresent()
            ? buildDvMetadata(effectiveDv.get(), cdcFile.getDvFilterType())
            : scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, Object> entry : cdcConstants.entrySet()) {
      metadata = metadata.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    MapValue partitionValues = cdcFile.getPartitionValues();
    if (partitionValues == null) {
      // Only RemoveFile reaches here, when extendedFileMetadata is absent. Match V1's
      // TahoeRemoveFileIndex behaviour rather than silently consuming missing metadata.
      throw (RuntimeException) DeltaErrors.removeFileCDCMissingExtendedMetadata(cdcFile.getPath());
    }
    InternalRow partitionRow =
        partitionSchema.fields().length > 0
            ? getPartitionRow(partitionValues, partitionSchema, zoneId)
            : emptyPartitionRow(0);
    long modificationTime =
        cdcFile.getAddFile() != null
            ? cdcFile.getAddFile().getModificationTime()
            : cdcFile.getCommitTimestamp();
    return makePartitionedFile(
        resolveTableRelativePath(tablePath, cdcFile.getPath()),
        cdcFile.getFileSize(),
        modificationTime,
        partitionRow,
        metadata);
  }

  /**
   * Resolves a Delta-log-stored relative path against the table root and returns its {@link
   * SparkPath}.
   *
   * <p>Delta stores file paths URL-encoded per the protocol. Building the absolute path with {@code
   * new Path(parent, child)} re-encodes the already-encoded child (e.g. '%' -> '%25'), and {@code
   * SparkPath.fromUrlString(Path.toString())} then assumes a fully URL-encoded URI that {@code
   * Path.toString()} does not produce. Reserved characters in the path (spaces, '%') therefore
   * raise {@link URISyntaxException} or resolve to the wrong (double-encoded) file. Wrapping the
   * child in {@code new URI(child)} accepts the already-encoded form verbatim, and {@link
   * SparkPath#fromPath} routes through Hadoop's URI builder instead of assuming an encoded string.
   */
  public static SparkPath resolveTableRelativePath(String tablePath, String relativeOrAbsolute) {
    try {
      Path child = new Path(new URI(relativeOrAbsolute));
      Path absolute = child.isAbsolute() ? child : new Path(new Path(tablePath), child);
      return SparkPath.fromPath(absolute);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Could not parse Delta-log file path as URI: " + relativeOrAbsolute, e);
    }
  }

  private static PartitionedFile makePartitionedFile(
      SparkPath path,
      long size,
      long modificationTime,
      InternalRow partitionRow,
      scala.collection.immutable.Map<String, Object> metadata) {
    return new PartitionedFile(
        partitionRow,
        path,
        /* start= */ 0L,
        /* length= */ size,
        /* preferredLocations= */ new String[0],
        modificationTime,
        /* fileSize= */ size,
        metadata);
  }

  private static InternalRow emptyPartitionRow(int numFields) {
    return new GenericInternalRow(new Object[numFields]);
  }

  private static scala.collection.immutable.Map<String, Object> mergeIntoScalaMap(
      scala.collection.immutable.Map<String, Object> base,
      scala.collection.immutable.Map<String, Object> additions) {
    scala.collection.immutable.Map<String, Object> result = base;
    for (Map.Entry<String, Object> entry : CollectionConverters.asJava(additions).entrySet()) {
      result = result.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return result;
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
   * @param isWriteTimeCDCRead If {@code true}, this is a write-time CDF read (streaming reads of
   *     the legacy {@code .option("readChangeFeed")} format): the read schema is augmented with CDC
   *     tail columns and the reader is wrapped with {@link CDCReadFunction}. If {@code false}, this
   *     is a plain table scan or a read-time Auto-CDF read; CDC handling is left to the caller in
   *     that case (Auto-CDF's outer {@code CDCPartitionReaderFactory} injects the tail columns as
   *     per-partition constants instead).
   */
  public static PartitionReaderFactory createDeltaParquetReaderFactory(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      StructType ddlOrderedReadOutputSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    return createDeltaParquetReaderFactory(
        snapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf,
        /* isWriteTimeCDCRead */ false);
  }

  public static PartitionReaderFactory createDeltaParquetReaderFactory(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      StructType ddlOrderedReadOutputSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf,
      boolean isWriteTimeCDCRead) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    // Use Path.toString() instead of toUri().toString() to avoid URL encoding issues.
    // toUri().toString() encodes special characters (e.g., space -> %20), which causes
    // DV file path resolution failures.
    String tablePath = snapshotImpl.getDataPath().toString();

    // Preserve the caller-provided readDataSchema (pre-DV/RT/CDC augmentation) for the final
    // column-reorder wrapper below.
    final StructType originalReadDataSchema = readDataSchema;

    // For write-time CDF reads (streaming with readChangeFeed=true), build the schema context
    // and augment readDataSchema with CDC tail columns before DV wrapping so that DV column
    // indices account for them. Read-time CDF (Auto-CDF, via DeltaChangelogBatch) does not go
    // through this path: DeltaChangelogBatch's outer CDCPartitionReaderFactory injects the
    // tail columns as per-partition constants instead.
    Optional<CDCSchemaContext> cdcSchemaContext =
        isWriteTimeCDCRead
            ? Optional.of(new CDCSchemaContext(readDataSchema, partitionSchema))
            : Optional.empty();
    if (cdcSchemaContext.isPresent()) {
      readDataSchema = cdcSchemaContext.get().getReadDataSchemaWithCDC();
    }

    // DV presence governs `optimizationsEnabled` / `useMetadataRowIndex`, both of which feed
    // into `DeltaParquetFileFormatV2` construction below. Compute it upfront so the format can
    // be built before MetadataStructSchemaContext (which consumes the format's
    // `fileConstantMetadataExtractors`).
    boolean tableSupportsDV = tableSupportsDeletionVectors(snapshot);
    boolean optimizationsEnabled = !tableSupportsDV;
    Option<Boolean> useMetadataRowIndex =
        tableSupportsDV ? Option.apply(Boolean.FALSE) : Option.empty();
    DeltaParquetFileFormatV2 deltaFormat =
        createDeltaParquetFileFormat(
            snapshot, tablePath, optimizationsEnabled, useMetadataRowIndex, isWriteTimeCDCRead);

    // Build the metadata context only when the scan requested `_metadata`. The context owns the
    // pruned struct, the parquet read schema (with row-tracking helper columns when needed),
    // and the per-field MetadataValueSetterBuilder array used to materialise values per row.
    Optional<MetadataStructSchemaContext> metadataSchemaContext =
        MetadataStructSchemaContext.forSchema(
            readDataSchema, partitionSchema, deltaFormat, snapshotImpl.getMetadata());
    if (metadataSchemaContext.isPresent()) {
      readDataSchema = metadataSchemaContext.get().getParquetReadSchema();
    }

    // Create DV schema context if table supports deletion vectors
    Optional<DeletionVectorSchemaContext> dvSchemaContext =
        tableSupportsDV
            ? Optional.of(new DeletionVectorSchemaContext(readDataSchema, partitionSchema))
            : Optional.empty();
    if (dvSchemaContext.isPresent()) {
      readDataSchema = dvSchemaContext.get().getSchemaWithDvColumn();
    }

    boolean enableVectorizedReader =
        // Disabled because MetadataStructReadFunction operates on individual InternalRows (to
        // synthesise the `_metadata` struct, including row-tracking field coalesce against
        // materialised helper columns). The wrapper does not currently produce ColumnarBatch
        // output.
        !metadataSchemaContext.isPresent()
            && ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithVectorizedReading =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    // TODO(https://github.com/delta-io/delta/issues/5859): Enable file splitting for DV tables
    // (`optimizationsEnabled`) and support _metadata.row_index for DV (`useMetadataRowIndex`).
    // Both flags are computed above the metadata-context construction so the format can drive
    // its `fileConstantMetadataExtractors`.

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
    // at its expected index, before row tracking or metadata injection changes the column
    // layout.
    if (dvSchemaContext.isPresent()) {
      readFunc =
          DeletionVectorReadFunction.wrap(readFunc, dvSchemaContext.get(), enableVectorizedReader);
    }

    // Wrap reader to materialise the `_metadata` struct whenever any `_metadata` subfield is
    // requested by the scan. The metadata context owns one MetadataValueSetter per requested
    // field; both file-source base fields and Delta row-tracking fields flow through the same
    // per-row materialisation step.
    if (metadataSchemaContext.isPresent()) {
      readFunc = MetadataStructReadFunction.wrap(readFunc, metadataSchemaContext.get());
    }

    // TODO(#5319): add e2e test for CDC reads (full schema + column pruning) when streaming CDC
    // reads become user-reachable end-to-end.
    if (cdcSchemaContext.isPresent()) {
      if (metadataSchemaContext.isPresent()) {
        throw new UnsupportedOperationException(
            "CDC reads combined with _metadata reads are not supported");
      }
      readFunc = CDCReadFunction.wrap(readFunc, cdcSchemaContext.get(), enableVectorizedReader);
    }

    // DV and RT strip their internal helpers before yielding; CDC appends its fields at the tail.
    // The wrapper infers the source layout from (data, partition, target) - CDC tail fields end up
    // identity-mapped while data/partition columns are permuted into DDL order.
    readFunc =
        ColumnReorderReadFunction.wrap(
            readFunc,
            enableVectorizedReader,
            originalReadDataSchema,
            partitionSchema,
            ddlOrderedReadOutputSchema);

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
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
    return createDeltaParquetFileFormat(
        snapshot, tablePath, optimizationsEnabled, useMetadataRowIndex, /* isCDCRead */ false);
  }

  public static DeltaParquetFileFormatV2 createDeltaParquetFileFormat(
      Snapshot snapshot,
      String tablePath,
      boolean optimizationsEnabled,
      Option<Boolean> useMetadataRowIndex,
      boolean isCDCRead) {
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    return new DeltaParquetFileFormatV2(
        snapshotImpl.getProtocol(),
        snapshotImpl.getMetadata(),
        /* nullableRowTrackingConstantFields */ false,
        /* nullableRowTrackingGeneratedFields */ false,
        optimizationsEnabled,
        Option.apply(tablePath),
        isCDCRead,
        useMetadataRowIndex);
  }

  /**
   * Build metadata map for PartitionedFile containing DV descriptor if present.
   *
   * <p>The metadata is used by DeltaParquetFileFormat to generate the is_row_deleted column. Uses
   * {@code IF_CONTAINED} filter semantics (visible-rows semantics). Returns an empty map when
   * {@code dvBase64} is {@code null}.
   */
  public static Map<String, Object> buildDvMetadata(String dvBase64) {
    Map<String, Object> metadata = new HashMap<>();
    if (dvBase64 != null) {
      metadata.put(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED(), dvBase64);
      metadata.put(
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE(), RowIndexFilterType.IF_CONTAINED);
    }
    return metadata;
  }

  private static scala.collection.immutable.Map<String, Object> buildDvMetadataScala(
      Optional<DeletionVectorDescriptor> dvOpt) {
    Map<String, Object> metadata =
        buildDvMetadata(dvOpt.map(DeletionVectorDescriptor::serializeToBase64).orElse(null));
    return scala.collection.immutable.Map$.MODULE$.from(CollectionConverters.asScala(metadata));
  }

  private static scala.collection.immutable.Map<String, Object> buildDvMetadata(
      String dvBase64, RowIndexFilterType filterType) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED(), dvBase64);
    metadata.put(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE(), filterType);
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
