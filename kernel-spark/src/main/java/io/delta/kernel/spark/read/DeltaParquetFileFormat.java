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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.types.StructType;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.sources.Filter;
import scala.Function1;
import scala.collection.Iterator;

/**
 * A Delta-aware Parquet file format that supports: - Column Mapping (ID and Name modes) - Deletion
 * Vectors
 *
 * <p>This format wraps Spark's ParquetFileFormat and adds Delta-specific transformations using
 * Kernel APIs.
 */
public class DeltaParquetFileFormat extends ParquetFileFormat {

  // Metadata key for deletion vector descriptor
  public static final String DV_DESCRIPTOR_KEY = "__delta_dv_descriptor";

  private final Protocol protocol;
  private final Metadata metadata;
  private final String tablePath;
  private final Engine kernelEngine;
  private final ColumnMapping.ColumnMappingMode columnMappingMode;

  public DeltaParquetFileFormat(
      Protocol protocol, Metadata metadata, String tablePath, Engine kernelEngine) {
    this.protocol = Objects.requireNonNull(protocol, "protocol is null");
    this.metadata = Objects.requireNonNull(metadata, "metadata is null");
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");
    this.kernelEngine = Objects.requireNonNull(kernelEngine, "kernelEngine is null");
    this.columnMappingMode = ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
  }

  public Function1<PartitionedFile, Iterator<InternalRow>> buildReaderWithPartitionValues(
      SparkSession sparkSession,
      org.apache.spark.sql.types.StructType dataSchema,
      org.apache.spark.sql.types.StructType partitionSchema,
      org.apache.spark.sql.types.StructType requiredSchema,
      scala.collection.Seq<Filter> filters,
      scala.collection.immutable.Map<String, String> options,
      Configuration hadoopConf) {

    // Step 1: Convert logical schema to physical schema using Kernel API
    org.apache.spark.sql.types.StructType physicalDataSchema = convertToPhysicalSchema(dataSchema);
    org.apache.spark.sql.types.StructType physicalPartitionSchema =
        convertToPhysicalSchema(partitionSchema);
    org.apache.spark.sql.types.StructType physicalRequiredSchema =
        convertToPhysicalSchema(requiredSchema);

    // Step 2: Translate filters to use physical column names
    scala.collection.immutable.Seq<Filter> physicalFilters =
        columnMappingMode == ColumnMapping.ColumnMappingMode.NONE
            ? convertFiltersToImmutable(filters)
            : io.delta.kernel.spark.utils.ExpressionUtils.convertFiltersToPhysicalNames(
                filters, dataSchema, physicalDataSchema);

    // Step 3: Build standard Parquet reader with physical schema
    Function1<PartitionedFile, Iterator<InternalRow>> baseReader =
        super.buildReaderWithPartitionValues(
            sparkSession,
            physicalDataSchema,
            physicalPartitionSchema,
            physicalRequiredSchema,
            physicalFilters,
            options,
            hadoopConf);

    // Step 4: Wrap reader to apply deletion vector filtering
    return (PartitionedFile file) -> {
      Iterator<InternalRow> baseIterator = baseReader.apply(file);
      return applyDeletionVectorIfNeeded(file, baseIterator);
    };
  }

  /** Convert logical Spark schema to physical schema using Kernel's ColumnMapping utilities. */
  private org.apache.spark.sql.types.StructType convertToPhysicalSchema(
      org.apache.spark.sql.types.StructType logicalSchema) {
    if (columnMappingMode == ColumnMapping.ColumnMappingMode.NONE) {
      return logicalSchema;
    }

    // Convert Spark StructType to Kernel StructType
    StructType kernelLogicalSchema = SchemaUtils.convertSparkSchemaToKernelSchema(logicalSchema);
    StructType kernelFullSchema = metadata.getSchema();

    // Use Kernel API to convert to physical schema
    StructType kernelPhysicalSchema =
        ColumnMapping.convertToPhysicalSchema(
            kernelLogicalSchema, kernelFullSchema, columnMappingMode);

    // Convert back to Spark StructType
    return SchemaUtils.convertKernelSchemaToSparkSchema(kernelPhysicalSchema);
  }

  /** Convert Seq to immutable Seq for compatibility. */
  private scala.collection.immutable.Seq<Filter> convertFiltersToImmutable(
      scala.collection.Seq<Filter> filters) {
    if (filters instanceof scala.collection.immutable.Seq) {
      return (scala.collection.immutable.Seq<Filter>) filters;
    }
    return scala.collection.JavaConverters.asScalaBuffer(
            scala.collection.JavaConverters.seqAsJavaList(filters))
        .toSeq();
  }

  /** Apply deletion vector filtering if present. */
  private Iterator<InternalRow> applyDeletionVectorIfNeeded(
      PartitionedFile file, Iterator<InternalRow> dataIterator) {

    Optional<DeletionVectorDescriptor> dvDescriptorOpt = extractDeletionVectorDescriptor(file);

    if (!dvDescriptorOpt.isPresent()) {
      return dataIterator;
    }

    // Load deletion vector using Kernel API
    RoaringBitmapArray deletionVector = loadDeletionVector(dvDescriptorOpt.get());

    // Filter out deleted rows
    return new DeletionVectorFilterIterator(dataIterator, deletionVector);
  }

  /** Extract deletion vector descriptor from PartitionedFile metadata. */
  private Optional<DeletionVectorDescriptor> extractDeletionVectorDescriptor(PartitionedFile file) {
    scala.collection.immutable.Map<String, Object> metadata =
        file.otherConstantMetadataColumnValues();

    scala.Option<Object> dvOption = metadata.get(DV_DESCRIPTOR_KEY);
    if (dvOption.isDefined()) {
      Object dvObj = dvOption.get();
      if (dvObj instanceof DeletionVectorDescriptor) {
        return Optional.of((DeletionVectorDescriptor) dvObj);
      }
    }
    return Optional.empty();
  }

  /** Load deletion vector bitmap using Kernel API. */
  private RoaringBitmapArray loadDeletionVector(DeletionVectorDescriptor dvDescriptor) {
    try {
      Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> result =
          io.delta.kernel.internal.deletionvectors.DeletionVectorUtils.loadNewDvAndBitmap(
              kernelEngine, tablePath, dvDescriptor);
      return result._2;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load deletion vector", e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof DeltaParquetFileFormat)) return false;

    DeltaParquetFileFormat that = (DeltaParquetFileFormat) other;
    return Objects.equals(this.tablePath, that.tablePath)
        && Objects.equals(this.columnMappingMode, that.columnMappingMode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablePath, columnMappingMode);
  }

  /** Iterator that filters out rows marked as deleted in the deletion vector. */
  private static class DeletionVectorFilterIterator
      extends scala.collection.AbstractIterator<InternalRow> {
    private final Iterator<InternalRow> underlying;
    private final RoaringBitmapArray deletionVector;
    private long currentRowIndex = 0;
    private InternalRow nextRow = null;

    DeletionVectorFilterIterator(
        Iterator<InternalRow> underlying, RoaringBitmapArray deletionVector) {
      this.underlying = underlying;
      this.deletionVector = deletionVector;
    }

    @Override
    public boolean hasNext() {
      // Find the next non-deleted row
      while (nextRow == null && underlying.hasNext()) {
        InternalRow row = underlying.next();
        if (!deletionVector.contains(currentRowIndex)) {
          nextRow = row;
        }
        currentRowIndex++;
      }
      return nextRow != null;
    }

    @Override
    public InternalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      InternalRow result = nextRow;
      nextRow = null;
      return result;
    }
  }
}
