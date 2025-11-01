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
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarBatchRow;
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

  // Serializable wrappers for Protocol and Metadata
  private final SerializableKernelRowWrapper protocolWrapper;
  private final SerializableKernelRowWrapper metadataWrapper;
  private final String tablePath;
  private final ColumnMapping.ColumnMappingMode columnMappingMode;

  public DeltaParquetFileFormat(
      Engine kernelEngine, Protocol protocol, Metadata metadata, String tablePath) {
    Objects.requireNonNull(kernelEngine, "kernelEngine is null");
    Objects.requireNonNull(protocol, "protocol is null");
    Objects.requireNonNull(metadata, "metadata is null");
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");

    // Wrap Protocol and Metadata in serializable wrappers
    this.protocolWrapper = new SerializableKernelRowWrapper(protocol.toRow());
    this.metadataWrapper = new SerializableKernelRowWrapper(metadata.toRow());
    this.columnMappingMode = ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
  }

  /** Get Metadata from wrapper */
  private Metadata getMetadata() {
    return Metadata.fromRow(metadataWrapper.getRow());
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
      return applyDeletionVectorIfNeeded(file, baseIterator, hadoopConf);
    };
  }

  /** Convert logical Spark schema to physical schema using Kernel's ColumnMapping utilities. */
  private org.apache.spark.sql.types.StructType convertToPhysicalSchema(
      org.apache.spark.sql.types.StructType logicalSchema) {
    if (columnMappingMode == ColumnMapping.ColumnMappingMode.NONE) {
      return logicalSchema;
    }

    Metadata metadata = getMetadata();

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

  /**
   * Apply deletion vector filtering if present. Supports both vectorized (ColumnarBatch) and
   * non-vectorized (InternalRow) data from Parquet reader.
   */
  @SuppressWarnings("unchecked")
  private Iterator<InternalRow> applyDeletionVectorIfNeeded(
      PartitionedFile file, Iterator<InternalRow> dataIterator, Configuration hadoopConf) {

    Optional<DeletionVectorDescriptor> dvDescriptorOpt = extractDeletionVectorDescriptor(file);

    if (!dvDescriptorOpt.isPresent()) {
      return dataIterator;
    }

    // Load deletion vector using Kernel API
    RoaringBitmapArray deletionVector = loadDeletionVector(dvDescriptorOpt.get(), hadoopConf);

    // Filter out deleted rows - handle both vectorized and row-based data
    // Cast to Iterator<Object> since Parquet may return ColumnarBatch or InternalRow
    Iterator<Object> objectIterator = (Iterator<Object>) (Iterator<?>) dataIterator;
    return new DeletionVectorFilterIterator(objectIterator, deletionVector);
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
  private RoaringBitmapArray loadDeletionVector(
      DeletionVectorDescriptor dvDescriptor, Configuration hadoopConf) {
    try {
      // Create a new engine for this task
      Engine engine = io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf);
      Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> result =
          io.delta.kernel.internal.deletionvectors.DeletionVectorUtils.loadNewDvAndBitmap(
              engine, tablePath, dvDescriptor);
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

  /**
   * Iterator that filters out rows marked as deleted in the deletion vector. Supports both
   * vectorized (ColumnarBatch) and non-vectorized (InternalRow) data.
   */
  private static class DeletionVectorFilterIterator
      extends scala.collection.AbstractIterator<InternalRow> {
    private final Iterator<Object> underlying;
    private final RoaringBitmapArray deletionVector;
    private long currentRowIndex = 0;

    // For handling ColumnarBatch - use Scala Iterator
    private scala.collection.Iterator<InternalRow> currentBatchIterator = null;

    // Type handlers map for processing different data formats
    private final Map<Class<?>, Function<Object, InternalRow>> typeHandlers;

    DeletionVectorFilterIterator(Iterator<Object> underlying, RoaringBitmapArray deletionVector) {
      this.underlying = underlying;
      this.deletionVector = deletionVector;

      // Initialize type handlers
      this.typeHandlers = new HashMap<>();
      typeHandlers.put(ColumnarBatch.class, this::handleColumnarBatch);
      typeHandlers.put(ColumnarBatchRow.class, this::handleColumnarBatchRow);
      typeHandlers.put(InternalRow.class, this::handleInternalRow);
    }

    @Override
    public boolean hasNext() {
      // First check if we have rows from current batch
      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return true;
      }

      // Try to get next batch or row
      return underlying.hasNext();
    }

    @Override
    public InternalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      // If we have rows from current batch, return next one
      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return currentBatchIterator.next();
      }

      // Get next item from underlying iterator
      Object next = underlying.next();

      // Use type handlers map to process different data formats
      Function<Object, InternalRow> handler = typeHandlers.get(next.getClass());
      if (handler != null) {
        return handler.apply(next);
      } else {
        throw new RuntimeException(
            "Unexpected row type from Parquet reader: " + next.getClass().getName());
      }
    }

    /** Handle vectorized ColumnarBatch data */
    private InternalRow handleColumnarBatch(Object obj) {
      ColumnarBatch batch = (ColumnarBatch) obj;
      List<InternalRow> filteredRows = filterColumnarBatch(batch);
      // Convert Java Iterator to Scala Iterator
      currentBatchIterator =
          scala.collection.JavaConverters.asScalaIterator(filteredRows.iterator());
      return currentBatchIterator.next();
    }

    /**
     * Handle ColumnarBatchRow - vectorized reader enabled but returns immutable rows. This is not
     * efficient and should only affect wide tables.
     */
    private InternalRow handleColumnarBatchRow(Object obj) {
      ColumnarBatchRow columnarRow = (ColumnarBatchRow) obj;
      // Need to copy the row since ColumnarBatchRow is immutable
      InternalRow row = columnarRow.copy();
      // Filter out deleted rows
      while (deletionVector.contains(currentRowIndex)) {
        currentRowIndex++;
        if (!underlying.hasNext()) {
          throw new NoSuchElementException();
        }
        Object next = underlying.next();
        if (next instanceof ColumnarBatchRow) {
          row = ((ColumnarBatchRow) next).copy();
        } else {
          row = (InternalRow) next;
        }
      }
      currentRowIndex++;
      return row;
    }

    /** Handle non-vectorized InternalRow data */
    private InternalRow handleInternalRow(Object obj) {
      InternalRow row = (InternalRow) obj;
      // Filter out deleted rows
      while (deletionVector.contains(currentRowIndex)) {
        currentRowIndex++;
        if (!underlying.hasNext()) {
          throw new NoSuchElementException();
        }
        row = (InternalRow) underlying.next();
      }
      currentRowIndex++;
      return row;
    }

    /** Filter ColumnarBatch by deletion vector. Returns list of non-deleted rows. */
    private List<InternalRow> filterColumnarBatch(ColumnarBatch batch) {
      List<InternalRow> result = new ArrayList<>();
      int numRows = batch.numRows();

      for (int i = 0; i < numRows; i++) {
        if (!deletionVector.contains(currentRowIndex + i)) {
          result.add(batch.getRow(i).copy());
        }
      }

      currentRowIndex += numRows;
      return result;
    }
  }
}
