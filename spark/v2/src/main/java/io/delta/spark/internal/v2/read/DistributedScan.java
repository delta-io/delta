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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Kernel-compatible Scan that uses distributed DataFrame processing.
 *
 * <p>This implementation uses only standard Kernel APIs while providing lazy, distributed
 * processing. The DataFrame is not collected until getScanFiles() is called.
 *
 * <p>Key design: getScanFiles() returns an iterator that lazily processes DataFrame rows,
 * satisfying Kernel API contract while maintaining distributed processing benefits.
 */
public class DistributedScan implements Scan {
  private final Scan delegateScan;
  private final Dataset<org.apache.spark.sql.Row> dataFrame;
  private final SparkSession spark;
  private final boolean maintainOrdering;

  /**
   * Create a DistributedScan.
   *
   * @param spark Spark session
   * @param dataFrame DataFrame with distributed log replay results (not yet executed - lazy!)
   * @param snapshot Delta snapshot
   * @param readSchema Read schema
   * @param maintainOrdering Whether to preserve DataFrame ordering (required for streaming)
   */
  public DistributedScan(
      SparkSession spark,
      Dataset<org.apache.spark.sql.Row> dataFrame,
      Snapshot snapshot,
      StructType readSchema,
      boolean maintainOrdering) {
    this.spark = spark;
    this.dataFrame = dataFrame;
    this.maintainOrdering = maintainOrdering;
    // Build the scan using Kernel's standard API for delegation
    this.delegateScan = snapshot.getScanBuilder().withReadSchema(readSchema).build();
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    // Lazy execution: toLocalIterator() streams data from executors without collecting all
    // Get the full DataFrame schema (includes "add" struct)
    final io.delta.kernel.types.StructType dataFrameSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            dataFrame.schema());

    return toCloseableIterator(dataFrame.toLocalIterator())
        .map(
            sparkRow -> {
              // Wrap full row (with "add" struct) as ColumnarBatch
              // StreamingHelper needs to extract "add" field
              ColumnarBatch batch = new SparkRowColumnarBatch(sparkRow, dataFrameSchema);
              return new FilteredColumnarBatch(batch, Optional.empty());
            });
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return delegateScan.getRemainingFilter();
  }

  @Override
  public Row getScanState(Engine engine) {
    return delegateScan.getScanState(engine);
  }

  /**
   * Minimal ColumnarBatch implementation that wraps a single Spark Row. This allows us to pass
   * Spark DataFrame rows through Kernel API.
   */
  private static class SparkRowColumnarBatch implements ColumnarBatch {
    private final org.apache.spark.sql.Row sparkRow;
    private final StructType schema;

    SparkRowColumnarBatch(org.apache.spark.sql.Row sparkRow, StructType addSchema) {
      this.sparkRow = sparkRow;
      this.schema = addSchema;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public int getSize() {
      return 1; // Single row batch
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
      // Return a wrapper that exposes the Spark Row field as a ColumnVector
      // This is needed for StreamingHelper.getAddFile()
      return new SparkRowFieldAsColumnVector(sparkRow, ordinal, schema);
    }

    @Override
    public CloseableIterator<Row> getRows() {
      // Single row batch - similar to ColumnarBatch.getRows() default implementation
      return new CloseableIterator<Row>() {
        int rowId = 0;

        @Override
        public boolean hasNext() {
          return rowId < 1;
        }

        @Override
        public Row next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          rowId++;
          // Return full row with "add" struct - StreamingHelper will extract it
          return new SparkRowAsKernelRow(sparkRow, schema);
        }

        @Override
        public void close() {}
      };
    }
  }

  /**
   * Wraps a Spark Row field as a Kernel ColumnVector. Used by StreamingHelper to extract struct
   * fields from ColumnarBatch.
   */
  private static class SparkRowFieldAsColumnVector implements ColumnVector {
    private final org.apache.spark.sql.Row sparkRow;
    private final int fieldOrdinal;
    private final StructType parentSchema;

    SparkRowFieldAsColumnVector(
        org.apache.spark.sql.Row sparkRow, int fieldOrdinal, StructType parentSchema) {
      this.sparkRow = sparkRow;
      this.fieldOrdinal = fieldOrdinal;
      this.parentSchema = parentSchema;
    }

    @Override
    public io.delta.kernel.types.DataType getDataType() {
      if (parentSchema != null) {
        return parentSchema.at(fieldOrdinal).getDataType();
      }
      return null;
    }

    @Override
    public int getSize() {
      return 1; // Single row
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return sparkRow.isNullAt(fieldOrdinal);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      // StructRow.fromStructVector() uses getChild() to access struct fields
      // Extract the nested struct and return a ColumnVector for the child field
      if (sparkRow.isNullAt(fieldOrdinal)) {
        throw new IllegalStateException("Cannot get child of null struct");
      }
      org.apache.spark.sql.Row nestedStruct = sparkRow.getStruct(fieldOrdinal);
      StructType nestedSchema = null;
      if (parentSchema != null) {
        nestedSchema = (StructType) parentSchema.at(fieldOrdinal).getDataType();
      }
      return new SparkRowFieldAsColumnVector(nestedStruct, ordinal, nestedSchema);
    }

    // Bridge primitive types to Spark Row
    // These are needed by ChildVectorBasedRow to access AddFile fields
    @Override
    public boolean getBoolean(int rowId) {
      return sparkRow.getBoolean(fieldOrdinal);
    }

    @Override
    public byte getByte(int rowId) {
      return sparkRow.getByte(fieldOrdinal);
    }

    @Override
    public short getShort(int rowId) {
      return sparkRow.getShort(fieldOrdinal);
    }

    @Override
    public int getInt(int rowId) {
      return sparkRow.getInt(fieldOrdinal);
    }

    @Override
    public long getLong(int rowId) {
      return sparkRow.getLong(fieldOrdinal);
    }

    @Override
    public float getFloat(int rowId) {
      return sparkRow.getFloat(fieldOrdinal);
    }

    @Override
    public double getDouble(int rowId) {
      return sparkRow.getDouble(fieldOrdinal);
    }

    @Override
    public byte[] getBinary(int rowId) {
      return (byte[]) sparkRow.get(fieldOrdinal);
    }

    @Override
    public String getString(int rowId) {
      return sparkRow.getString(fieldOrdinal);
    }

    @Override
    public java.math.BigDecimal getDecimal(int rowId) {
      return sparkRow.getDecimal(fieldOrdinal);
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int rowId) {
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(fieldOrdinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public io.delta.kernel.data.ArrayValue getArray(int rowId) {
      throw new UnsupportedOperationException("Array not yet supported");
    }
  }

  /**
   * Adapter to expose Spark Row as Kernel Row. Implements full Kernel Row API by bridging to Spark
   * Row methods. Package-private to allow SparkScan to access.
   */
  static class SparkRowAsKernelRow implements Row {
    private final org.apache.spark.sql.Row sparkRow;
    private final StructType schema;

    SparkRowAsKernelRow(org.apache.spark.sql.Row sparkRow, StructType schema) {
      this.sparkRow = sparkRow;
      this.schema = schema;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return sparkRow.isNullAt(ordinal);
    }

    // Bridge all primitive types to Spark Row
    @Override
    public boolean getBoolean(int ordinal) {
      return sparkRow.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      return sparkRow.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      return sparkRow.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      return sparkRow.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return sparkRow.getLong(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return sparkRow.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return sparkRow.getDouble(ordinal);
    }

    @Override
    public String getString(int ordinal) {
      return sparkRow.getString(ordinal);
    }

    @Override
    public java.math.BigDecimal getDecimal(int ordinal) {
      return sparkRow.getDecimal(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) sparkRow.get(ordinal);
    }

    @Override
    public io.delta.kernel.data.ArrayValue getArray(int ordinal) {
      throw new UnsupportedOperationException("Use getSparkRow() instead");
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int ordinal) {
      // Convert Spark Scala Map to Kernel MapValue
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(ordinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public Row getStruct(int ordinal) {
      // Get nested struct schema from parent schema
      StructType nestedSchema = null;
      if (schema != null) {
        nestedSchema = (StructType) schema.at(ordinal).getDataType();
      }
      return new SparkRowAsKernelRow(sparkRow.getStruct(ordinal), nestedSchema);
    }
  }

  /**
   * Adapter to expose Spark Scala Map as Kernel MapValue. This bridges Spark's partition values
   * (Scala Map) to Kernel API.
   */
  private static class SparkMapAsKernelMapValue implements io.delta.kernel.data.MapValue {
    private final java.util.List<String> keys;
    private final java.util.List<String> values;

    SparkMapAsKernelMapValue(scala.collection.Map<String, String> scalaMap) {
      this.keys = new java.util.ArrayList<>();
      this.values = new java.util.ArrayList<>();

      // Convert Scala Map to Lists
      if (scalaMap != null) {
        scala.collection.Iterator<scala.Tuple2<String, String>> iter = scalaMap.iterator();
        while (iter.hasNext()) {
          scala.Tuple2<String, String> entry = iter.next();
          keys.add(entry._1());
          values.add(entry._2());
        }
      }
    }

    @Override
    public int getSize() {
      return keys.size();
    }

    @Override
    public io.delta.kernel.data.ColumnVector getKeys() {
      return new StringColumnVector(keys);
    }

    @Override
    public io.delta.kernel.data.ColumnVector getValues() {
      return new StringColumnVector(values);
    }
  }

  /** Simple ArrayValue implementation for String lists. */
  private static class StringArrayValue implements io.delta.kernel.data.ArrayValue {
    private final java.util.List<String> list;

    StringArrayValue(java.util.List<String> list) {
      this.list = list;
    }

    @Override
    public int getSize() {
      return list.size();
    }

    @Override
    public io.delta.kernel.data.ColumnVector getElements() {
      return new StringColumnVector(list);
    }
  }

  /** Simple ColumnVector implementation for String lists. */
  private static class StringColumnVector implements io.delta.kernel.data.ColumnVector {
    private final java.util.List<String> list;

    StringColumnVector(java.util.List<String> list) {
      this.list = list;
    }

    @Override
    public io.delta.kernel.types.DataType getDataType() {
      return io.delta.kernel.types.StringType.STRING;
    }

    @Override
    public int getSize() {
      return list.size();
    }

    @Override
    public void close() {
      // No resources to close
    }

    @Override
    public boolean isNullAt(int rowId) {
      return list.get(rowId) == null;
    }

    @Override
    public String getString(int rowId) {
      return list.get(rowId);
    }

    // All other methods throw UnsupportedOperationException
    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.math.BigDecimal getDecimal(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.data.ArrayValue getArray(int rowId) {
      throw new UnsupportedOperationException();
    }
  }
}
