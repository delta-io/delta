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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.spark.sql.Dataset;

/**
 * Default implementation of {@link ScanExecutor}.
 *
 * <p>Executes a planned DataFrame and streams results as Kernel {@link FilteredColumnarBatch}
 * objects. The Spark-to-Kernel boundary is handled by lightweight, zero-copy wrappers that expose
 * Spark Rows through the Kernel Row API.
 *
 * <h3>Contract:</h3>
 *
 * <p>The input DataFrame must already have all filters applied (partition pruning + data skipping).
 * This class only materializes rows and converts them — it performs <b>no filtering</b>.
 *
 * <h3>Effective Java design (Items 15, 17, 19, 49, 64):</h3>
 *
 * <ul>
 *   <li>Implements {@link ScanExecutor} interface (Item 64).
 *   <li>Immutable — all fields {@code final}; no setters (Item 17).
 *   <li>{@code final} class — prohibits sub-classing (Item 19).
 *   <li>Row wrappers are {@code private static} inner classes (Item 15).
 * </ul>
 */
public final class SparkRowScanExecutor implements ScanExecutor {

  private final Dataset<org.apache.spark.sql.Row> plannedDataFrame;

  /**
   * Creates a SparkRowScanExecutor with an already-planned DataFrame.
   *
   * @param plannedDataFrame filtered DataFrame from Planner (non-null)
   */
  SparkRowScanExecutor(Dataset<org.apache.spark.sql.Row> plannedDataFrame) {
    this.plannedDataFrame = requireNonNull(plannedDataFrame, "plannedDataFrame"); // Item 49
  }

  /**
   * Executes the planned DataFrame and streams results as Kernel ColumnarBatch objects.
   *
   * <p>Uses {@code toLocalIterator()} for lazy, streaming execution — rows are pulled from
   * executors on demand without collecting all results to the driver.
   *
   * <p>Each Spark Row is wrapped as a single-row {@link ColumnarBatch} via zero-copy adapters.
   */
  @Override
  public CloseableIterator<FilteredColumnarBatch> execute() {
    // Convert Spark schema to Kernel schema (done once, reused for all rows)
    final StructType dataFrameSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            plannedDataFrame.schema());

    // Execute: stream rows from executors without collecting all
    CloseableIterator<org.apache.spark.sql.Row> rowIterator =
        toCloseableIterator(plannedDataFrame.toLocalIterator());

    // Wrap each Spark Row as Kernel ColumnarBatch (zero-copy boundary)
    return rowIterator.map(
        sparkRow -> {
          ColumnarBatch batch = new SparkRowColumnarBatch(sparkRow, dataFrameSchema);
          return new FilteredColumnarBatch(batch, Optional.empty());
        });
  }

  // ===================== Private Row Wrappers (Item 15: minimize accessibility) ==================

  /**
   * Minimal {@link ColumnarBatch} wrapping a single Spark Row. Allows Spark DataFrame rows to pass
   * through the Kernel API boundary without data copying.
   */
  private static final class SparkRowColumnarBatch implements ColumnarBatch {
    private final org.apache.spark.sql.Row sparkRow;
    private final StructType schema;

    SparkRowColumnarBatch(org.apache.spark.sql.Row sparkRow, StructType schema) {
      this.sparkRow = sparkRow;
      this.schema = schema;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public int getSize() {
      return 1;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
      return new SparkRowFieldAsColumnVector(sparkRow, ordinal, schema);
    }

    @Override
    public CloseableIterator<Row> getRows() {
      return new CloseableIterator<Row>() {
        private boolean consumed = false;

        @Override
        public boolean hasNext() {
          return !consumed;
        }

        @Override
        public Row next() {
          if (consumed) {
            throw new NoSuchElementException();
          }
          consumed = true;
          return new SparkRowAsKernelRow(sparkRow, schema);
        }

        @Override
        public void close() {}
      };
    }
  }

  /**
   * Wraps a Spark Row field as a Kernel {@link ColumnVector}. Each field in the row is exposed as a
   * single-element vector.
   */
  private static final class SparkRowFieldAsColumnVector implements ColumnVector {
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
      return parentSchema != null ? parentSchema.at(fieldOrdinal).getDataType() : null;
    }

    @Override
    public int getSize() {
      return 1;
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return sparkRow.isNullAt(fieldOrdinal);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      if (sparkRow.isNullAt(fieldOrdinal)) {
        throw new IllegalStateException("Cannot get child of null struct");
      }
      org.apache.spark.sql.Row nested = sparkRow.getStruct(fieldOrdinal);
      StructType nestedSchema =
          parentSchema != null ? (StructType) parentSchema.at(fieldOrdinal).getDataType() : null;
      return new SparkRowFieldAsColumnVector(nested, ordinal, nestedSchema);
    }

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
   * Adapts a Spark Row to the Kernel {@link Row} interface. Zero-copy: delegates directly to Spark
   * Row accessor methods. Package-private to allow DistributedScan access.
   */
  static final class SparkRowAsKernelRow implements Row {
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
      throw new UnsupportedOperationException("Array not yet supported");
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int ordinal) {
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(ordinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public Row getStruct(int ordinal) {
      StructType nestedSchema =
          schema != null ? (StructType) schema.at(ordinal).getDataType() : null;
      return new SparkRowAsKernelRow(sparkRow.getStruct(ordinal), nestedSchema);
    }
  }

  /**
   * Adapts a Spark Scala Map to the Kernel {@link io.delta.kernel.data.MapValue} interface. Used
   * for partition values which are stored as {@code Map<String, String>} in Delta.
   */
  private static final class SparkMapAsKernelMapValue implements io.delta.kernel.data.MapValue {
    private final java.util.List<String> keys;
    private final java.util.List<String> values;

    SparkMapAsKernelMapValue(scala.collection.Map<String, String> scalaMap) {
      this.keys = new java.util.ArrayList<>();
      this.values = new java.util.ArrayList<>();
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
    public ColumnVector getKeys() {
      return new StringColumnVector(keys);
    }

    @Override
    public ColumnVector getValues() {
      return new StringColumnVector(values);
    }
  }

  /** Immutable {@link ColumnVector} backed by a {@code List<String>}. */
  private static final class StringColumnVector implements ColumnVector {
    private final java.util.List<String> data;

    StringColumnVector(java.util.List<String> data) {
      this.data = data;
    }

    @Override
    public io.delta.kernel.types.DataType getDataType() {
      return io.delta.kernel.types.StringType.STRING;
    }

    @Override
    public int getSize() {
      return data.size();
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return data.get(rowId) == null;
    }

    @Override
    public String getString(int rowId) {
      return data.get(rowId);
    }

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
