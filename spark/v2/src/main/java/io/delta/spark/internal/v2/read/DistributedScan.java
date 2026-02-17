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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Kernel {@link Scan} implementation backed by a pre-filtered Spark DataFrame.
 *
 * <p>{@link DistributedScanBuilder} orchestrates the full V1 shared pipeline (state reconstruction
 * + filter planning + scan execution) and produces a filtered DataFrame whose rows conform to the
 * Kernel scan-file schema ({@code {add: struct<...>, tableRoot: string}}). This class streams those
 * rows through zero-copy adapters that expose Spark Rows as Kernel Rows — no data copying or
 * serialization is performed.
 *
 * <p>{@code SparkScan} consumes the output of {@link #getScanFiles(Engine)} without changes.
 *
 * @see DistributedScanBuilder builder that creates this scan
 */
public final class DistributedScan implements Scan {

  private final Dataset<Row> plannedDataFrame;
  private final Scan delegateScan; // for getScanState / getRemainingFilter

  /**
   * Creates a DistributedScan from a pre-filtered DataFrame.
   *
   * @param plannedDataFrame filtered DataFrame with scan-file schema (non-null)
   * @param snapshot kernel snapshot for delegation metadata (non-null)
   * @param readSchema read schema for Kernel delegation (non-null)
   */
  DistributedScan(Dataset<Row> plannedDataFrame, Snapshot snapshot, StructType readSchema) {
    this.plannedDataFrame = requireNonNull(plannedDataFrame, "plannedDataFrame");
    requireNonNull(snapshot, "snapshot");
    requireNonNull(readSchema, "readSchema");
    this.delegateScan = snapshot.getScanBuilder().withReadSchema(readSchema).build();
  }

  /**
   * Streams the planned DataFrame rows as Kernel {@link FilteredColumnarBatch} objects.
   *
   * <p>Each Spark Row is wrapped as a single-row {@link ColumnarBatch} via zero-copy adapters —
   * Spark Row accessor methods are called directly through the Kernel Row/ColumnVector interfaces.
   */
  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    final StructType kernelSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            plannedDataFrame.schema());

    CloseableIterator<Row> rowIter = toCloseableIterator(plannedDataFrame.toLocalIterator());

    return rowIter.map(
        sparkRow -> {
          ColumnarBatch batch = new SparkRowColumnarBatch(sparkRow, kernelSchema);
          return new FilteredColumnarBatch(batch, Optional.empty());
        });
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return delegateScan.getRemainingFilter();
  }

  @Override
  public io.delta.kernel.data.Row getScanState(Engine engine) {
    return delegateScan.getScanState(engine);
  }

  // =========================================================================
  // Zero-copy Spark Row → Kernel Row/ColumnVector/MapValue adapters
  // =========================================================================

  /** Single-row {@link ColumnarBatch} wrapping a Spark Row. Zero-copy. */
  private static final class SparkRowColumnarBatch implements ColumnarBatch {
    private final Row sparkRow;
    private final StructType schema;

    SparkRowColumnarBatch(Row sparkRow, StructType schema) {
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
      return new SparkRowFieldVector(sparkRow, ordinal, schema);
    }

    @Override
    public CloseableIterator<io.delta.kernel.data.Row> getRows() {
      return new CloseableIterator<io.delta.kernel.data.Row>() {
        private boolean consumed = false;

        @Override
        public boolean hasNext() {
          return !consumed;
        }

        @Override
        public io.delta.kernel.data.Row next() {
          if (consumed) throw new NoSuchElementException();
          consumed = true;
          return new SparkRowAsKernelRow(sparkRow, schema);
        }

        @Override
        public void close() {}
      };
    }
  }

  /**
   * Wraps a Spark Row field as a single-element Kernel {@link ColumnVector}. Each field in the row
   * is exposed as a vector of size 1.
   */
  private static final class SparkRowFieldVector implements ColumnVector {
    private final Row sparkRow;
    private final int fieldOrdinal;
    private final StructType parentSchema;

    SparkRowFieldVector(Row sparkRow, int fieldOrdinal, StructType parentSchema) {
      this.sparkRow = sparkRow;
      this.fieldOrdinal = fieldOrdinal;
      this.parentSchema = parentSchema;
    }

    @Override
    public DataType getDataType() {
      return parentSchema.at(fieldOrdinal).getDataType();
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
    public BigDecimal getDecimal(int rowId) {
      return sparkRow.getDecimal(fieldOrdinal);
    }

    @Override
    public MapValue getMap(int rowId) {
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(fieldOrdinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public ArrayValue getArray(int rowId) {
      throw new UnsupportedOperationException("Array not yet supported in DistributedScan");
    }

    @Override
    public ColumnVector getChild(int childOrdinal) {
      if (sparkRow.isNullAt(fieldOrdinal)) {
        throw new IllegalStateException("Cannot get child of null struct");
      }
      Row nested = sparkRow.getStruct(fieldOrdinal);
      StructType nestedSchema = (StructType) parentSchema.at(fieldOrdinal).getDataType();
      return new SparkRowFieldVector(nested, childOrdinal, nestedSchema);
    }
  }

  /**
   * Zero-copy adapter: Spark Row → Kernel {@link io.delta.kernel.data.Row}. Delegates directly to
   * Spark Row accessor methods.
   */
  static final class SparkRowAsKernelRow implements io.delta.kernel.data.Row {
    private final Row sparkRow;
    private final StructType schema;

    SparkRowAsKernelRow(Row sparkRow, StructType schema) {
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
    public BigDecimal getDecimal(int ordinal) {
      return sparkRow.getDecimal(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) sparkRow.get(ordinal);
    }

    @Override
    public ArrayValue getArray(int ordinal) {
      throw new UnsupportedOperationException("Array not yet supported in DistributedScan");
    }

    @Override
    public MapValue getMap(int ordinal) {
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(ordinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public io.delta.kernel.data.Row getStruct(int ordinal) {
      StructType nestedSchema = (StructType) schema.at(ordinal).getDataType();
      return new SparkRowAsKernelRow(sparkRow.getStruct(ordinal), nestedSchema);
    }
  }

  /**
   * Zero-copy adapter: Spark Scala Map → Kernel {@link MapValue}. Used for partition values which
   * are stored as {@code Map<String, String>} in Delta.
   */
  private static final class SparkMapAsKernelMapValue implements MapValue {
    private final List<String> keys;
    private final List<String> values;

    SparkMapAsKernelMapValue(scala.collection.Map<String, String> scalaMap) {
      this.keys = new ArrayList<>();
      this.values = new ArrayList<>();
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
    private final List<String> data;

    StringColumnVector(List<String> data) {
      this.data = data;
    }

    @Override
    public DataType getDataType() {
      return StringType.STRING;
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
    public BigDecimal getDecimal(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MapValue getMap(int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ArrayValue getArray(int rowId) {
      throw new UnsupportedOperationException();
    }
  }
}
