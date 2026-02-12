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
import static org.apache.spark.sql.functions.*;

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
import org.apache.spark.sql.delta.stats.DataFiltersBuilderV2;
import org.apache.spark.sql.sources.Filter;

/**
 * Kernel-compatible Scan that uses distributed DataFrame processing.
 *
 * <p><b>Phase 1 Architecture (aligned with 1DD):</b>
 *
 * <ol>
 *   <li><b>Log Replay (DataFrame)</b>: Distributed deduplication of AddFiles (already done before
 *       this class)
 *   <li><b>Data Skipping (DataFrame)</b>: Parse stats JSON → apply V1 rewrite logic as
 *       df.filter(Column)
 *   <li><b>Output</b>: Kernel ColumnarBatch via SparkRow wrapper at collection boundary
 * </ol>
 *
 * <p>Data skipping is purely DataFrame-based. No OpaquePredicate, no row-level callback. All
 * filtering happens via Spark's distributed execution engine (Catalyst optimized).
 */
public class DistributedScan implements Scan {
  private final Scan delegateScan;
  private final Dataset<org.apache.spark.sql.Row> dataFrame;
  private final SparkSession spark;
  private final boolean maintainOrdering;
  private final Snapshot snapshot;

  /**
   * Create a DistributedScan with DataFrame-based data skipping.
   *
   * <p>If data skipping filters are provided, this constructor will:
   *
   * <ol>
   *   <li>Extract AddFile fields from the "add" struct
   *   <li>Delegate to shared {@code DataFiltersBuilderUtils.applyDataSkipping} (reuses V1 logic:
   *       parse stats JSON, verifyStatsForFilter, apply filter)
   *   <li>Re-wrap the results back into the "add" struct for downstream compatibility
   * </ol>
   *
   * @param spark Spark session
   * @param dataFrame DataFrame with distributed log replay results (schema: {add: struct})
   * @param snapshot Delta snapshot
   * @param readSchema Read schema
   * @param maintainOrdering Whether to preserve DataFrame ordering (required for streaming)
   * @param dataSkippingFilters Raw Spark filters for data skipping (empty array = no skipping)
   */
  public DistributedScan(
      SparkSession spark,
      Dataset<org.apache.spark.sql.Row> dataFrame,
      Snapshot snapshot,
      StructType readSchema,
      boolean maintainOrdering,
      Filter[] dataSkippingFilters) {
    this.spark = spark;
    this.maintainOrdering = maintainOrdering;
    this.snapshot = snapshot;

    // Apply data skipping using shared V1 logic (via DataFiltersBuilderV2 →
    // DataFiltersBuilderUtils)
    if (dataSkippingFilters != null && dataSkippingFilters.length > 0) {
      // Extract flat AddFile fields: {add: {path, stats, ...}} → {path, stats, ...}
      Dataset<org.apache.spark.sql.Row> addFiles = dataFrame.selectExpr("add.*");

      // Delegate to shared data skipping pipeline (statsSchema + from_json + verifyStats + filter)
      Dataset<org.apache.spark.sql.Row> filtered =
          DataFiltersBuilderV2.applyDataSkipping(addFiles, dataSkippingFilters, snapshot, spark);

      // Re-wrap as "add" struct for downstream compatibility
      this.dataFrame = filtered.select(struct(col("*")).as("add"));
    } else {
      this.dataFrame = dataFrame;
    }

    // Build the scan using Kernel's standard API for delegation (getScanState, getRemainingFilter)
    this.delegateScan = snapshot.getScanBuilder().withReadSchema(readSchema).build();
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    // Get the full DataFrame schema
    final io.delta.kernel.types.StructType dataFrameSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            dataFrame.schema());

    // Lazy execution: toLocalIterator() streams data from executors without collecting all
    CloseableIterator<org.apache.spark.sql.Row> rowIterator =
        toCloseableIterator(dataFrame.toLocalIterator());

    // Simply wrap each Spark Row as Kernel ColumnarBatch — no row-level filtering needed
    // Data skipping was already applied as df.filter() in the constructor
    return rowIterator.map(
        sparkRow -> {
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
      return new SparkRowFieldAsColumnVector(sparkRow, ordinal, schema);
    }

    @Override
    public CloseableIterator<Row> getRows() {
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
      org.apache.spark.sql.Row nestedStruct = sparkRow.getStruct(fieldOrdinal);
      StructType nestedSchema = null;
      if (parentSchema != null) {
        nestedSchema = (StructType) parentSchema.at(fieldOrdinal).getDataType();
      }
      return new SparkRowFieldAsColumnVector(nestedStruct, ordinal, nestedSchema);
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
      scala.collection.Map<String, String> scalaMap = sparkRow.getMap(ordinal);
      return new SparkMapAsKernelMapValue(scalaMap);
    }

    @Override
    public Row getStruct(int ordinal) {
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
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return list.get(rowId) == null;
    }

    @Override
    public String getString(int rowId) {
      return list.get(rowId);
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
