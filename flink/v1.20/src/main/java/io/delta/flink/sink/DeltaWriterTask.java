/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DeltaWriterTask} represents a runnable unit of work responsible for writing row data to a
 * single Parquet file as part of a Delta write operation.
 *
 * <p>Each writer task processes a subset of input data, writes it to exactly one Parquet file, and
 * produces the corresponding Delta {@code AddFile} action describing the written file.
 *
 * <p>{@code DeltaWriterTask} instances are designed to be executed concurrently as part of a
 * multi-threaded write pipeline, allowing a single sink writer to parallelize file generation while
 * maintaining isolation between output files.
 *
 * <p>The result of executing a {@code DeltaWriterTask} is a wrapped Delta action that is later
 * collected into a {@link DeltaWriterResult} and emitted at checkpoint boundaries.
 */
public class DeltaWriterTask {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterTask.class);

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;
  private final Map<String, Literal> partitionValues;

  private final DeltaTable deltaTable;
  private final DeltaSinkConf conf;

  private final StructType writeSchema;
  private final List<RowData> buffer;

  private final List<DeltaWriterResult> resultBuffer;

  private final DeltaSinkConf.FileRollingStrategy fileRollingStrategy;

  private long lowWatermark = Long.MAX_VALUE;
  private long highWatermark = -1;

  public DeltaWriterTask(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable deltaTable,
      DeltaSinkConf conf,
      Map<String, Literal> partitionValues) {
    this.jobId = jobId;
    this.subtaskId = subtaskId;
    this.attemptNumber = attemptNumber;

    this.buffer = new ArrayList<>();
    this.resultBuffer = new ArrayList<>();

    this.partitionValues = partitionValues;
    this.deltaTable = deltaTable;
    this.conf = conf;
    this.writeSchema = conf.getSinkSchema();

    this.fileRollingStrategy = conf.createFileRollingStrategy();
  }

  protected List<DeltaWriterResult> getResultBuffer() {
    return resultBuffer;
  }

  /** Add a single row to write buffer, and optionally trigger the write to file. */
  public void write(RowData element, SinkWriter.Context context)
      throws IOException, InterruptedException {
    // upperbound the buffer size with file rolling strategy
    buffer.add(element);
    highWatermark = Math.max(highWatermark, context.currentWatermark());
    lowWatermark = Math.min(lowWatermark, context.currentWatermark());

    if (fileRollingStrategy.shouldRoll(element)) {
      dump();
    }
  }

  /**
   * Write the current buffer content to file and store the generated writer results in resultBuffer
   */
  private void dump() throws IOException {
    LOG.debug(
        "Writing {} elements to Parquet, partition: {}, jobId: {}, subtaskId: {}, attemptNumber: {}",
        buffer.size(),
        partitionValues,
        jobId,
        subtaskId,
        attemptNumber);
    if (buffer.isEmpty()) {
      return;
    }
    final CloseableIterator<FilteredColumnarBatch> logicalData = flinkRowDataAsKernelData();
    // Append job and task information to target directory
    final String pathSuffix = String.format("%s-%d-%d", jobId, subtaskId, attemptNumber);
    final CloseableIterator<Row> actions =
        deltaTable.writeParquet(pathSuffix, logicalData, partitionValues);
    final WriterResultContext context = new WriterResultContext(highWatermark, lowWatermark);
    final Collection<DeltaWriterResult> output =
        actions.map(row -> new DeltaWriterResult(List.of(row), context)).toInMemoryList();
    resultBuffer.addAll(output);

    this.highWatermark = -1L;
    this.lowWatermark = Long.MAX_VALUE;
    buffer.clear();
  }

  /** End the writer task and flush everything still in buffer to file. */
  public Collection<DeltaWriterResult> complete() throws IOException {
    dump();
    return resultBuffer;
  }

  private CloseableIterator<FilteredColumnarBatch> flinkRowDataAsKernelData() {
    final int numColumns = writeSchema.length();
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];
    RowAccess rowAccess = new RowDataListAccess(buffer);
    for (int colIdx = 0; colIdx < numColumns; colIdx++) {
      final DataType colDataType = writeSchema.at(colIdx).getDataType();
      columnVectors[colIdx] = new RowDataColumnVectorView(rowAccess, colIdx, colDataType);
    }
    return Utils.singletonCloseableIterator(
        new FilteredColumnarBatch(
            new DefaultColumnarBatch(buffer.size(), writeSchema, columnVectors),
            Optional.empty() /* selectionVector */));
  }

  /* ==================================
   *     Supporting Classes
   * ==================================*/
  abstract static class AbstractColumnVectorView implements ColumnVector {

    protected final DataType dataType;

    public AbstractColumnVectorView(DataType dataType) {
      this.dataType = dataType;
    }

    protected void checkValidRowId(int rowId) {
      if (rowId < 0 || rowId >= getSize()) {
        throw new IllegalArgumentException("RowId out of range: " + rowId + " <-> " + getSize());
      }
    }

    protected void checkValidDataType(DataType dataType) {
      if (!this.getDataType().equivalent(dataType)) {
        throw new UnsupportedOperationException(
            "Invalid value request for data type: " + this.dataType + " <> " + dataType);
      }
    }

    public ArrayValue getArray(ArrayData arrayData) {
      ArrayType arrayType = (ArrayType) getDataType();
      ColumnVector elementView =
          new ArrayDataColumnVectorView(arrayData, arrayType.getElementType());
      return new ArrayValue() {
        @Override
        public int getSize() {
          return arrayData.size();
        }

        @Override
        public ColumnVector getElements() {
          return elementView;
        }
      };
    }

    public MapValue getMap(MapData mapData) {
      MapType mapType = (MapType) getDataType();
      ColumnVector keyView =
          new ArrayDataColumnVectorView(mapData.keyArray(), mapType.getKeyType());
      ColumnVector valueView =
          new ArrayDataColumnVectorView(mapData.valueArray(), mapType.getValueType());
      return new MapValue() {
        @Override
        public int getSize() {
          return mapData.size();
        }

        @Override
        public ColumnVector getKeys() {
          return keyView;
        }

        @Override
        public ColumnVector getValues() {
          return valueView;
        }
      };
    }
  }

  interface RowAccess {
    RowData get(int index);

    int size();
  }

  static class RowDataListAccess implements RowAccess {
    final List<RowData> rows;

    protected RowDataListAccess(List<RowData> rows) {
      this.rows = rows;
    }

    @Override
    public RowData get(int index) {
      return rows.get(index);
    }

    @Override
    public int size() {
      return rows.size();
    }
  }

  static class ArrayRowAccess implements RowAccess {
    final ArrayData arrayData;
    final int numFields;

    public ArrayRowAccess(ArrayData arrayData, int numFields) {
      this.arrayData = arrayData;
      this.numFields = numFields;
    }

    @Override
    public RowData get(int index) {
      return arrayData.getRow(index, numFields);
    }

    @Override
    public int size() {
      return arrayData.size();
    }
  }

  static class RowDataColumnVectorView extends AbstractColumnVectorView {
    private final RowAccess rowAccess;
    private final int[] path;
    private final int[] nestedSize;
    private final int index;

    public RowDataColumnVectorView(RowAccess rowAccess, int index, DataType dataType) {
      this(rowAccess, new int[] {index}, new int[0], dataType);
    }

    public RowDataColumnVectorView(
        RowAccess rowAccess, int[] path, int[] nestedSize, DataType dataType) {
      super(dataType);
      this.rowAccess = rowAccess;
      this.path = path;
      this.nestedSize = nestedSize;
      this.index = this.path[this.path.length - 1];
    }

    @Override
    public DataType getDataType() {
      return this.dataType;
    }

    @Override
    public int getSize() {
      return this.rowAccess.size();
    }

    @Override
    public void close() {}

    protected RowData extract(int rowId) {
      RowData current = rowAccess.get(rowId);
      for (int i = 0; i < path.length - 1; i++) {
        if (current.isNullAt(path[i])) {
          return null;
        }
        current = current.getRow(path[i], nestedSize[i]);
      }
      return current;
    }

    @Override
    public boolean isNullAt(int rowId) {
      checkValidRowId(rowId);
      RowData current = extract(rowId);
      if (current == null) {
        return true;
      }
      return current.isNullAt(index);
    }

    @Override
    public boolean getBoolean(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(BooleanType.BOOLEAN);
      return extract(rowId).getBoolean(index);
    }

    @Override
    public byte[] getBinary(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(BinaryType.BINARY);
      return extract(rowId).getBinary(index);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkValidRowId(rowId);
      // Do not check precision and scale here because RowData support conversion
      if (!(this.getDataType() instanceof DecimalType)) {
        throw new UnsupportedOperationException("Invalid value request for data type");
      }
      DecimalType actualType = (DecimalType) dataType;
      return extract(rowId)
          .getDecimal(index, actualType.getPrecision(), actualType.getScale())
          .toBigDecimal();
    }

    @Override
    public double getDouble(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(DoubleType.DOUBLE);
      return extract(rowId).getDouble(index);
    }

    @Override
    public float getFloat(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(FloatType.FLOAT);
      return extract(rowId).getFloat(index);
    }

    @Override
    public int getInt(int rowId) {
      checkValidRowId(rowId);
      if (getDataType().equivalent(IntegerType.INTEGER)
          || getDataType().equivalent(DateType.DATE)) {
        return extract(rowId).getInt(index);
      } else {
        throw new UnsupportedOperationException(
            "Invalid value request for data type: " + this.dataType);
      }
    }

    @Override
    public long getLong(int rowId) {
      checkValidRowId(rowId);
      if (getDataType().equivalent(LongType.LONG)) {
        return extract(rowId).getLong(index);
      } else if (getDataType().equivalent(TimestampType.TIMESTAMP)
          || getDataType().equivalent(TimestampNTZType.TIMESTAMP_NTZ)) {
        return Conversions.FlinkToDelta.timestamp(extract(rowId).getTimestamp(index, 6));
      } else {
        throw new UnsupportedOperationException(
            "Invalid value request for data type: " + this.dataType);
      }
    }

    @Override
    public String getString(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(StringType.STRING);
      return extract(rowId).getString(index).toString();
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      int[] childPath = new int[path.length + 1];
      int[] childNestedSize = new int[nestedSize.length + 1];
      System.arraycopy(path, 0, childPath, 0, path.length);
      System.arraycopy(nestedSize, 0, childNestedSize, 0, nestedSize.length);
      StructType nested = (StructType) getDataType();
      childPath[path.length] = ordinal;
      childNestedSize[nestedSize.length] = nested.length();
      return new RowDataColumnVectorView(
          rowAccess, childPath, childNestedSize, nested.fields().get(ordinal).getDataType());
    }

    @Override
    public ArrayValue getArray(int rowId) {
      ArrayData arrayData = extract(rowId).getArray(index);
      return getArray(arrayData);
    }

    @Override
    public MapValue getMap(int rowId) {
      MapData mapData = extract(rowId).getMap(index);
      return getMap(mapData);
    }
  }

  static class ArrayDataColumnVectorView extends AbstractColumnVectorView {

    final ArrayData data;

    public ArrayDataColumnVectorView(ArrayData data, DataType elementType) {
      super(elementType);
      this.data = data;
    }

    @Override
    public DataType getDataType() {
      return super.dataType;
    }

    @Override
    public int getSize() {
      return data.size();
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      checkValidRowId(rowId);
      return data.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(BooleanType.BOOLEAN);
      return data.getBoolean(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(BinaryType.BINARY);
      return data.getBinary(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkValidRowId(rowId);
      // Do not check precision and scale here because RowData support conversion
      if (!(this.getDataType() instanceof DecimalType)) {
        throw new UnsupportedOperationException("Invalid value request for data type");
      }
      DecimalType actualType = (DecimalType) dataType;
      return data.getDecimal(rowId, actualType.getPrecision(), actualType.getScale())
          .toBigDecimal();
    }

    @Override
    public double getDouble(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(DoubleType.DOUBLE);
      return data.getDouble(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(FloatType.FLOAT);
      return data.getFloat(rowId);
    }

    @Override
    public int getInt(int rowId) {
      checkValidRowId(rowId);
      if (getDataType().equivalent(IntegerType.INTEGER)
          || getDataType().equivalent(DateType.DATE)) {
        return data.getInt(rowId);
      } else {
        throw new UnsupportedOperationException(
            "Invalid value request for data type: " + this.dataType);
      }
    }

    @Override
    public long getLong(int rowId) {
      checkValidRowId(rowId);
      if (getDataType().equivalent(LongType.LONG)) {
        return data.getLong(rowId);
      } else if (getDataType().equivalent(TimestampType.TIMESTAMP)
          || getDataType().equivalent(TimestampNTZType.TIMESTAMP_NTZ)) {
        return Conversions.FlinkToDelta.timestamp(data.getTimestamp(rowId, 6));
      } else {
        throw new UnsupportedOperationException(
            "Invalid value request for data type: " + this.dataType);
      }
    }

    @Override
    public String getString(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(StringType.STRING);
      return data.getString(rowId).toString();
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      StructType nested = (StructType) getDataType();
      int nestedSize = nested.fields().size();
      return new RowDataColumnVectorView(
          new ArrayRowAccess(data, nestedSize),
          ordinal,
          nested.fields().get(ordinal).getDataType());
    }

    @Override
    public ArrayValue getArray(int rowId) {
      return getArray(data.getArray(rowId));
    }

    @Override
    public MapValue getMap(int rowId) {
      return getMap(data.getMap(rowId));
    }
  }
}
