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
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import org.apache.flink.api.connector.sink2.SinkWriter;
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

  private long estimatedWriteSize = 0L;

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
  }

  /** Add a single row to write buffer, and optionally trigger the write to file. */
  public void write(RowData element, SinkWriter.Context context)
      throws IOException, InterruptedException {
    // TODO upperbound the buffer size
    buffer.add(element);
    highWatermark = Math.max(highWatermark, context.currentWatermark());
    lowWatermark = Math.min(lowWatermark, context.currentWatermark());

    estimatedWriteSize += estimateSize(element);
    if (estimatedWriteSize >= conf.getFileRollingSize()) {
      resultBuffer.addAll(dump());
      estimatedWriteSize = 0L;
    }
  }

  /**
   * Write the current buffer content to file
   *
   * @return
   */
  private Collection<DeltaWriterResult> dump() throws IOException {
    LOG.debug(
        "Writing {} elements to Parquet, partition: {}, jobId: {}, subtaskId: {}, attemptNumber: {}",
        buffer.size(),
        partitionValues,
        jobId,
        subtaskId,
        attemptNumber);
    if (buffer.isEmpty()) {
      return Collections.emptyList();
    }
    final CloseableIterator<FilteredColumnarBatch> logicalData = flinkRowDataAsKernelData();
    // Append job and task information to target directory
    final String pathSuffix = String.format("%s-%d-%d", jobId, subtaskId, attemptNumber);
    final CloseableIterator<Row> actions =
        deltaTable.writeParquet(pathSuffix, logicalData, partitionValues);
    final WriterResultContext context = new WriterResultContext(highWatermark, lowWatermark);
    final Collection<DeltaWriterResult> output =
        actions.map(row -> new DeltaWriterResult(List.of(row), context)).toInMemoryList();
    buffer.clear();
    return output;
  }

  /** End the writer task and flush everything still in buffer to file. */
  public Collection<DeltaWriterResult> complete() throws IOException {
    return dump();
  }

  private long estimateSize(RowData rowData) {
    // TODO provide an estimate to the row data
    return 0;
  }

  private CloseableIterator<FilteredColumnarBatch> flinkRowDataAsKernelData() {
    final int numColumns = writeSchema.length();
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];

    for (int colIdx = 0; colIdx < numColumns; colIdx++) {
      final DataType colDataType = writeSchema.at(colIdx).getDataType();
      columnVectors[colIdx] = new RowDataColumnVectorView(buffer, colIdx, colDataType);
    }

    return Utils.singletonCloseableIterator(
        new FilteredColumnarBatch(
            new DefaultColumnarBatch(buffer.size(), writeSchema, columnVectors),
            Optional.empty() /* selectionVector */));
  }

  static class RowDataColumnVectorView implements ColumnVector {

    private final List<RowData> rows;
    private final int colIdx;
    private final DataType dataType;

    public RowDataColumnVectorView(List<RowData> rows, int colIdx, DataType dataType) {
      this.rows = rows;
      this.colIdx = colIdx;
      this.dataType = dataType;
    }

    @Override
    public DataType getDataType() {
      return this.dataType;
    }

    @Override
    public int getSize() {
      return this.rows.size();
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      checkValidRowId(rowId);
      return rows.get(rowId).isNullAt(colIdx);
    }

    protected void checkValidRowId(int rowId) {
      if (rowId < 0 || rowId >= getSize()) {
        throw new IllegalArgumentException("RowId out of range: " + rowId + " <-> " + getSize());
      }
    }

    protected void checkValidDataType(DataType dataType) {
      if (!this.getDataType().equivalent(dataType)) {
        throw new UnsupportedOperationException("Invalid value request for data type");
      }
    }

    @Override
    public int getInt(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(IntegerType.INTEGER);
      return rows.get(rowId).getInt(colIdx);
    }

    @Override
    public long getLong(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(LongType.LONG);
      return rows.get(rowId).getLong(colIdx);
    }

    @Override
    public String getString(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(StringType.STRING);
      return rows.get(rowId).getString(colIdx).toString();
    }

    @Override
    public float getFloat(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(FloatType.FLOAT);
      return rows.get(rowId).getFloat(colIdx);
    }

    @Override
    public double getDouble(int rowId) {
      checkValidRowId(rowId);
      checkValidDataType(DoubleType.DOUBLE);
      return rows.get(rowId).getDouble(colIdx);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkValidRowId(rowId);
      // Do not check precision and scale here because RowData support conversion
      if (!(this.getDataType() instanceof DecimalType)) {
        throw new UnsupportedOperationException("Invalid value request for data type");
      }
      DecimalType actualType = (DecimalType) dataType;
      return rows.get(rowId)
          .getDecimal(colIdx, actualType.getPrecision(), actualType.getScale())
          .toBigDecimal();
    }
  }
}
