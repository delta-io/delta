package io.delta.flink.sink;

import io.delta.flink.DeltaTable;
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
 * Each writer task is responsible for writing row data to a single Parquet and return the wrapped
 * add file action.
 */
public class DeltaWriterTask {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterTask.class);

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;
  private final Map<String, Literal> partitionValues;
  private final DeltaTable deltaTable;

  private final StructType writeSchema;
  private final List<RowData> buffer;

  public DeltaWriterTask(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable deltaTable,
      Map<String, Literal> partitionValues) {
    this.jobId = jobId;
    this.subtaskId = subtaskId;
    this.attemptNumber = attemptNumber;

    this.buffer = new ArrayList<>();
    this.partitionValues = partitionValues;
    this.deltaTable = deltaTable;
    this.writeSchema = deltaTable.getSchema();
  }

  public void write(RowData element, SinkWriter.Context context)
      throws IOException, InterruptedException {
    buffer.add(element);
  }

  public Collection<DeltaWriterResult> complete() throws IOException {
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
    final Collection<DeltaWriterResult> output =
        actions.map(row -> new DeltaWriterResult(List.of(row))).toInMemoryList();
    buffer.clear();
    return output;
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
