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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Per-task writer that buffers Spark {@link InternalRow}s, converts them to Kernel format, runs
 * transformLogicalData → writeParquetFiles → generateAppendActions, and returns a commit message
 * with the serialized Delta log actions.
 *
 * <p>Uses table schema (not LogicalWriteInfo schema) for Kernel so nullability matches the table;
 * the query schema can have different nullability (e.g. non-null id) and Kernel requires exact
 * match. Column order is assumed to match between InternalRow and table schema.
 */
public class DeltaKernelDataWriter implements DataWriter<InternalRow> {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  /** Table schema; used for Kernel and adapter so nullability matches table. */
  private final org.apache.spark.sql.types.StructType tableSchema;

  private final List<String> partitionColumnNames;
  private final Map<String, String> options;
  private final int partitionId;
  private final long taskId;

  private final List<InternalRow> rowBuffer = new ArrayList<>();
  private final StructType kernelSchema;

  public DeltaKernelDataWriter(
      String tablePath,
      Configuration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      org.apache.spark.sql.types.StructType tableSchema,
      List<String> partitionColumnNames,
      Map<String, String> options,
      int partitionId,
      long taskId) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : Collections.emptyList();
    this.options = options != null ? options : Collections.emptyMap();
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(tableSchema);
  }

  @Override
  public void write(InternalRow record) {
    rowBuffer.add(projectDataColumns(record).copy());
  }

  /**
   * Projects the InternalRow to only include data columns matching the table schema. In COW
   * operations, Spark may prepend metadata columns (e.g. file_path) to the row, causing a mismatch
   * between record.numFields() and tableSchema.length(). This method strips the leading metadata
   * columns by computing an offset.
   */
  private InternalRow projectDataColumns(InternalRow record) {
    int numDataCols = tableSchema.length();
    if (record.numFields() == numDataCols) {
      return record;
    }
    int offset = record.numFields() - numDataCols;
    org.apache.spark.sql.catalyst.expressions.GenericInternalRow projected =
        new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(numDataCols);
    for (int i = 0; i < numDataCols; i++) {
      projected.update(i, record.get(i + offset, tableSchema.apply(i).dataType()));
    }
    return projected;
  }

  @Override
  public WriterCommitMessage commit() {
    if (rowBuffer.isEmpty()) {
      return new DeltaKernelWriterCommitMessage(Collections.emptyList());
    }

    Engine engine = DefaultEngine.create(hadoopConf);
    Row txnState = serializedTxnState.getRow();

    // Convert buffered InternalRows to Kernel Rows (table schema so nullability matches)
    List<Row> kernelRows = new ArrayList<>(rowBuffer.size());
    for (InternalRow internalRow : rowBuffer) {
      kernelRows.add(new InternalRowToKernelRowAdapter(internalRow, tableSchema, kernelSchema));
    }

    // Single partition (unpartitioned) for first version
    Map<String, Literal> partitionValues = Collections.emptyMap();

    ColumnarBatch batch = new DefaultRowBasedColumnarBatch(kernelSchema, kernelRows);
    FilteredColumnarBatch filteredBatch =
        new FilteredColumnarBatch(batch, java.util.Optional.empty());

    CloseableIterator<FilteredColumnarBatch> dataIter =
        Utils.toCloseableIterator(Collections.singletonList(filteredBatch).iterator());

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(engine, txnState, dataIter, partitionValues);

    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

    CloseableIterator<DataFileStatus> dataFiles;
    try {
      dataFiles =
          engine
              .getParquetHandler()
              .writeParquetFiles(
                  writeContext.getTargetDirectory(),
                  physicalData,
                  writeContext.getStatisticsColumns());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write Parquet files", e);
    }

    CloseableIterator<Row> actionRowsIter =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);

    List<SerializableKernelRowWrapper> serializedActions = new ArrayList<>();
    try {
      while (actionRowsIter.hasNext()) {
        serializedActions.add(new SerializableKernelRowWrapper(actionRowsIter.next()));
      }
    } finally {
      try {
        actionRowsIter.close();
      } catch (Exception ignored) {
        // best effort
      }
    }

    return new DeltaKernelWriterCommitMessage(serializedActions);
  }

  @Override
  public void abort() {
    // No Kernel commit; buffer is discarded.
  }

  @Override
  public void close() {
    // No-op; resources are released in commit() or abort().
  }
}
