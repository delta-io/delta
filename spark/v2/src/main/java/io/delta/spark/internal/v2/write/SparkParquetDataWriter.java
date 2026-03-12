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
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter;
import org.apache.spark.sql.types.StructType;

/**
 * Per-task writer that uses Spark's native Parquet writer ({@link ParquetOutputWriter}) to write
 * data files directly from {@link InternalRow}s. No row conversion to Kernel format is needed.
 *
 * <p>This is the Option B write path: Spark writes Parquet, Kernel only commits. Column mapping is
 * handled by the physical write schema (with physical column names and field IDs) computed on the
 * driver and embedded in the serialized Hadoop config via {@code ParquetWriteSupport.setSchema()}.
 *
 * <p>After writing, file metadata is collected as {@link DataFileStatus} and passed to {@link
 * Transaction#generateAppendActions} to produce AddFile action rows for the Kernel commit.
 */
public class SparkParquetDataWriter implements DataWriter<InternalRow> {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType tableSchema;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;
  private final Map<String, String> options;
  private final int partitionId;
  private final long taskId;

  private final List<InternalRow> rowBuffer = new ArrayList<>();
  private ParquetOutputWriter outputWriter;
  private String outputFilePath;

  public SparkParquetDataWriter(
      String tablePath,
      Configuration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType tableSchema,
      StructType writeSchema,
      List<String> partitionColumnNames,
      Map<String, String> options,
      int partitionId,
      long taskId) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    this.writeSchema = writeSchema;
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : Collections.emptyList();
    this.options = options != null ? options : Collections.emptyMap();
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  @Override
  public void write(InternalRow record) throws IOException {
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

  private void initWriter() {
    Engine engine = DefaultEngine.create(hadoopConf);
    Row txnState = serializedTxnState.getRow();
    Map<String, Literal> partitionValues = Collections.emptyMap();
    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);
    String targetDirectory = writeContext.getTargetDirectory();

    String uuid = UUID.randomUUID().toString();
    this.outputFilePath =
        String.format("%s/part-%05d-%s.snappy.parquet", targetDirectory, partitionId, uuid);

    // Ensure the write schema is embedded in the Hadoop config for ParquetOutputWriter.
    // prepareWrite() on the driver already set this, but the config was serialized as a map
    // and reconstructed — we set it again to be safe.
    org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport.setSchema(
        writeSchema, hadoopConf);

    TaskAttemptID taskAttemptID =
        new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, partitionId), (int) taskId);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptID);

    this.outputWriter = new ParquetOutputWriter(outputFilePath, taskContext);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    if (rowBuffer.isEmpty()) {
      return new DeltaKernelWriterCommitMessage(Collections.emptyList());
    }

    // Write all buffered rows to Parquet
    initWriter();
    for (InternalRow row : rowBuffer) {
      outputWriter.write(row);
    }
    outputWriter.close();

    // Stat the written file
    Path hadoopPath = new Path(outputFilePath);
    FileSystem fs = hadoopPath.getFileSystem(hadoopConf);
    org.apache.hadoop.fs.FileStatus fileStatus = fs.getFileStatus(hadoopPath);
    String qualifiedPath = fs.makeQualified(hadoopPath).toString();

    DataFileStatus dataFileStatus =
        new DataFileStatus(
            qualifiedPath, fileStatus.getLen(), fileStatus.getModificationTime(), Optional.empty());

    // Generate AddFile actions via Kernel
    Engine engine = DefaultEngine.create(hadoopConf);
    Row txnState = serializedTxnState.getRow();
    Map<String, Literal> partitionValues = Collections.emptyMap();
    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

    CloseableIterator<DataFileStatus> fileStatusIter =
        Utils.toCloseableIterator(Collections.singletonList(dataFileStatus).iterator());
    CloseableIterator<Row> actionRowsIter =
        Transaction.generateAppendActions(engine, txnState, fileStatusIter, writeContext);

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
  public void abort() throws IOException {
    if (outputWriter != null) {
      outputWriter.close();
    }
    // Optionally delete the partial file; Kernel transaction won't commit it.
  }

  @Override
  public void close() throws IOException {
    // Resources released in commit() or abort().
  }
}
