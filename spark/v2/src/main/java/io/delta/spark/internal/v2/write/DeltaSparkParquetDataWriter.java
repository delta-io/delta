/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Writes {@link InternalRow}s to Parquet via Spark's {@link OutputWriter}, then calls Kernel's
 * {@link Transaction#generateAppendActions} to produce Delta log actions (e.g. AddFile). The writer
 * is lazily initialized on the first {@link #write} call and rows stream through directly without
 * buffering.
 *
 * <p>The {@link OutputWriterFactory} is prepared once by {@link DeltaSparkParquetBatchWrite} on the
 * driver and transported to executors via {@link DeltaSparkParquetDataWriterFactory}. This writer
 * only calls {@code outputWriterFactory.newInstance(...)} to create its {@link OutputWriter}.
 */
public class DeltaSparkParquetDataWriter implements DataWriter<InternalRow> {

  private final String targetDirectory;
  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType tableSchema;
  private final OutputWriterFactory outputWriterFactory;
  private final int partitionId;
  private final long taskId;

  private OutputWriter writer;
  private Path outputPath;
  private long rowCount;

  public DeltaSparkParquetDataWriter(
      String targetDirectory,
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType tableSchema,
      OutputWriterFactory outputWriterFactory,
      int partitionId,
      long taskId) {
    this.targetDirectory = targetDirectory;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    this.outputWriterFactory = outputWriterFactory;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.rowCount = 0;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    if (writer == null) {
      initWriter();
    }
    writer.write(record);
    rowCount++;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    if (rowCount == 0) {
      return new DeltaWriterCommitMessage(Collections.emptyList());
    }

    writer.close();
    writer = null;

    List<SerializableKernelRowWrapper> actionRows = generateActionRows();
    return new DeltaWriterCommitMessage(actionRows);
  }

  @Override
  public void abort() throws IOException {
    closeWriterQuietly();
    tryDeleteOutputFile();
  }

  @Override
  public void close() throws IOException {
    closeWriterQuietly();
  }

  /**
   * Builds a {@link DataFileStatus} for the written file and calls Kernel's generateAppendActions
   * to produce the Delta log action rows.
   */
  private List<SerializableKernelRowWrapper> generateActionRows() throws IOException {
    Configuration conf = hadoopConf.value();
    Engine engine = DefaultEngine.create(conf);
    Row txnState = serializedTxnState.getRow();
    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

    FileSystem fs = outputPath.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(outputPath);
    DataFileStatus dataFileStatus =
        new DataFileStatus(
            outputPath.toString(),
            fileStatus.getLen(),
            fileStatus.getModificationTime(),
            Optional.empty());

    CloseableIterator<DataFileStatus> dataFilesIter =
        Utils.toCloseableIterator(Collections.singletonList(dataFileStatus).iterator());

    CloseableIterator<Row> actionRowsIter =
        Transaction.generateAppendActions(engine, txnState, dataFilesIter, writeContext);

    List<SerializableKernelRowWrapper> actionRows = new ArrayList<>();
    try {
      while (actionRowsIter.hasNext()) {
        actionRows.add(new SerializableKernelRowWrapper(actionRowsIter.next()));
      }
    } finally {
      actionRowsIter.close();
    }
    return actionRows;
  }

  private void closeWriterQuietly() {
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
        // best-effort cleanup
      }
      writer = null;
    }
  }

  private void tryDeleteOutputFile() {
    if (outputPath != null) {
      try {
        FileSystem fs = outputPath.getFileSystem(hadoopConf.value());
        fs.delete(outputPath, false);
      } catch (Exception e) {
        // best-effort cleanup
      }
    }
  }

  /**
   * Lazily initializes the Parquet OutputWriter. File naming follows V1's {@code
   * DelayedCommitProtocol.getFileName}: {@code part-{split%05d}-{uuid}.parquet}. Random prefix
   * subdirs and CDC-specific naming ({@code cdc-} prefix) will be added when partition and CDC
   * support are wired in.
   */
  private void initWriter() {
    Configuration conf = hadoopConf.value();
    String fileName = String.format("part-%05d-%s.parquet", partitionId, UUID.randomUUID());
    outputPath = new Path(targetDirectory, fileName);

    TaskAttemptContextImpl taskAttemptContext =
        new TaskAttemptContextImpl(
            new JobConf(conf), new TaskAttemptID("", 0, TaskType.REDUCE, partitionId, 0));

    writer =
        outputWriterFactory.newInstance(outputPath.toString(), tableSchema, taskAttemptContext);
  }
}
