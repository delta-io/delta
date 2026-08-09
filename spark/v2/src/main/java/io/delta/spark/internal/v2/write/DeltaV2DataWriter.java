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
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * DSv2 {@link DataWriter} that writes {@link InternalRow}s to Parquet via Spark's {@link
 * OutputWriter}, then calls Kernel's {@link Transaction#generateAppendActions} to produce Delta log
 * actions (e.g. AddFile). Writers are opened lazily on the first row of each partition and rows
 * stream through directly without buffering.
 *
 * <p><b>Partitioned writes:</b> rows arrive grouped by partition (see {@link DeltaV2Write}), so
 * this writer keeps one open file and rotates it when the partition key changes. The target
 * directory comes from {@link Transaction#getWriteContext}; partition columns are not written into
 * the file.
 *
 * <p><b>V1 reuse:</b> The {@link OutputWriterFactory} is the same one produced by V1's {@code
 * DeltaParquetFileFormatBase.prepareWrite} (via {@code DeltaParquetFileFormatV2}), and file naming
 * follows V1's {@code DelayedCommitProtocol.getFileName} pattern ({@code part-%05d-uuid.parquet}).
 *
 * <p><b>Why a new class instead of reusing V1's {@code DelayedCommitProtocol}:</b> V1's protocol is
 * a {@code FileCommitProtocol} tightly coupled to Spark's {@code FileFormatWriter.write()}
 * execution model. DSv2 requires implementing {@code DataWriter<InternalRow>} instead, which has a
 * fundamentally different lifecycle (write/commit/abort called directly by Spark's V2 execution
 * engine, not orchestrated through a Spark job with task commit coordination).
 */
class DeltaV2DataWriter implements DataWriter<InternalRow> {

  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final int[] dataOrdinals;
  private final int[] partitionOrdinals;
  private final boolean partitioned;
  private final OutputWriterFactory outputWriterFactory;
  private final int partitionId;
  private final long taskId;

  // Kernel driver state, reconstituted once on the executor.
  private Engine engine;
  private Row txnState;

  // Current open file: the context it was opened under (reused to generate its actions at close)
  // and its partition values (to detect the boundary).
  private OutputWriter writer;
  private Path outputPath;
  private DataWriteContext currentWriteContext;
  private Object[] currentPartitionValues;

  // Accumulated across all files closed by this task.
  private final List<SerializableKernelRowWrapper> actionRows = new ArrayList<>();
  private final List<Path> writtenPaths = new ArrayList<>();
  // Rows in the currently-open file; reset on each rotation, recorded as the AddFile numRecords.
  private long rowCount;

  DeltaV2DataWriter(
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType dataSchema,
      StructType partitionSchema,
      int[] dataOrdinals,
      int[] partitionOrdinals,
      OutputWriterFactory outputWriterFactory,
      int partitionId,
      long taskId) {
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.dataSchema = dataSchema;
    this.partitionSchema = partitionSchema;
    this.dataOrdinals = dataOrdinals;
    this.partitionOrdinals = partitionOrdinals;
    this.partitioned = partitionOrdinals.length > 0;
    this.outputWriterFactory = outputWriterFactory;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.rowCount = 0;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // TODO(#7140): honor maxRecordsPerFile -- we never split a partition's rows by row count.
    if (partitioned) {
      Object[] partitionValues =
          PartitionUtils.extractPartitionValues(record, partitionSchema, partitionOrdinals);
      if (writer == null
          || !PartitionUtils.partitionValuesEqual(partitionValues, currentPartitionValues)) {
        releaseCurrentWriter();
        newOutputWriter(
            PartitionUtils.buildPartitionLiterals(record, partitionSchema, partitionOrdinals),
            partitionValues);
      }
      writer.write(getOutputRow(record));
    } else {
      if (writer == null) {
        newOutputWriter(Collections.emptyMap(), /* partitionValues */ null);
      }
      writer.write(record);
    }
    rowCount++;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    // Flush the last open file; a no-op (empty actions) if no rows were written.
    releaseCurrentWriter();
    return new DeltaV2WriterCommitMessage(new ArrayList<>(actionRows));
  }

  @Override
  public void abort() throws IOException {
    closeWriterQuietly();
    for (Path writtenPath : writtenPaths) {
      tryDeleteOutputFile(writtenPath);
    }
  }

  @Override
  public void close() throws IOException {
    closeWriterQuietly();
  }

  /**
   * Opens a new Parquet {@link OutputWriter} under {@code partitionLiterals}' target directory.
   *
   * <p>V2 port of V1's {@code SingleDirectoryDataWriter.newOutputWriter}.
   */
  private void newOutputWriter(Map<String, Literal> partitionLiterals, Object[] partitionValues) {
    // The previous file must be closed first; otherwise its writer is orphaned and its rows are
    // never committed (silent data loss). Every caller closes before opening -- pin that invariant.
    if (writer != null) {
      throw new IllegalStateException("newOutputWriter called with an open writer; close it first");
    }
    ensureEngine();
    Configuration conf = hadoopConf.value();

    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, partitionLiterals);

    TaskAttemptContextImpl taskAttemptContext =
        new TaskAttemptContextImpl(
            new JobConf(conf), new TaskAttemptID("", 0, TaskType.MAP, partitionId, 0));

    String ext = outputWriterFactory.getFileExtension(taskAttemptContext);
    // buildFileName embeds a UUID, so reusing partitionId across a task's files stays unique.
    String fileName =
        org.apache.spark.sql.delta.files.DelayedCommitProtocol.buildFileName(
            partitionId, ext, /* isCdc */ false);
    outputPath = new Path(writeContext.getTargetDirectory(), fileName);
    writtenPaths.add(outputPath);
    currentWriteContext = writeContext;
    currentPartitionValues = partitionValues;
    rowCount = 0;

    writer = outputWriterFactory.newInstance(outputPath.toString(), dataSchema, taskAttemptContext);
  }

  /**
   * Closes the current file (if any) and appends its Delta log actions to {@link #actionRows}.
   *
   * <p>V2 port of V1's {@code FileFormatDataWriter.releaseCurrentWriter}.
   */
  private void releaseCurrentWriter() throws IOException {
    if (writer == null) {
      return;
    }
    writer.close();
    writer = null;
    actionRows.addAll(generateActionRows());
  }

  /**
   * Builds a {@link DataFileStatus} for the written file and calls Kernel's generateAppendActions
   * to produce the Delta log action rows.
   */
  private List<SerializableKernelRowWrapper> generateActionRows() throws IOException {
    Configuration conf = hadoopConf.value();
    FileSystem fs = outputPath.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(outputPath);
    // Populate numRecords so AddFile.stats is non-null: metadata-only count(*) and
    // IcebergCompat V2/V3 validation require it. Column min/max/null stats are not collected here.
    DataFileStatistics stats =
        new DataFileStatistics(
            rowCount,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Optional.empty());
    DataFileStatus dataFileStatus =
        new DataFileStatus(
            outputPath.toString(),
            fileStatus.getLen(),
            fileStatus.getModificationTime(),
            Optional.of(stats));

    CloseableIterator<DataFileStatus> dataFilesIter =
        Utils.toCloseableIterator(Collections.singletonList(dataFileStatus).iterator());

    CloseableIterator<Row> actionRowsIter =
        Transaction.generateAppendActions(engine, txnState, dataFilesIter, currentWriteContext);

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

  /** Reconstitutes the Kernel engine and transaction state once, on first use on the executor. */
  private void ensureEngine() {
    if (engine == null) {
      engine = DefaultEngine.create(hadoopConf.value());
      txnState = serializedTxnState.getRow();
    }
  }

  /**
   * Projects the row to the data columns in {@code dataSchema} order (what the writer expects).
   *
   * <p>V2 port of V1's {@code BaseDynamicPartitionDataWriter.getOutputRow}.
   */
  private InternalRow getOutputRow(InternalRow record) {
    Object[] values = new Object[dataOrdinals.length];
    StructField[] fields = dataSchema.fields();
    for (int i = 0; i < dataOrdinals.length; i++) {
      values[i] = record.get(dataOrdinals[i], fields[i].dataType());
    }
    return new GenericInternalRow(values);
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

  private void tryDeleteOutputFile(Path writtenPath) {
    if (writtenPath != null) {
      try {
        FileSystem fs = writtenPath.getFileSystem(hadoopConf.value());
        fs.delete(writtenPath, false);
      } catch (Exception e) {
        // best-effort cleanup
      }
    }
  }
}
