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
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplaceData writer that tracks source file metadata when Spark routes rows through
 * DataAndMetadataWritingSparkTask.
 */
class DeltaReplaceDataWriter implements DataWriter<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaReplaceDataWriter.class);

  private static final int SOURCE_FILE_PATH_METADATA_INDEX = 0;

  private final String targetDirectory;
  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final OutputWriterFactory outputWriterFactory;
  private final StructType partitionSchema;
  private final Map<String, String> partitionValueStrings;
  private final String sessionTimeZone;
  private final int partitionId;
  private final long taskId;
  private final Set<String> sourceFilePaths = new LinkedHashSet<>();

  private OutputWriter writer;
  private Path outputPath;
  private long rowCount;

  DeltaReplaceDataWriter(
      String targetDirectory,
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType dataSchema,
      OutputWriterFactory outputWriterFactory,
      StructType partitionSchema,
      Map<String, String> partitionValueStrings,
      String sessionTimeZone,
      int partitionId,
      long taskId) {
    this.targetDirectory = targetDirectory;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.dataSchema = dataSchema;
    this.outputWriterFactory = outputWriterFactory;
    this.partitionSchema = partitionSchema;
    this.partitionValueStrings = partitionValueStrings;
    this.sessionTimeZone = sessionTimeZone;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.rowCount = 0L;
  }

  @Override
  public void write(InternalRow metadata, InternalRow record) throws IOException {
    if (metadata != null && !metadata.isNullAt(SOURCE_FILE_PATH_METADATA_INDEX)) {
      Object sourceFilePath =
          metadata.get(SOURCE_FILE_PATH_METADATA_INDEX, DataTypes.StringType);
      String sourceFilePathClass =
          sourceFilePath == null ? "null" : sourceFilePath.getClass().getName();
      Preconditions.checkArgument(
          sourceFilePath instanceof UTF8String || sourceFilePath instanceof String,
          "Expected source file path metadata at index %s to be a string, but found %s",
          SOURCE_FILE_PATH_METADATA_INDEX,
          sourceFilePathClass);
      sourceFilePaths.add(sourceFilePath.toString());
    }
    write(record);
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
      return new DeltaReplaceDataCommitMessage(Collections.emptyList(), Collections.emptySet());
    }

    writer.close();
    writer = null;

    List<SerializableKernelRowWrapper> actionRows = generateActionRows();
    return new DeltaReplaceDataCommitMessage(actionRows, sourceFilePaths);
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

  private List<SerializableKernelRowWrapper> generateActionRows() throws IOException {
    Configuration conf = hadoopConf.value();
    Engine engine = DefaultEngine.create(conf);
    Row txnState = serializedTxnState.getRow();
    Map<String, Literal> partitionLiterals =
        PartitionUtils.buildKernelPartitionLiteralMap(
            partitionValueStrings, partitionSchema, ZoneId.of(sessionTimeZone));
    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, partitionLiterals);

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
        LOG.debug("Best-effort close of ReplaceData Parquet writer failed", e);
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
        LOG.debug("Best-effort delete of partially written file {} failed", outputPath, e);
      }
    }
  }

  private void initWriter() {
    Configuration conf = hadoopConf.value();

    TaskAttemptContextImpl taskAttemptContext =
        new TaskAttemptContextImpl(
            new JobConf(conf), new TaskAttemptID("", 0, TaskType.MAP, partitionId, 0));

    String ext = outputWriterFactory.getFileExtension(taskAttemptContext);
    String fileName =
        org.apache.spark.sql.delta.files.DelayedCommitProtocol.buildFileName(
            partitionId, ext, /* isCdc */ false);
    outputPath = new Path(targetDirectory, fileName);

    writer = outputWriterFactory.newInstance(outputPath.toString(), dataSchema, taskAttemptContext);
  }
}
