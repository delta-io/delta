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

import static java.util.Objects.requireNonNull;

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/** Executor-side writer skeleton for upcoming parquet write implementation. */
public class SparkParquetDataWriter implements DataWriter<InternalRow> {
  private final String tablePath;
  private final String queryId;
  private final List<String> partitionColumnNames;
  private final StructType writeSchema;
  private final String targetDirectory;
  private final SerializableKernelRowWrapper serializableTxnState;
  private final SerializableConfiguration serializableHadoopConf;
  private final int partitionId;
  private final long taskId;
  private long numRowsWritten;

  public SparkParquetDataWriter(
      String tablePath,
      String queryId,
      List<String> partitionColumnNames,
      StructType writeSchema,
      String targetDirectory,
      SerializableKernelRowWrapper serializableTxnState,
      SerializableConfiguration serializableHadoopConf,
      int partitionId,
      long taskId) {
    this.tablePath = requireNonNull(tablePath, "table path is null");
    this.queryId = requireNonNull(queryId, "query id is null");
    this.partitionColumnNames =
        Collections.unmodifiableList(
            requireNonNull(partitionColumnNames, "partition column names is null"));
    this.writeSchema = requireNonNull(writeSchema, "write schema is null");
    this.targetDirectory = requireNonNull(targetDirectory, "target directory is null");
    this.serializableTxnState = requireNonNull(serializableTxnState, "transaction state is null");
    this.serializableHadoopConf = requireNonNull(serializableHadoopConf, "hadoop conf is null");
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.numRowsWritten = 0L;
  }

  @Override
  public void write(InternalRow record) {
    requireNonNull(record, "record is null");
    numRowsWritten++;
  }

  @Override
  public WriterCommitMessage commit() {
    return new SparkParquetWriterCommitMessage(
        partitionId, taskId, numRowsWritten, targetDirectory);
  }

  @Override
  public void abort() {}

  @Override
  public void close() {}
}
