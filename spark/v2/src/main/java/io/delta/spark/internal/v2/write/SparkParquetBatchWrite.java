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

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * BatchWrite construction object for DSv2 batch append.
 *
 * <p>Subsequent changes will implement factory creation and commit behavior.
 */
public class SparkParquetBatchWrite implements BatchWrite {
  private static final String ENGINE_INFO = "Spark-Delta-Kernel-DSv2";

  private final String tablePath;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final StructType alignedWriteSchema;
  private final String queryId;
  private final Map<String, String> options;
  private final List<String> partitionColumnNames;
  private final Engine engine;
  private final Transaction transaction;
  private final Row txnState;
  private final SerializableKernelRowWrapper serializableTxnState;
  private final SerializableConfiguration serializableHadoopConf;
  private final DataWriteContext writeContext;
  private final String targetDirectory;

  public SparkParquetBatchWrite(
      String tablePath,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      StructType writeSchema,
      String queryId,
      Map<String, String> options,
      List<String> partitionColumnNames) {
    this.tablePath = requireNonNull(tablePath, "table path is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoop conf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initial snapshot is null");
    StructType tableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(this.initialSnapshot.getSchema());
    this.alignedWriteSchema = alignWriteSchema(writeSchema, tableSchema);
    this.queryId = requireNonNull(queryId, "query id is null");
    this.options = Collections.unmodifiableMap(requireNonNull(options, "options is null"));
    this.partitionColumnNames =
        Collections.unmodifiableList(
            requireNonNull(partitionColumnNames, "partition column names is null"));

    this.engine = DefaultEngine.create(this.hadoopConf);
    this.transaction =
        this.initialSnapshot
            .buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE)
            .build(this.engine);
    this.txnState = transaction.getTransactionState(this.engine);
    this.serializableTxnState =
        new SerializableKernelRowWrapper(
            requireNonNull(this.txnState, "transaction state is null"));
    this.serializableHadoopConf = new SerializableConfiguration(this.hadoopConf);
    this.writeContext =
        Transaction.getWriteContext(this.engine, this.txnState, new HashMap<String, Literal>());
    this.targetDirectory =
        requireNonNull(writeContext.getTargetDirectory(), "target directory is null");
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new SparkParquetDataWriterFactory(
        tablePath,
        queryId,
        partitionColumnNames,
        alignedWriteSchema,
        targetDirectory,
        serializableTxnState,
        serializableHadoopConf);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<SparkParquetWriterCommitMessage> decodedMessages =
        SparkParquetCommitMessageUtils.decodeMessages(messages);
    long totalRowsWritten = SparkParquetCommitMessageUtils.totalRows(decodedMessages);
    if (totalRowsWritten == 0L) {
      // Keep zero-row commit as a no-op: no files were produced, so there is nothing to append.
      return;
    }
    throw new UnsupportedOperationException(
        "Driver append-action generation and transaction commit are implemented in follow-up changes");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {}

  String getTablePath() {
    return tablePath;
  }

  Configuration getHadoopConf() {
    return hadoopConf;
  }

  Snapshot getInitialSnapshot() {
    return initialSnapshot;
  }

  StructType getWriteSchema() {
    return alignedWriteSchema;
  }

  String getQueryId() {
    return queryId;
  }

  Map<String, String> getOptions() {
    return options;
  }

  List<String> getPartitionColumnNames() {
    return partitionColumnNames;
  }

  Engine getEngine() {
    return engine;
  }

  Transaction getTransaction() {
    return transaction;
  }

  Row getTxnState() {
    return txnState;
  }

  SerializableKernelRowWrapper getSerializableTxnState() {
    return serializableTxnState;
  }

  SerializableConfiguration getSerializableHadoopConf() {
    return serializableHadoopConf;
  }

  DataWriteContext getWriteContext() {
    return writeContext;
  }

  String getTargetDirectory() {
    return targetDirectory;
  }

  private static StructType alignWriteSchema(StructType writeSchema, StructType tableSchema) {
    StructType nonNullWriteSchema = requireNonNull(writeSchema, "write schema is null");
    StructType nonNullTableSchema = requireNonNull(tableSchema, "table schema is null");
    if (!DataType.equalsIgnoreNullability(nonNullWriteSchema, nonNullTableSchema)) {
      throw new IllegalArgumentException(
          "Write schema does not match table schema after nullability normalization");
    }
    return nonNullTableSchema;
  }
}
