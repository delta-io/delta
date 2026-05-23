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

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * BatchWrite for DSv2 batch append using Spark's Parquet path. Creates a Kernel transaction on the
 * driver, obtains the target directory from the Kernel write context, creates a Spark Parquet
 * OutputWriterFactory via the shared {@link PartitionUtils#createDeltaParquetFileFormat} factory,
 * and serializes everything into a {@link DeltaV2DataWriterFactory} for executor transport.
 *
 * <p>The {@link Transaction} object lives only on the driver and is never serialized. Executors
 * receive only serializable state: transaction state row, Hadoop conf, OutputWriterFactory, schema,
 * and target directory.
 */
class DeltaV2BatchWrite implements Write, BatchWrite {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaV2BatchWrite.class);

  private static String getEngineInfo() {
    return "Delta-Spark-DSv2/" + org.apache.spark.package$.MODULE$.SPARK_VERSION();
  }

  private final Transaction transaction;
  private final Engine engine;

  private final String targetDirectory;
  private final SerializableConfiguration serializableHadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final OutputWriterFactory outputWriterFactory;

  DeltaV2BatchWrite(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      LogicalWriteInfo writeInfo) {
    this.engine = engine;
    this.transaction =
        initialSnapshot
            .buildUpdateTableTransaction(getEngineInfo(), Operation.WRITE)
            .build(this.engine);
    Row txnState = transaction.getTransactionState(this.engine);
    this.serializedTxnState = new SerializableKernelRowWrapper(txnState);

    this.targetDirectory =
        Transaction.getWriteContext(this.engine, txnState, Collections.emptyMap())
            .getTargetDirectory();

    StructType tableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    java.util.Set<String> partitionCols =
        new java.util.HashSet<>(initialSnapshot.getPartitionColumnNames());
    this.dataSchema =
        partitionCols.isEmpty()
            ? tableSchema
            : new StructType(
                java.util.Arrays.stream(tableSchema.fields())
                    .filter(f -> !partitionCols.contains(f.name()))
                    .toArray(org.apache.spark.sql.types.StructField[]::new));

    SparkSession session =
        SparkSession.getActiveSession()
            .getOrElse(
                () -> {
                  throw new IllegalStateException(
                      "SparkSession not active (batch write needs it for Parquet)");
                });

    Job job;
    try {
      job = Job.getInstance(hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Hadoop job for Parquet write", e);
    }
    // TODO: support write-time CDF on batch writes.
    DeltaParquetFileFormatV2 format =
        PartitionUtils.createDeltaParquetFileFormat(
            initialSnapshot,
            tablePath,
            /* optimizationsEnabled */ true,
            /* useMetadataRowIndex */ Option.empty(),
            /* isCDCRead */ false);
    org.apache.spark.sql.execution.datasources.DataSourceUtils.checkFieldNames(format, dataSchema);
    Map<String, String> options = writeInfo.options().asCaseSensitiveMap();
    scala.collection.immutable.Map<String, String> scalaOpts =
        ScalaUtils.toScalaMap(options != null ? options : Collections.emptyMap());
    this.outputWriterFactory = format.prepareWrite(session, job, scalaOpts, dataSchema);
    this.serializableHadoopConf = new SerializableConfiguration(job.getConfiguration());
  }

  @Override
  public BatchWrite toBatch() {
    return this;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new DeltaV2DataWriterFactory(
        targetDirectory,
        serializableHadoopConf,
        serializedTxnState,
        dataSchema,
        outputWriterFactory);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<Row> allActionRows = new ArrayList<>();
    for (WriterCommitMessage msg : messages) {
      if (msg instanceof DeltaV2WriterCommitMessage) {
        for (SerializableKernelRowWrapper wrapper :
            ((DeltaV2WriterCommitMessage) msg).getActionRows()) {
          allActionRows.add(wrapper.getRow());
        }
      }
    }

    CloseableIterable<Row> dataActions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(allActionRows.iterator()));

    TransactionCommitResult result = transaction.commit(engine, dataActions);
    LOG.info("DSv2 batch write committed at version {}", result.getVersion());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.warn(
        "DSv2 batch write aborted. {} task messages will not be committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        messages != null ? messages.length : 0);
  }
}
