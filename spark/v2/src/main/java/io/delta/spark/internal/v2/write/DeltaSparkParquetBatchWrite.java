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

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;

/**
 * BatchWrite for DSv2 batch append using Spark's Parquet path. Creates a Kernel transaction on the
 * driver, obtains the target directory from the Kernel write context, creates a Spark Parquet
 * OutputWriterFactory via {@link DeltaParquetFileFormatV2}, and serializes everything into a {@link
 * DeltaSparkParquetDataWriterFactory} for executor transport.
 *
 * <p>Commit and abort are not yet implemented and throw {@link UnsupportedOperationException}.
 */
public class DeltaSparkParquetBatchWrite implements BatchWrite {

  // Identifies this connector in Kernel's commit metadata
  private static final String ENGINE_INFO = "delta-spark-dsv2";

  private final String targetDirectory;
  private final SerializableConfiguration serializableHadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType tableSchema;
  private final OutputWriterFactory outputWriterFactory;

  public DeltaSparkParquetBatchWrite(
      Configuration hadoopConf, Snapshot initialSnapshot, Map<String, String> options)
      throws java.io.IOException {
    Engine engine = DefaultEngine.create(hadoopConf);
    Transaction transaction =
        initialSnapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE).build(engine);
    Row txnState = transaction.getTransactionState(engine);
    this.serializedTxnState = new SerializableKernelRowWrapper(txnState);

    this.targetDirectory =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap()).getTargetDirectory();

    this.tableSchema = SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());

    SparkSession session =
        SparkSession.getActiveSession()
            .getOrElse(
                () -> {
                  throw new IllegalStateException(
                      "SparkSession not active (batch write needs it for Parquet)");
                });

    Job job = Job.getInstance(hadoopConf);
    SnapshotImpl snapshotImpl = (SnapshotImpl) initialSnapshot;
    DeltaParquetFileFormatV2 format =
        new DeltaParquetFileFormatV2(
            snapshotImpl.getProtocol(),
            snapshotImpl.getMetadata(),
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            /* optimizationsEnabled */ true,
            Option.empty(),
            /* isCDCRead */ false,
            /* useMetadataRowIndex */ Option.empty());
    scala.collection.immutable.Map<String, String> scalaOpts =
        ScalaUtils.toScalaMap(options != null ? options : Collections.emptyMap());
    this.outputWriterFactory = format.prepareWrite(session, job, scalaOpts, tableSchema);
    this.serializableHadoopConf = new SerializableConfiguration(job.getConfiguration());
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new DeltaSparkParquetDataWriterFactory(
        targetDirectory,
        serializableHadoopConf,
        serializedTxnState,
        tableSchema,
        outputWriterFactory);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException("Batch write is not supported");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException("Batch write is not supported");
  }
}
