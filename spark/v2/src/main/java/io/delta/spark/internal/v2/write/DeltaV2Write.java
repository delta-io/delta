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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;

/**
 * The DSv2 {@link Write} for Delta. Holds the table-level write context (engine, Hadoop conf, table
 * path, snapshot, data schema, write info) and builds the executor-side write state -- a {@link
 * DeltaV2DataWriterFactory} (serialized transaction state, target directory, Spark Parquet {@link
 * OutputWriterFactory}, Hadoop conf) -- from a Kernel {@link Transaction} via {@link
 * #buildDataWriterFactory}. It passes that builder to {@link DeltaV2BatchWrite}, which owns the
 * transaction lifecycle (create, commit).
 *
 * <p>Splitting {@code Write} from {@code BatchWrite} keeps the operation-independent executor-state
 * construction (schema / protocol / path, not the {@code Operation}) separate from the
 * mode-specific transaction lifecycle, so {@link #buildDataWriterFactory} can be reused by future
 * write modes.
 *
 * <p>Only append is supported: the builder advertises no overwrite capability, so Spark does not
 * route overwrite, truncate, Complete, or Update here.
 */
class DeltaV2Write implements Write {

  private final Engine engine;
  private final Configuration hadoopConf;
  private final String tablePath;
  private final Snapshot initialSnapshot;
  private final StructType dataSchema;
  private final LogicalWriteInfo writeInfo;

  DeltaV2Write(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    this.engine = requireNonNull(engine, "engine is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.writeInfo = requireNonNull(writeInfo, "writeInfo is null");
  }

  static String getEngineInfo() {
    return "Delta-Spark-DSv2/" + org.apache.spark.package$.MODULE$.SPARK_VERSION();
  }

  @Override
  public BatchWrite toBatch() {
    return new DeltaV2BatchWrite(engine, initialSnapshot, this::buildDataWriterFactory);
  }

  /**
   * Builds the executor-side write state from {@code transaction}: the serialized transaction state
   * and target directory (from a Kernel write context), the Spark Parquet {@link
   * OutputWriterFactory}, and the Hadoop conf, packaged into a {@link DeltaV2DataWriterFactory}.
   *
   * <p>DSv1 has no analogue: it writes data files inside the transaction scope on the driver
   * ({@code txn.writeFiles}), whereas DSv2 writes them on executors before the driver commits, so
   * the write context must be extracted and serialized ahead of the commit. The extracted state is
   * operation-independent (schema / protocol / path), so any write mode can build its factory here.
   */
  private DeltaV2DataWriterFactory buildDataWriterFactory(Transaction transaction) {
    Row txnState = transaction.getTransactionState(engine);
    SerializableKernelRowWrapper serializedTxnState = new SerializableKernelRowWrapper(txnState);
    // TODO(#7140): pass real partitionValues to support partitioned writes.
    String targetDirectory =
        Transaction.getWriteContext(engine, txnState, /* partitionValues */ Collections.emptyMap())
            .getTargetDirectory();

    SparkSession session =
        SparkSession.getActiveSession()
            .getOrElse(
                () -> {
                  throw new IllegalStateException(
                      "SparkSession not active (write needs it for Parquet)");
                });

    Job job;
    try {
      job = Job.getInstance(hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Hadoop job for Parquet write", e);
    }
    // TODO(#7140): support write-time CDF on batch writes.
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
    OutputWriterFactory outputWriterFactory =
        format.prepareWrite(session, job, scalaOpts, dataSchema);
    SerializableConfiguration serializableHadoopConf =
        new SerializableConfiguration(job.getConfiguration());

    return new DeltaV2DataWriterFactory(
        targetDirectory,
        serializableHadoopConf,
        serializedTxnState,
        dataSchema,
        outputWriterFactory);
  }
}
