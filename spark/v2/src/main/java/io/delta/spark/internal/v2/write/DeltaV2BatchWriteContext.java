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
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;

/** Shared driver-side setup for Delta DSv2 batch writes. */
class DeltaV2BatchWriteContext {

  /** Returns the engine info string for Delta commit metadata. */
  static String getEngineInfo() {
    return "Apache-Spark/"
        + org.apache.spark.package$.MODULE$.SPARK_VERSION()
        + " Delta-Lake/"
        + io.delta.package$.MODULE$.VERSION();
  }

  private final Transaction transaction;
  private final Engine engine;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final io.delta.kernel.types.StructType kernelTableSchema;
  private final OutputWriterFactory outputWriterFactory;
  private final SerializableConfiguration serializableHadoopConf;
  private final String sessionTimeZone;

  static DeltaV2BatchWriteContext create(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      LogicalWriteInfo writeInfo) {
    return new DeltaV2BatchWriteContext(engine, hadoopConf, tablePath, initialSnapshot, writeInfo);
  }

  private DeltaV2BatchWriteContext(
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

    this.kernelTableSchema = initialSnapshot.getSchema();
    StructType tableSchema = SchemaUtils.convertKernelSchemaToSparkSchema(kernelTableSchema);
    java.util.Set<String> partitionCols =
        new java.util.HashSet<>(initialSnapshot.getPartitionColumnNames());
    this.partitionSchema =
        partitionCols.isEmpty()
            ? new StructType()
            : new StructType(
                java.util.Arrays.stream(tableSchema.fields())
                    .filter(field -> partitionCols.contains(field.name()))
                    .toArray(org.apache.spark.sql.types.StructField[]::new));
    this.dataSchema =
        partitionCols.isEmpty()
            ? tableSchema
            : new StructType(
                java.util.Arrays.stream(tableSchema.fields())
                    .filter(field -> !partitionCols.contains(field.name()))
                    .toArray(org.apache.spark.sql.types.StructField[]::new));

    SparkSession session =
        SparkSession.getActiveSession()
            .getOrElse(
                () -> {
                  throw new IllegalStateException(
                      "SparkSession not active (batch write needs it for Parquet)");
                });
    this.sessionTimeZone = session.sessionState().conf().sessionLocalTimeZone();

    Job job;
    try {
      job = Job.getInstance(hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Hadoop job for Parquet write", e);
    }
    DeltaParquetFileFormatV2 format =
        PartitionUtils.createDeltaParquetFileFormat(
            initialSnapshot,
            tablePath,
            /* optimizationsEnabled */ true,
            /* useMetadataRowIndex */ Option.empty(),
            /* isCDCRead */ false);
    org.apache.spark.sql.execution.datasources.DataSourceUtils.verifySchema(format, dataSchema);
    org.apache.spark.sql.execution.datasources.DataSourceUtils.checkFieldNames(format, dataSchema);
    Map<String, String> options = writeInfo.options().asCaseSensitiveMap();
    scala.collection.immutable.Map<String, String> scalaOpts =
        ScalaUtils.toScalaMap(options != null ? options : Collections.emptyMap());
    this.outputWriterFactory = format.prepareWrite(session, job, scalaOpts, dataSchema);
    this.serializableHadoopConf = new SerializableConfiguration(job.getConfiguration());
  }

  Transaction getTransaction() {
    return transaction;
  }

  Engine getEngine() {
    return engine;
  }

  String getTargetDirectory(Map<String, Literal> partitionLiterals) {
    return Transaction.getWriteContext(engine, serializedTxnState.getRow(), partitionLiterals)
        .getTargetDirectory();
  }

  SerializableKernelRowWrapper getSerializedTxnState() {
    return serializedTxnState;
  }

  StructType getDataSchema() {
    return dataSchema;
  }

  StructType getPartitionSchema() {
    return partitionSchema;
  }

  io.delta.kernel.types.StructType getKernelTableSchema() {
    return kernelTableSchema;
  }

  OutputWriterFactory getOutputWriterFactory() {
    return outputWriterFactory;
  }

  SerializableConfiguration getSerializableHadoopConf() {
    return serializableHadoopConf;
  }

  ZoneId getSessionTimeZone() {
    return ZoneId.of(sessionTimeZone);
  }

  String getSessionTimeZoneId() {
    return sessionTimeZone;
  }
}
