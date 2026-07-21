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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
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

/**
 * Shared driver-side setup for Delta DSv2 writes, independent of the write mode's {@code
 * Operation}. Builds the operation-independent write state -- schema split (data / partition),
 * Spark Parquet {@link OutputWriterFactory}, serializable Hadoop conf, session time zone -- once,
 * from the table snapshot. The mode-specific transaction lifecycle lives in subclasses: {@link
 * DeltaV2BatchWriteContext} (batch, {@code Operation.WRITE}) for the batch path, while the
 * streaming path drives per-epoch transactions and reuses only {@link #buildDataWriterFactory}.
 *
 * <p>{@link #buildDataWriterFactory} turns a caller-supplied {@link Transaction} into the executor
 * write state ({@link DeltaV2DataWriterFactory}); it is the single reuse point so batch and
 * streaming never duplicate the Parquet / schema setup.
 */
class DeltaV2WriteContext {

  /** Returns the engine info string for Delta commit metadata. */
  static String getEngineInfo() {
    return "Apache-Spark/"
        + org.apache.spark.package$.MODULE$.SPARK_VERSION()
        + " Delta-Lake/"
        + io.delta.package$.MODULE$.VERSION();
  }

  private final Engine engine;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final io.delta.kernel.types.StructType kernelTableSchema;
  private final OutputWriterFactory outputWriterFactory;
  private final SerializableConfiguration serializableHadoopConf;
  private final String sessionTimeZone;

  static DeltaV2WriteContext create(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    return new DeltaV2WriteContext(
        engine, hadoopConf, tablePath, initialSnapshot, dataSchema, writeInfo);
  }

  private static void verifySchemaForWrite(DeltaParquetFileFormatV2 format, StructType dataSchema) {
    try {
      org.apache.spark.sql.execution.datasources.DataSourceUtils.class
          .getMethod(
              "verifySchema",
              org.apache.spark.sql.execution.datasources.FileFormat.class,
              StructType.class,
              boolean.class)
          .invoke(null, format, dataSchema, false);
    } catch (NoSuchMethodException e) {
      invokeLegacyVerifySchema(format, dataSchema);
    } catch (java.lang.reflect.InvocationTargetException e) {
      throwUnchecked(e.getCause());
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Unable to verify Parquet write schema", e);
    }
  }

  private static void invokeLegacyVerifySchema(
      DeltaParquetFileFormatV2 format, StructType dataSchema) {
    try {
      org.apache.spark.sql.execution.datasources.DataSourceUtils.class
          .getMethod(
              "verifySchema",
              org.apache.spark.sql.execution.datasources.FileFormat.class,
              StructType.class)
          .invoke(null, format, dataSchema);
    } catch (java.lang.reflect.InvocationTargetException e) {
      throwUnchecked(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to verify Parquet write schema", e);
    }
  }

  private static void throwUnchecked(Throwable cause) {
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    throw new IllegalStateException("Unable to verify Parquet write schema", cause);
  }

  protected DeltaV2WriteContext(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    this.engine = engine;

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

    SparkSession session =
        SparkSession.getActiveSession()
            .getOrElse(
                () -> {
                  throw new IllegalStateException(
                      "SparkSession not active (write needs it for Parquet)");
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
    this.dataSchema = format.prepareSchemaForWrite(dataSchema);
    verifySchemaForWrite(format, this.dataSchema);
    org.apache.spark.sql.execution.datasources.DataSourceUtils.checkFieldNames(
        format, this.dataSchema);
    Map<String, String> options = writeInfo.options().asCaseSensitiveMap();
    scala.collection.immutable.Map<String, String> scalaOpts =
        ScalaUtils.toScalaMap(options != null ? options : Collections.emptyMap());
    this.outputWriterFactory = format.prepareWrite(session, job, scalaOpts, this.dataSchema);
    this.serializableHadoopConf = new SerializableConfiguration(job.getConfiguration());
  }

  /**
   * Builds the executor-side write state from {@code transaction}: the serialized transaction state
   * and target directory (from a Kernel write context), packaged with the shared Parquet {@link
   * OutputWriterFactory}, Hadoop conf, and data schema into a {@link DeltaV2DataWriterFactory}.
   *
   * <p>Operation-independent, so any write mode (batch {@code Operation.WRITE}, streaming {@code
   * Operation.STREAMING_UPDATE}) can build its factory from its own transaction without duplicating
   * the driver-side Parquet / schema setup done in the constructor.
   */
  DeltaV2DataWriterFactory buildDataWriterFactory(Transaction transaction) {
    Row txnState = transaction.getTransactionState(engine);
    SerializableKernelRowWrapper serializedTxnState = new SerializableKernelRowWrapper(txnState);
    // TODO(#7140): pass real partitionValues to support partitioned writes.
    String targetDirectory =
        Transaction.getWriteContext(engine, txnState, /* partitionValues */ Collections.emptyMap())
            .getTargetDirectory();
    return new DeltaV2DataWriterFactory(
        targetDirectory,
        serializableHadoopConf,
        serializedTxnState,
        dataSchema,
        outputWriterFactory);
  }

  Engine getEngine() {
    return engine;
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
