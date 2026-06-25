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

import io.delta.kernel.Operation;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;

/**
 * The DSv2 {@link Write} for Delta. Builds the executor-side write state shared by batch and
 * streaming -- the {@link DeltaV2DataWriterFactory} (serialized transaction state, target
 * directory, Spark Parquet {@link OutputWriterFactory}, Hadoop conf) -- <b>once</b>, and hands it
 * to the concrete {@link DeltaV2BatchWrite} / {@link DeltaV2StreamingWrite}, which add only their
 * mode-specific commit. The transaction state is operation-independent (it carries schema /
 * protocol / path, not the {@code Operation}), so the same state serves both batch and streaming.
 *
 * <p>Only append is supported; the builder advertises no overwrite capability, so Spark does not
 * route overwrite, truncate, Complete, or Update here.
 */
class DeltaV2Write implements Write {

  // Streaming-write options the sink does not honor; rejected rather than silently ignored.
  private static final List<String> UNSUPPORTED_STREAMING_OPTIONS =
      Arrays.asList(
          "mergeSchema",
          "overwriteSchema",
          "replaceWhere",
          "replaceOn",
          "replaceUsing",
          "partitionOverwriteMode",
          "txnAppId",
          "txnVersion",
          "userMetadata");

  private static String getEngineInfo() {
    return "Delta-Spark-DSv2/" + org.apache.spark.package$.MODULE$.SPARK_VERSION();
  }

  private final Engine engine;
  private final Configuration hadoopConf;
  private final String tablePath;
  private final Snapshot initialSnapshot;
  private final StructType dataSchema;
  private final String queryId;
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
    this.queryId = requireNonNull(writeInfo.queryId(), "queryId is null");
  }

  @Override
  public BatchWrite toBatch() {
    return asBatchAppend();
  }

  @Override
  public StreamingWrite toStreaming() {
    rejectUnsupportedStreamingOptions();
    return asStreamingAppend();
  }

  private BatchWrite asBatchAppend() {
    return new DeltaV2BatchWrite(engine, initialSnapshot, buildDataWriterFactory(Operation.WRITE));
  }

  private StreamingWrite asStreamingAppend() {
    return new DeltaV2StreamingWrite(
        engine, initialSnapshot, queryId, buildDataWriterFactory(Operation.STREAMING_UPDATE));
  }

  /**
   * Builds the executor-side write state shared by batch and streaming: the serialized transaction
   * state and target directory from a Kernel write context, plus the Spark Parquet
   * OutputWriterFactory. Built when the concrete write is selected (after streaming-option
   * rejection), not in the constructor, so unsupported options are rejected before their values are
   * parsed. The transaction built here is used only to extract that state and is then discarded --
   * each concrete write builds its own transaction for the actual commit. {@code operation} is the
   * write mode's operation ({@code WRITE} / {@code STREAMING_UPDATE}); the extracted state is
   * operation-independent, so this only keeps the throwaway transaction consistent with its mode.
   */
  private DeltaV2DataWriterFactory buildDataWriterFactory(Operation operation) {
    Transaction stateTxn =
        requireNonNull(
            initialSnapshot.buildUpdateTableTransaction(getEngineInfo(), operation).build(engine),
            "stateTxn is null");
    Row txnState = requireNonNull(stateTxn.getTransactionState(engine), "txnState is null");
    SerializableKernelRowWrapper serializedTxnState = new SerializableKernelRowWrapper(txnState);
    String targetDirectory =
        requireNonNull(
            Transaction.getWriteContext(engine, txnState, Collections.emptyMap())
                .getTargetDirectory(),
            "targetDirectory is null");

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
        requireNonNull(
            format.prepareWrite(session, job, scalaOpts, dataSchema),
            "outputWriterFactory is null");
    SerializableConfiguration serializableHadoopConf =
        new SerializableConfiguration(job.getConfiguration());

    return new DeltaV2DataWriterFactory(
        targetDirectory,
        serializableHadoopConf,
        serializedTxnState,
        dataSchema,
        outputWriterFactory);
  }

  private void rejectUnsupportedStreamingOptions() {
    CaseInsensitiveStringMap options = writeInfo.options();
    List<String> rejected = new ArrayList<>();
    for (String key : UNSUPPORTED_STREAMING_OPTIONS) {
      if (options.containsKey(key)) {
        rejected.add(key);
      }
    }
    if (!rejected.isEmpty()) {
      throw new UnsupportedOperationException(
          "DSv2 streaming writes to Delta do not support the option(s): "
              + rejected
              + ". Use the V1 write path (format(\"delta\")) if you need them.");
    }
  }
}
