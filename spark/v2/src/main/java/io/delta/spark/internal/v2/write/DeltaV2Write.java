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

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The DSv2 {@link Write} for Delta. Holds the table-level write context (engine, Hadoop conf, table
 * path, snapshot, data schema, write info) and dispatches to a mode-specific implementation: {@link
 * #toBatch} builds a {@link DeltaV2BatchWrite} (which owns its own driver-side context and
 * single-transaction commit off {@code initialSnapshot}), and {@link #toStreaming} builds a {@link
 * DeltaV2StreamingWrite} (per-epoch commit off a reloaded snapshot).
 *
 * <p>Both modes obtain their executor-side write state -- a {@link DeltaV2DataWriterFactory} --
 * from the shared {@link DeltaV2WriteContext#buildDataWriterFactory}, which keeps the
 * operation-independent executor-state construction (schema / protocol / path, not the {@code
 * Operation}) separate from the mode-specific transaction lifecycle.
 *
 * <p>Only append is supported: the builder advertises no overwrite capability, so Spark does not
 * route overwrite, truncate, Complete, or Update here.
 *
 * <p><b>Partitioned writes:</b> implements {@link RequiresDistributionAndOrdering} to request Spark
 * to perform a local sort of rows by the partition columns, so each partition's rows reach {@link
 * DeltaV2DataWriter} contiguously. An unpartitioned table requests no ordering.
 */
class DeltaV2Write implements Write, RequiresDistributionAndOrdering {

  // Streaming-write options the sink does not honor; rejected rather than silently ignored.
  // DeltaV2WriteTest asserts over this exact list instead of a drifting copy. Referencing the
  // DeltaOptions constants keeps these names in sync with the canonical option definitions.
  @VisibleForTesting
  static final List<String> UNSUPPORTED_STREAMING_OPTIONS =
      Arrays.asList(
          DeltaOptions.MERGE_SCHEMA_OPTION(),
          DeltaOptions.OVERWRITE_SCHEMA_OPTION(),
          DeltaOptions.REPLACE_WHERE_OPTION(),
          DeltaOptions.REPLACE_ON_OPTION(),
          DeltaOptions.REPLACE_USING_OPTION(),
          DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION(),
          DeltaOptions.TXN_APP_ID(),
          DeltaOptions.TXN_VERSION(),
          DeltaOptions.USER_METADATA_OPTION());

  private final Engine engine;
  private final Configuration hadoopConf;
  private final String tablePath;
  private final Snapshot initialSnapshot;
  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final String queryId;
  private final LogicalWriteInfo writeInfo;

  /**
   * @param initialSnapshot the batch's planned snapshot the write state is built from (and the
   *     streaming guard's schema/protocol baseline)
   * @param snapshotManager reloads the latest snapshot per epoch on the streaming path; unused by
   *     the batch path (a single commit off {@code initialSnapshot})
   * @param dataSchema the non-partition columns (the Parquet file body)
   * @param partitionSchema the partition columns in partition order (empty when unpartitioned)
   */
  DeltaV2Write(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      StructType partitionSchema,
      LogicalWriteInfo writeInfo) {
    this.engine = requireNonNull(engine, "engine is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.writeInfo = requireNonNull(writeInfo, "writeInfo is null");
    this.queryId = requireNonNull(writeInfo.queryId(), "queryId is null");
  }

  /**
   * Returns the engine info string for Delta commit metadata. Delegates to {@link
   * DeltaV2WriteContext#getEngineInfo} so batch and streaming share a single source of truth.
   */
  static String getEngineInfo() {
    return DeltaV2WriteContext.getEngineInfo();
  }

  @Override
  public BatchWrite toBatch() {
    // The batch path builds its own driver-side context (DeltaV2BatchWriteContext) and commits a
    // single transaction off initialSnapshot.
    return new DeltaV2BatchWrite(
        engine, hadoopConf, tablePath, initialSnapshot, dataSchema, partitionSchema, writeInfo);
  }

  @Override
  public StreamingWrite toStreaming() {
    rejectUnsupportedStreamingOptions();
    // Build the operation-independent write context once; the streaming write drives its own
    // per-epoch transactions (Operation.STREAMING_UPDATE) and reuses buildDataWriterFactory to
    // produce the executor write state -- the same setup the batch path uses, no duplication.
    DeltaV2WriteContext context =
        DeltaV2WriteContext.create(
            engine, hadoopConf, tablePath, initialSnapshot, dataSchema, partitionSchema, writeInfo);
    return new DeltaV2StreamingWrite(
        engine, initialSnapshot, snapshotManager, queryId, context::buildDataWriterFactory);
  }

  /**
   * No distribution: we do not repartition the input, matching V1's default sink.
   *
   * <p>TODO(#7140): support optimized write (V1's {@code optimizeWrite}) to consolidate small
   * files.
   */
  @Override
  public Distribution requiredDistribution() {
    return Distributions.unspecified();
  }

  /**
   * Sorts by the partition columns so each partition's rows are contiguous (empty when
   * unpartitioned).
   */
  @Override
  public SortOrder[] requiredOrdering() {
    return Arrays.stream(partitionSchema.fields())
        .map(f -> Expressions.sort(Expressions.column(f.name()), SortDirection.ASCENDING))
        .toArray(SortOrder[]::new);
  }

  /**
   * Rejects streaming-write options the sink does not honor (schema evolution, overwrite/replace,
   * idempotent-txn overrides, user metadata) so a user gets a clear error rather than silent
   * ignore. Append is the only supported mode.
   */
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
