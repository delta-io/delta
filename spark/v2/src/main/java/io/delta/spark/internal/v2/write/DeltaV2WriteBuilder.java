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
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.TypeWideningMode;
import org.apache.spark.sql.delta.schema.SchemaMergingUtils;
import org.apache.spark.sql.types.StructType;

/**
 * WriteBuilder for DSv2 batch writes to Delta tables. Mirrors the read-side {@code
 * SparkScanBuilder} pattern: takes table-level state and Spark's {@link LogicalWriteInfo}, and
 * builds a {@link DeltaV2BatchWrite} (which implements both Write and BatchWrite).
 *
 * <p>Schema validation uses the shared V1 utility {@code SchemaMergingUtils.mergeSchemas} to check
 * type compatibility and reject duplicate columns before the write proceeds.
 */
// Public: accessed from DeltaV2Table in v2.catalog package.
public class DeltaV2WriteBuilder implements WriteBuilder {

  private final Engine engine;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final LogicalWriteInfo writeInfo;

  /**
   * @param engine Kernel engine (persisted in DeltaV2Table, shared across operations)
   * @param tablePath filesystem path to the Delta table root
   * @param hadoopConf Hadoop configuration (with merged table options)
   * @param initialSnapshot Kernel snapshot loaded at table construction time
   * @param snapshotManager reloads the latest snapshot; used by the streaming write to build each
   *     epoch's commit against the current table state (see {@link DeltaV2StreamingWrite})
   * @param dataSchema the table's data (non-partition) schema, from DeltaV2Table's SchemaProvider
   * @param writeInfo Spark's logical write info (schema, queryId, options)
   */
  public DeltaV2WriteBuilder(
      Engine engine,
      String tablePath,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    this.engine = requireNonNull(engine, "engine is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.writeInfo = requireNonNull(writeInfo, "writeInfo is null");
  }

  @Override
  public Write build() {
    validateDataSchema(initialSnapshot, writeInfo.schema());

    // Returns a mode-dispatching Write: toBatch() -> DeltaV2BatchWrite (batch commit off
    // initialSnapshot), toStreaming() -> DeltaV2StreamingWrite (per-epoch commit off the latest
    // snapshot via snapshotManager). Both modes share the executor-side write-state construction.
    return new DeltaV2Write(
        engine, hadoopConf, tablePath, initialSnapshot, snapshotManager, dataSchema, writeInfo);
  }

  static void validateDataSchema(Snapshot initialSnapshot, StructType dataSchema) {
    // Validate data schema against table schema using the same utility as V1
    // (ImplicitMetadataOperation.updateMetadata -> SchemaMergingUtils.mergeSchemas).
    StructType tableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    // Strip column mapping metadata (physical names, IDs) so mergeSchemas compares
    // only logical types - matches V1's dropColumnMappingMetadata call.
    StructType cleanTableSchema =
        DeltaColumnMapping.dropColumnMappingMetadata(tableSchema.asNullable());

    // Throws DeltaAnalysisException for type incompatibilities or duplicate columns.
    // Return value discarded - we only need the validation side-effect.
    // TODO: Support schema evolution (mergeSchema option). When enabled, use the
    // merged schema returned here to update table metadata instead of discarding it.
    SchemaMergingUtils.mergeSchemas(
        cleanTableSchema,
        dataSchema,
        /* allowImplicitConversions */ false,
        /* keepExistingType */ false,
        TypeWideningMode.NoTypeWidening$.MODULE$,
        /* caseSensitive */ false);
  }
}
