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
package io.delta.spark.dsv2.table;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.scan.KernelSparkScanBuilder;
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * DataSource V2 Table implementation for Delta Lake tables using the Delta Kernel API. Provides
 * Hadoop configuration externalization for improved testability and dependency management.
 */
public class DeltaKernelTable implements Table, SupportsRead {

  private static final Set<TableCapability> CAPABILITIES =
      Collections.unmodifiableSet(EnumSet.of(TableCapability.BATCH_READ));

  private final Identifier identifier;
  // TODO: [delta-io/delta#5029] Add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;
  private final Engine engine;

  /**
   * Creates a new DeltaKernelTable instance.
   *
   * @param identifier the table identifier
   * @param snapshot the Delta table snapshot
   * @param hadoopConf the Hadoop configuration to use for creating the engine
   */
  public DeltaKernelTable(Identifier identifier, SnapshotImpl snapshot, Configuration hadoopConf) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.snapshot = requireNonNull(snapshot, "snapshot is null");
    this.engine =
        io.delta.kernel.defaults.engine.DefaultEngine.create(
            requireNonNull(hadoopConf, "hadoopConf is null"));
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public StructType schema() {
    return SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
  }

  @Override
  public Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema());
  }

  @Override
  public Transform[] partitioning() {
    // Delta currently just support identity partition
    return snapshot.getPartitionColumnNames().stream()
        .map(Expressions::identity)
        .toArray(Transform[]::new);
  }

  @Override
  public Map<String, String> properties() {
    return snapshot.getMetadata().getConfiguration();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new KernelSparkScanBuilder(snapshot, engine);
  }
}
