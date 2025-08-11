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

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.scan.DeltaKernelScanBuilder;
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** A DataSource V2 Table implementation for Delta Lake tables using the Delta Kernel API. */
public class DeltaKernelTable implements Table, SupportsRead {

  private final Identifier identifier;
  // TODO: [delta-io/delta#5029] Add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;

  /**
   * Creates a new DeltaKernelTable instance.
   *
   * @param identifier the table identifier
   * @param snapshot the Delta table snapshot
   */
  public DeltaKernelTable(Identifier identifier, SnapshotImpl snapshot) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.snapshot = requireNonNull(snapshot, "snapshot is null");
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
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    return Collections.unmodifiableSet(capabilities);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DeltaKernelScanBuilder(snapshot);
  }
}
