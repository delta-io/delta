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
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.*;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** A DataSource V2 Table implementation for Delta Lake tables using the Delta Kernel API. */
public class DeltaDsv2Table implements Table {

  private final Identifier identifier;
  // TODO: add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;

  /**
   * Creates a new DeltaDsv2Table instance.
   *
   * @param identifier the table identifier
   * @param snapshot the Delta table snapshot
   */
  public DeltaDsv2Table(Identifier identifier, SnapshotImpl snapshot) {
    requireNonNull(identifier);
    requireNonNull(snapshot);
    this.identifier = identifier;
    this.snapshot = snapshot;
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
    // TODO: fill in when implementing mix-in interface
    return Collections.unmodifiableSet(new HashSet<>());
  }
}
