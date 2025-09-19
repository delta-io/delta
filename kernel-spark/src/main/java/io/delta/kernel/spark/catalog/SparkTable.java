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
package io.delta.kernel.spark.catalog;

import static io.delta.kernel.spark.utils.ScalaUtils.toScalaMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.utils.SchemaUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** DataSource V2 Table implementation for Delta Lake using the Delta Kernel API. */
public class SparkTable implements Table, SupportsRead {

  private static final Set<TableCapability> CAPABILITIES =
      Collections.unmodifiableSet(EnumSet.of(TableCapability.BATCH_READ));

  private final Identifier identifier;
  private final String tablePath;
  private final Map<String, String> options;
  // TODO: [delta-io/delta#5029] Add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;
  private final Optional<CatalogTable> v1CatalogTable;

  private final StructType schema;
  private final List<String> partColNames;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Column[] columns;
  private final Transform[] partitionTransforms;

  /**
   * Creates a SparkTable backed by a Delta Kernel snapshot and initializes Spark-facing metadata
   * (schemas, partitioning, capabilities).
   *
   * <p>Side effects: - Loads the latest snapshot for the given tablePath. - Builds Hadoop
   * configuration from options for subsequent I/O. - Derives data schema, partition schema, and
   * full table schema from the snapshot.
   *
   * <p>Notes: - Partition column order from the snapshot is preserved for partitioning and appended
   * after data columns in the public Spark schema, per Spark conventions. - Read-time scan options
   * are later merged with these options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, SnapshotImpl snapshot, Map<String, String> options, Optional<CatalogTable> v1CatalogTable) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.tablePath = requireNonNull(snapshot.getPath(), "snapshot is null");
    this.options = options;
    this.v1CatalogTable = v1CatalogTable;
    this.snapshot = snapshot;
    StructType snapshotSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    this.partColNames =
        Collections.unmodifiableList(new ArrayList<>(snapshot.getPartitionColumnNames()));

    final List<StructField> dataFields = new ArrayList<>();
    final List<StructField> partitionFields = new ArrayList<>();

    // Build a map for O(1) field lookups to improve performance
    Map<String, StructField> fieldMap = new HashMap<>();
    for (StructField field : snapshotSchema.fields()) {
      fieldMap.put(field.name(), field);
    }

    // IMPORTANT: Add partition fields in the exact order specified by partColNames
    // This is crucial because the order in partColNames may differ from the order
    // in snapshotSchema, and we need to preserve the partColNames order for
    // proper partitioning behavior
    for (String partColName : partColNames) {
      StructField field = fieldMap.get(partColName);
      if (field != null) {
        partitionFields.add(field);
      }
    }

    // Add remaining fields as data fields (non-partition columns)
    // These are fields that exist in the schema but are not partition columns
    for (StructField field : snapshotSchema.fields()) {
      if (!partColNames.contains(field.name())) {
        dataFields.add(field);
      }
    }
    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));
    // For Spark, the table schema is always data columns plus partition columns.
    // This is different from the schema from snapshot which is partition columns plus data columns.
    dataFields.addAll(partitionFields);
    this.schema = new StructType(dataFields.toArray(new StructField[0]));
    this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    this.partitionTransforms =
        partColNames.stream().map(Expressions::identity).toArray(Transform[]::new);
  }

  public SparkTable(Identifier identifier, String tablePath, Map<String, String> options, Optional<CatalogTable> v1CatalogTable) {
    this(identifier, (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(
            DefaultEngine.create(SparkSession.active().sessionState().newHadoopConfWithOptions(toScalaMap(options)))), options, v1CatalogTable);
  }

  /**
   * Convenience constructor that uses empty options. See {@link #SparkTable(Identifier, String,
   * java.util.Map)} for full behavior and notes.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath, Optional<CatalogTable> v1CatalogTable) {
    this(identifier, tablePath, Collections.emptyMap(), v1CatalogTable);
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Transform[] partitioning() {
    return partitionTransforms;
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> props = new HashMap<>(snapshot.getMetadata().getConfiguration());
    props.putAll(this.options);
    return Collections.unmodifiableMap(props);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
    Map<String, String> combined = new HashMap<>(this.options);
    combined.putAll(scanOptions.asCaseSensitiveMap());
    CaseInsensitiveStringMap merged = new CaseInsensitiveStringMap(combined);
    return new SparkScanBuilder(name(), tablePath, dataSchema, partitionSchema, snapshot, merged);
  }

  @Override
  public String toString() {
    return "SparkTable{identifier=" + identifier + '}';
  }

    public Optional<CatalogTable> getV1CatalogTable() {
        return v1CatalogTable;
    }
}
