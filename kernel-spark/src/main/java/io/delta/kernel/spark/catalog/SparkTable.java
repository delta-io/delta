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

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.utils.SchemaUtils;
import java.util.*;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
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
      Collections.unmodifiableSet(
          EnumSet.of(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ));

  private final Identifier identifier;
  private final String tablePath;
  private final Map<String, String> options;
  // TODO: [delta-io/delta#5029] Add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;
  private final Configuration hadoopConf;

  private final StructType schema;
  private final List<String> partColNames;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Column[] columns;
  private final Transform[] partitionTransforms;
  private final Optional<org.apache.spark.sql.catalyst.catalog.CatalogTable> v1CatalogTable;

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
  public SparkTable(Identifier identifier, String tablePath, Map<String, String> options) {
    this(identifier, tablePath, options, Optional.empty());
  }

  /** Private constructor with v1CatalogTable parameter. */
  private SparkTable(
      Identifier identifier,
      String tablePath,
      Map<String, String> options,
      Optional<org.apache.spark.sql.catalyst.catalog.CatalogTable> v1CatalogTable) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.tablePath = requireNonNull(tablePath, "snapshot is null");
    this.options = options;
    this.v1CatalogTable = v1CatalogTable;
    this.hadoopConf =
        SparkSession.active()
            .sessionState()
            .newHadoopConfWithOptions(
                v1CatalogTable.map(t -> t.storage().properties()).orElse(toScalaMap((options))));
    this.snapshot =
        (SnapshotImpl)
            io.delta.kernel.TableManager.loadSnapshot(tablePath)
                .build(io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf));

    this.schema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    this.partColNames =
        Collections.unmodifiableList(new ArrayList<>(snapshot.getPartitionColumnNames()));

    final List<StructField> dataFields = new ArrayList<>();
    final List<StructField> partitionFields = new ArrayList<>();

    // Build a map for O(1) field lookups to improve performance
    Map<String, StructField> fieldMap = new HashMap<>();
    for (StructField field : schema.fields()) {
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
    for (StructField field : schema.fields()) {
      if (!partColNames.contains(field.name())) {
        dataFields.add(field);
      }
    }
    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));

    this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    this.partitionTransforms =
        partColNames.stream().map(Expressions::identity).toArray(Transform[]::new);
  }

  /**
   * Convenience constructor that uses empty options. See {@link #SparkTable(Identifier, String,
   * java.util.Map)} for full behavior and notes.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath) {
    this(identifier, tablePath, Collections.emptyMap());
  }

  /**
   * Constructor that accepts a Spark CatalogTable. Extracts the table location from the catalog
   * table and initializes the SparkTable using that location with empty options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @throws NullPointerException if identifier or catalogTable is null
   */
  public SparkTable(
      Identifier identifier, org.apache.spark.sql.catalyst.catalog.CatalogTable catalogTable) {
    this(
        identifier,
        requireNonNull(catalogTable, "catalogTable is null").location().toString(),
        Collections.emptyMap(),
        Optional.of(catalogTable));
  }

  /**
   * Returns the V1 CatalogTable if this SparkTable was created from a catalog table.
   *
   * @return Optional containing the CatalogTable, or empty if this table was created from a path
   */
  public Optional<org.apache.spark.sql.catalyst.catalog.CatalogTable> getV1CatalogTable() {
    return v1CatalogTable;
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
}
