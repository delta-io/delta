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
package io.delta.kernel.spark.table;

import static io.delta.kernel.spark.utils.ScalaUtils.toScalaMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.snapshot.PathBasedSnapshotManager;
import io.delta.kernel.spark.utils.SchemaUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.delta.DeltaTableUtils;
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
  private final DeltaSnapshotManager snapshotManager;
  /** Snapshot created during connector setup */
  private final Snapshot initialSnapshot;

  private final Configuration hadoopConf;

  private final StructType schema;
  private final List<String> partColNames;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Column[] columns;
  private final Transform[] partitionTransforms;
  private final Optional<CatalogTable> catalogTable;

  /**
   * Creates a SparkTable from a filesystem path without a catalog table.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath) {
    this(identifier, tablePath, Collections.emptyMap(), Optional.empty());
  }

  /**
   * Creates a SparkTable from a filesystem path with options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath, Map<String, String> options) {
    this(identifier, tablePath, options, Optional.empty());
  }

  /**
   * Constructor that accepts a Spark CatalogTable and user-provided options. Extracts the table
   * location and storage properties from the catalog table, then merges with user options. User
   * options take precedence over catalog properties in case of conflicts.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @param options user-provided options to override catalog properties
   */
  public SparkTable(Identifier identifier, CatalogTable catalogTable, Map<String, String> options) {
    this(
        identifier,
        getDecodedPath(requireNonNull(catalogTable, "catalogTable is null").location()),
        options,
        Optional.of(catalogTable));
  }

  /**
   * Creates a SparkTable backed by a Delta Kernel snapshot manager and initializes Spark-facing
   * metadata (schemas, partitioning, capabilities).
   *
   * <p>Side effects: - Initializes a SnapshotManager for the given tablePath. - Loads the latest
   * snapshot via the manager. - Builds Hadoop configuration from options for subsequent I/O. -
   * Derives data schema, partition schema, and full table schema from the snapshot.
   *
   * <p>Notes: - Partition column order from the snapshot is preserved for partitioning and appended
   * after data columns in the public Spark schema, per Spark conventions. - Read-time scan options
   * are later merged with these options.
   */
  private SparkTable(
      Identifier identifier,
      String tablePath,
      Map<String, String> userOptions,
      Optional<CatalogTable> catalogTable) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.catalogTable = catalogTable;
    // Merge options: file system options from catalog + user options (user takes precedence)
    // This follows the same pattern as DeltaTableV2 in delta-spark
    Map<String, String> merged = new HashMap<>();
    // Only extract file system options from table storage properties
    catalogTable.ifPresent(
        table ->
            scala.collection.JavaConverters.mapAsJavaMap(table.storage().properties())
                .forEach(
                    (key, value) -> {
                      if (DeltaTableUtils.validDeltaTableHadoopPrefixes()
                          .exists(prefix -> key.startsWith(prefix))) {
                        merged.put(key, value);
                      }
                    }));
    // User options override catalog properties
    merged.putAll(userOptions);
    this.options = Collections.unmodifiableMap(merged);

    this.hadoopConf =
        SparkSession.active().sessionState().newHadoopConfWithOptions(toScalaMap(options));
    this.snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    // Load the initial snapshot through the manager
    this.initialSnapshot = snapshotManager.loadLatestSnapshot();
    this.schema = SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    this.partColNames =
        Collections.unmodifiableList(new ArrayList<>(initialSnapshot.getPartitionColumnNames()));

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
   * Helper method to decode URI path handling URL-encoded characters correctly. E.g., converts
   * "spark%25dir%25prefix" to "spark%dir%prefix"
   */
  private static String getDecodedPath(java.net.URI location) {
    return new java.io.File(location).getPath();
  }

  /**
   * Returns the CatalogTable if this SparkTable was created from a catalog table.
   *
   * @return Optional containing the CatalogTable, or empty if this table was created from a path
   */
  public Optional<CatalogTable> getCatalogTable() {
    return catalogTable;
  }

  /**
   * Returns the table name in a format compatible with DeltaTableV2.
   *
   * <p>For catalog-based tables, returns the fully qualified table name (e.g.,
   * "spark_catalog.default.table_name"). For path-based tables, returns the path-based identifier
   * (e.g., "delta.`/path/to/table`").
   *
   * @return the table name string
   */
  @Override
  public String name() {
    return catalogTable
        .map(ct -> ct.identifier().unquotedString())
        .orElse("delta.`" + tablePath + "`");
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
    Map<String, String> props = new HashMap<>(initialSnapshot.getTableProperties());
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
    return new SparkScanBuilder(
        name(), initialSnapshot, snapshotManager, dataSchema, partitionSchema, merged);
  }

  @Override
  public String toString() {
    return "SparkTable{identifier=" + identifier + '}';
  }
}
