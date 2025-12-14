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

import io.delta.kernel.Snapshot;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.snapshot.SnapshotManagerFactory;
import io.delta.kernel.spark.utils.SchemaUtils;
import java.util.*;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

  private final SchemaProvider schemaProvider;
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
    this.snapshotManager = SnapshotManagerFactory.create(tablePath, hadoopConf, catalogTable);
    // Load the initial snapshot through the manager
    this.initialSnapshot = snapshotManager.loadLatestSnapshot();

    // Schema-related metadata is lazily computed on first access within SchemaProvider
    this.schemaProvider = new SchemaProvider(SparkSession.active(), initialSnapshot);
  }

  /**
   * Helper method to decode URI path handling URL-encoded characters correctly. E.g., converts
   * "spark%25dir%25prefix" to "spark%dir%prefix"
   *
   * <p>Uses Hadoop's Path class to properly handle all URI schemes (file, s3, abfss, gs, hdfs,
   * etc.), not just file:// URIs.
   */
  private static String getDecodedPath(java.net.URI location) {
    return new Path(location).toString();
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
    return schemaProvider.getPublicSchema();
  }

  @Override
  public Column[] columns() {
    return schemaProvider.getColumns();
  }

  @Override
  public Transform[] partitioning() {
    return schemaProvider.getPartitionTransforms();
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
        name(),
        initialSnapshot,
        snapshotManager,
        schemaProvider.getDataSchema(),
        schemaProvider.getPartitionSchema(),
        merged);
  }

  @Override
  public String toString() {
    return "SparkTable{identifier=" + identifier + '}';
  }

  /**
   * Private helper class that lazily computes and caches schema-related metadata.
   *
   * <p>This class encapsulates all schema computation logic including:
   *
   * <ul>
   *   <li>Raw schema conversion from Kernel to Spark
   *   <li>Public schema with internal metadata removed
   *   <li>Data and partition schema derivation
   *   <li>Column and partition transform creation
   * </ul>
   *
   * <p>All schema computations are deferred until first access.
   */
  private static class SchemaProvider {
    private final SparkSession sparkSession;
    private final Snapshot snapshot;

    // Lazily computed fields
    private boolean initialized = false;
    private StructType rawSchema;
    private StructType publicSchema;
    private List<String> partColNames;
    private StructType dataSchema;
    private StructType partitionSchema;
    private Column[] columns;
    private Transform[] partitionTransforms;

    SchemaProvider(SparkSession sparkSession, Snapshot snapshot) {
      this.sparkSession = sparkSession;
      this.snapshot = snapshot;
    }

    private synchronized void ensureInitialized() {
      if (initialized) {
        return;
      }

      // Convert Kernel schema to Spark schema - keep all metadata for internal use
      this.rawSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());

      // Create public schema by removing internal metadata (for schema() method)
      this.publicSchema =
          DeltaTableUtils.removeInternalDeltaMetadata(
              sparkSession, DeltaTableUtils.removeInternalWriterMetadata(sparkSession, rawSchema));

      this.partColNames =
          Collections.unmodifiableList(new ArrayList<>(snapshot.getPartitionColumnNames()));

      final List<StructField> dataFields = new ArrayList<>();
      final List<StructField> partitionFields = new ArrayList<>();

      // Build a map for O(1) field lookups to improve performance
      // Use rawSchema (with metadata) for deriving data and partition schemas
      Map<String, StructField> fieldMap = new HashMap<>();
      for (StructField field : rawSchema.fields()) {
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
      for (StructField field : rawSchema.fields()) {
        if (!partColNames.contains(field.name())) {
          dataFields.add(field);
        }
      }
      this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
      this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));

      // Use publicSchema (cleaned) for external API
      this.columns = CatalogV2Util.structTypeToV2Columns(publicSchema);
      this.partitionTransforms =
          partColNames.stream().map(Expressions::identity).toArray(Transform[]::new);

      this.initialized = true;
    }

    private <T> T withInit(Supplier<T> supplier) {
      ensureInitialized();
      return supplier.get();
    }

    StructType getPublicSchema() {
      return withInit(() -> publicSchema);
    }

    StructType getDataSchema() {
      return withInit(() -> dataSchema);
    }

    StructType getPartitionSchema() {
      return withInit(() -> partitionSchema);
    }

    Column[] getColumns() {
      return withInit(() -> columns);
    }

    Transform[] getPartitionTransforms() {
      return withInit(() -> partitionTransforms);
    }
  }
}
