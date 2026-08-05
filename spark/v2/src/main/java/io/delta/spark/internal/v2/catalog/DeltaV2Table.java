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
package io.delta.spark.internal.v2.catalog;

import static io.delta.spark.internal.v2.utils.ScalaUtils.toJavaOptional;
import static io.delta.spark.internal.v2.utils.ScalaUtils.toScalaMap;
import static io.delta.spark.internal.v2.utils.StatsUtils.toV2Statistics;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.spark.internal.v2.adapters.KernelMetadataAdapter;
import io.delta.spark.internal.v2.adapters.KernelProtocolAdapter;
import io.delta.spark.internal.v2.exception.TimestampOutOfRangeException;
import io.delta.spark.internal.v2.read.DeltaV2ScanUtils;
import io.delta.spark.internal.v2.read.MetadataEvolutionHandler;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.write.DeltaRowLevelOperationBuilder;
import io.delta.spark.internal.v2.write.DeltaV2WriteBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.delta.catalog.DeltaV2TableMarker;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractProtocol;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.jdk.javaapi.CollectionConverters;

/** DataSource V2 Table implementation for Delta Lake using the Delta Kernel API. */
public class DeltaV2Table extends DeltaV2TableShims
    implements Table, SupportsRead, SupportsWrite, SupportsMetadataColumns, DeltaV2TableMarker {
  private static final String METADATA_COLUMN_NAME = FileFormat$.MODULE$.METADATA_NAME();
  private static final String ROW_ID_METADATA_FIELD_NAME = RowId$.MODULE$.ROW_ID();
  private static final String ROW_COMMIT_VERSION_METADATA_FIELD_NAME =
      RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME();

  private static final Set<TableCapability> CAPABILITIES = buildCapabilities();

  private static Set<TableCapability> buildCapabilities() {
    EnumSet<TableCapability> caps =
        EnumSet.of(
            TableCapability.BATCH_READ,
            TableCapability.MICRO_BATCH_READ,
            TableCapability.BATCH_WRITE,
            TableCapability.STREAMING_WRITE);
    DeltaV2TableShims.schemaEvolutionCapability().ifPresent(caps::add);
    return Collections.unmodifiableSet(caps);
  }

  private final Identifier identifier;
  private final String tablePath;
  private final Map<String, String> options;
  private final DeltaSnapshotManager snapshotManager;
  /** Snapshot created during connector setup */
  private final Snapshot initialSnapshot;

  private final Configuration hadoopConf;
  private final Engine kernelEngine;

  private final SchemaProvider schemaProvider;
  private final Optional<CatalogTable> catalogTable;
  private final boolean isCDCRead;

  /**
   * Creates a DeltaV2Table from a filesystem path without a catalog table.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  public DeltaV2Table(Identifier identifier, String tablePath) {
    this(identifier, tablePath, Collections.emptyMap(), Optional.empty(), OptionalLong.empty());
  }

  /**
   * Creates a DeltaV2Table from a filesystem path with options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @throws NullPointerException if identifier or tablePath is null
   */
  public DeltaV2Table(Identifier identifier, String tablePath, Map<String, String> options) {
    this(identifier, tablePath, options, Optional.empty(), OptionalLong.empty());
  }

  /**
   * Path-based constructor pinned to an explicit time travel version (if present).
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @param timeTravelVersion the table version to pin the initial snapshot to
   */
  public DeltaV2Table(
      Identifier identifier,
      String tablePath,
      Map<String, String> options,
      OptionalLong timeTravelVersion) {
    this(identifier, tablePath, options, Optional.empty(), timeTravelVersion);
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
  public DeltaV2Table(
      Identifier identifier, CatalogTable catalogTable, Map<String, String> options) {
    this(identifier, catalogTable, options, OptionalLong.empty());
  }

  /**
   * Catalog-table constructor pinned to an explicit time travel version (if present).
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @param options user-provided options to override catalog properties
   * @param timeTravelVersion the table version to pin the initial snapshot to
   */
  public DeltaV2Table(
      Identifier identifier,
      CatalogTable catalogTable,
      Map<String, String> options,
      OptionalLong timeTravelVersion) {
    this(
        identifier,
        getDecodedPath(requireNonNull(catalogTable, "catalogTable is null").location()),
        options,
        Optional.of(catalogTable),
        timeTravelVersion);
  }

  /**
   * Creates a DeltaV2Table backed by a Delta Kernel snapshot manager and initializes Spark-facing
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
  private DeltaV2Table(
      Identifier identifier,
      String tablePath,
      Map<String, String> userOptions,
      Optional<CatalogTable> catalogTable,
      OptionalLong timeTravelVersion) {
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
    this.kernelEngine = DefaultEngine.create(this.hadoopConf);
    this.snapshotManager = SnapshotManagerFactory.create(tablePath, kernelEngine, catalogTable);
    this.initialSnapshot =
        timeTravelVersion.isPresent()
            ? loadSnapshotAtCheckedVersion(snapshotManager, timeTravelVersion.getAsLong())
            : snapshotManager.loadLatestSnapshot();

    this.isCDCRead = CDCReader.isCDCRead(new CaseInsensitiveStringMap(this.options));

    Optional<PersistedMetadata> persistedMetadata =
        MetadataEvolutionHandler.getPersistedMetadataForMicroBatchStream(
            SparkSession.active(), initialSnapshot, options, snapshotManager, kernelEngine);

    StructType rawSchema;
    List<String> partitionColumnNames;
    if (persistedMetadata.isPresent()) {
      PersistedMetadata persisted = persistedMetadata.get();
      rawSchema = persisted.dataSchema();
      partitionColumnNames = Arrays.asList(persisted.partitionSchema().fieldNames());
    } else {
      rawSchema = SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
      partitionColumnNames = new ArrayList<>(initialSnapshot.getPartitionColumnNames());
    }
    // Schema-related metadata is lazily computed on first access within SchemaProvider
    this.schemaProvider =
        new SchemaProvider(SparkSession.active(), rawSchema, partitionColumnNames);
  }

  /**
   * Helper method to decode URI path handling URL-encoded characters correctly. E.g., converts
   * "spark%25dir%25prefix" to "spark%dir%prefix"
   *
   * <p>Uses Hadoop's Path class to properly handle all URI schemes (file, s3, abfss, gs, hdfs,
   * etc.), not just file:// URIs.
   */
  private static String getDecodedPath(java.net.URI location) {
    Path hadoopPath = new Path(location);
    // For local file system paths, return just the path component without the scheme
    // to maintain consistency with path-based table construction where tablePath is a
    // plain filesystem path string.
    if (location.getScheme() == null || "file".equals(location.getScheme())) {
      return hadoopPath.toUri().getPath();
    }
    return hadoopPath.toString();
  }

  /**
   * Returns the CatalogTable if this DeltaV2Table was created from a catalog table.
   *
   * @return Optional containing the CatalogTable, or empty if this table was created from a path
   */
  public Optional<CatalogTable> getCatalogTable() {
    return catalogTable;
  }

  /**
   * Returns the Path to the Delta table root.
   *
   * @return Path created from the table path
   */
  public Path getTablePath() {
    return new Path(tablePath);
  }

  /**
   * Returns the V2 identifier this table was constructed with.
   *
   * @return Identifier provided at construction
   */
  public Identifier getIdentifier() {
    return identifier;
  }

  /**
   * Returns the merged options (catalog storage + user options) this table was constructed with.
   *
   * @return Map of merged catalog storage and user options
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /**
   * Returns the snapshot manager backing this table. Catalog-driven features such as read-time CDF
   * (TableCatalog.loadChangelog) use this to resolve versions, timestamps, and snapshots without
   * having to build their own snapshot manager.
   */
  public DeltaSnapshotManager getSnapshotManager() {
    return snapshotManager;
  }

  /** The table protocol from the initial snapshot. */
  protected AbstractProtocol protocol() {
    return new KernelProtocolAdapter(initialSnapshot.getProtocol());
  }

  /** The table metadata from the initial snapshot. */
  protected AbstractMetadata metadata() {
    return new KernelMetadataAdapter(initialSnapshot.getMetadata());
  }

  /** Returns a copy of this table pinned to {@code version}. */
  public DeltaV2Table withVersion(long version) {
    return catalogTable.isPresent()
        ? new DeltaV2Table(identifier, catalogTable.get(), options, OptionalLong.of(version))
        : new DeltaV2Table(identifier, tablePath, options, OptionalLong.of(version));
  }

  /** Returns a copy of this table pinned to the snapshot active at {@code timestampMicros}. */
  public DeltaV2Table withTimestamp(long timestampMicros) {
    return withVersion(resolveTimestampToVersion(snapshotManager, timestampMicros));
  }

  /**
   * Resolves a time travel timestamp to the active commit version using the Kernel snapshot
   * manager.
   *
   * <p>This loads the latest snapshot more than once (here and inside the Kernel lookup), make it
   * share a singular load once the snapshot manager exposes it TODO(#5999).
   */
  private static long resolveTimestampToVersion(
      DeltaSnapshotManager manager, long timestampMicros) {
    long timeMillis = timestampMicros / 1000;
    DeltaHistoryManager.Commit commit =
        manager.getActiveCommitAtTime(
            timeMillis,
            /* canReturnLastCommit = */ true,
            /* mustBeRecreatable = */ true,
            /* canReturnEarliestCommit = */ true);
    long latestVersion = manager.loadLatestSnapshot().getVersion();
    if (commit.getTimestamp() > timeMillis) {
      // The earliest available commit is younger than the requested time.
      throw new TimestampOutOfRangeException(timeMillis, commit.getTimestamp(), false);
    } else if (commit.getVersion() == latestVersion && commit.getTimestamp() < timeMillis) {
      // The requested time is after the latest commit.
      throw new TimestampOutOfRangeException(timeMillis, commit.getTimestamp(), true);
    }
    return commit.getVersion();
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
    StructType base = schemaProvider.getPublicSchema();
    return isCDCRead ? CDCSchemaContext.appendCDCColumns(base) : base;
  }

  @Override
  public Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema());
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

  /**
   * Returns the version of the snapshot loaded when this table was constructed, as a String.
   *
   * <p>Add {@code @Override} annotation eventually: TODO(#7128)
   */
  public String version() {
    return Long.toString(initialSnapshot.getVersion());
  }

  /**
   * Exposes file-level and row-tracking metadata via a single DSv2 metadata struct column.
   *
   * <p>This always returns one metadata column named {@code _metadata}. The struct contains Spark
   * file-source base metadata fields with the same names and types as {@link
   * org.apache.spark.sql.execution.datasources.FileFormat#BASE_METADATA_FIELDS} ({@code file_path},
   * {@code file_name}, {@code file_size}, {@code file_block_start}, {@code file_block_length},
   * {@code file_modification_time}); when row tracking is enabled, {@code row_id} and {@code
   * row_commit_version} are appended. Values follow {@link
   * org.apache.spark.sql.execution.datasources.PartitionedFile} semantics (see {@code
   * FileFormat.BASE_METADATA_EXTRACTORS}).
   *
   * <p>Field order mirrors Spark's canonical {@code BASE_METADATA_FIELDS} for parity with V1 Delta,
   * but resolution is name-based - order is not load-bearing for correctness.
   */
  @Override
  public MetadataColumn[] metadataColumns() {
    boolean rowTrackingEnabled =
        RowTracking.isEnabled(initialSnapshot.getProtocol(), initialSnapshot.getMetadata());

    StructType metadataType = new StructType();
    for (StructField field :
        CollectionConverters.asJava(FileFormat$.MODULE$.BASE_METADATA_FIELDS())) {
      metadataType = metadataType.add(field);
    }
    if (rowTrackingEnabled) {
      metadataType =
          metadataType
              .add(ROW_ID_METADATA_FIELD_NAME, DataTypes.LongType, false)
              .add(ROW_COMMIT_VERSION_METADATA_FIELD_NAME, DataTypes.LongType, false);
    }
    final StructType finalMetadataType = metadataType;

    MetadataColumn[] columns = new MetadataColumn[1];
    columns[0] =
        new MetadataColumn() {
          @Override
          public String name() {
            return METADATA_COLUMN_NAME;
          }

          @Override
          public DataType dataType() {
            return finalMetadataType;
          }

          @Override
          public boolean isNullable() {
            return false;
          }
        };

    return columns;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
    Map<String, String> combined = new HashMap<>(this.options);
    combined.putAll(scanOptions.asCaseSensitiveMap());
    CaseInsensitiveStringMap merged = new CaseInsensitiveStringMap(combined);
    Optional<Statistics> catalogStats =
        catalogTable
            .flatMap(ct -> toJavaOptional(ct.stats()))
            .map(
                stats ->
                    toV2Statistics(
                        stats,
                        schemaProvider.getDataSchema(),
                        schemaProvider.getPartitionSchema()));
    return DeltaV2ScanUtils.newScanBuilder(
        name(),
        initialSnapshot,
        snapshotManager,
        schemaProvider.getDataSchema(),
        schemaProvider.getPartitionSchema(),
        schemaProvider.getRawSchema(),
        catalogStats,
        merged);
  }

  /**
   * Validates that {@code version} exists in the Delta log, then loads the snapshot pinned to it.
   */
  private static Snapshot loadSnapshotAtCheckedVersion(DeltaSnapshotManager manager, long version) {
    manager.checkVersionExists(
        version, /* mustBeRecreatable = */ true, /* allowOutOfRange = */ false);
    return manager.loadSnapshotAt(version);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    requireNonNull(info, "write info is null");
    return new DeltaV2WriteBuilder(
        kernelEngine,
        tablePath,
        hadoopConf,
        initialSnapshot,
        snapshotManager,
        schemaProvider.getDataSchema(),
        info);
  }

  /**
   * Returns a builder for Delta row-level operations. The builder currently wires Spark planning to
   * Delta's copy-on-write operation shell; the concrete ReplaceData write path is introduced in a
   * follow-up PR.
   */
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    requireNonNull(info, "row-level operation info is null");
    return new DeltaRowLevelOperationBuilder(this, kernelEngine, hadoopConf, initialSnapshot, info);
  }

  @Override
  public String toString() {
    return "DeltaV2Table{identifier=" + identifier + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeltaV2Table that = (DeltaV2Table) o;
    return Objects.equals(identifier, that.identifier)
        && Objects.equals(tablePath, that.tablePath)
        && Objects.equals(options, that.options)
        && Objects.equals(catalogTable, that.catalogTable)
        && Objects.equals(initialSnapshot.getPath(), that.initialSnapshot.getPath())
        && initialSnapshot.getVersion() == that.initialSnapshot.getVersion();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        identifier,
        tablePath,
        options,
        catalogTable,
        initialSnapshot.getPath(),
        initialSnapshot.getVersion());
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
    private final StructType rawSchema;
    private final List<String> partColNames;

    // Lazily computed fields
    private boolean initialized = false;
    private StructType publicSchema;
    private StructType dataSchema;
    private StructType partitionSchema;
    private Transform[] partitionTransforms;

    SchemaProvider(SparkSession sparkSession, StructType rawSchema, List<String> partColNames) {
      this.sparkSession = sparkSession;
      this.rawSchema = rawSchema;
      this.partColNames = Collections.unmodifiableList(new ArrayList<>(partColNames));
    }

    private synchronized void ensureInitialized() {
      if (initialized) {
        return;
      }

      // Create public schema by removing internal metadata (for schema() method)
      this.publicSchema =
          DeltaTableUtils.removeInternalDeltaMetadata(
              sparkSession, DeltaTableUtils.removeInternalWriterMetadata(sparkSession, rawSchema));

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

    StructType getRawSchema() {
      return withInit(() -> rawSchema);
    }

    Transform[] getPartitionTransforms() {
      return withInit(() -> partitionTransforms);
    }
  }
}
