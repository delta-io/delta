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
package io.delta.spark.internal.v2.catalog

import java.lang.{Long => JLong}
import java.net.URI
import java.util.{
  ArrayList,
  Arrays,
  Collections,
  EnumSet,
  HashMap,
  List => JList,
  Map => JMap,
  Objects,
  Optional,
  Set => JSet
}
import java.util.Objects.requireNonNull
import java.util.function.Supplier

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{
  DeltaTableUtils,
  RowCommitVersion,
  RowId,
  RowTracking,
  Snapshot
}
import org.apache.spark.sql.delta.catalog.DeltaV2TableMarker
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.PersistedMetadata
import org.apache.spark.sql.delta.util.DeltaFileSystemOptions
import io.delta.spark.internal.v2.exception.{
  NoRecreatableHistoryException,
  TableNotFoundException,
  TimestampOutOfRangeException
}
import org.apache.spark.sql.delta.v2.interop.{
  AbstractMetadata,
  AbstractProtocol,
  DeltaV2QueryContext,
  DeltaV2SnapshotManager
}
import io.delta.spark.internal.v2.read.{DeltaV2ScanUtils, MetadataEvolutionHandler}
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext
import io.delta.spark.internal.v2.tablemanager.{
  DeltaV2TableManager,
  DeltaV2TableManagerCache
}
import io.delta.spark.internal.v2.utils.ScalaUtils.{
  toJavaOptional,
  toScalaMap,
  toScalaOption
}
import io.delta.spark.internal.v2.utils.StatsUtils.toV2Statistics
import io.delta.spark.internal.v2.write.{
  DeltaRowLevelOperationBuilder,
  DeltaV2WriteBuilder
}
import io.delta.spark.internal.v2.shims.CatalogV2UtilShims
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.exceptions.{
  KernelException,
  TableNotFoundException => KernelTableNotFoundException
}
import io.delta.kernel.internal.{DeltaHistoryManager => KernelDeltaHistoryManager}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.catalog.{
  Column,
  Identifier,
  MetadataColumn,
  SupportsMetadataColumns,
  SupportsRead,
  SupportsRowLevelOperations,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.{ScanBuilder, Statistics}
import org.apache.spark.sql.connector.write.{
  LogicalWriteInfo,
  RowLevelOperationBuilder,
  RowLevelOperationInfo,
  WriteBuilder
}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** DataSource V2 Table implementation for Delta Lake using the Delta Kernel API. */
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
class DeltaV2Table private (
    private val identifier: Identifier,
    private val tablePath: String,
    userOptions: JMap[String, String],
    catalogTable: Optional[CatalogTable],
    timeTravelVersion: Option[Long])
    extends DeltaV2TableShimsWithLogging
    with Table
    with SupportsRead
    with SupportsWrite
    with SupportsMetadataColumns
    with SupportsRowLevelOperations
    with DeltaV2TableMarker
    with DeltaV2TableVersion {

  import DeltaV2Table._

  requireNonNull(identifier, "identifier is null")
  requireNonNull(tablePath, "tablePath is null")
  private val catalogTableValue = catalogTable
  private val queryContext = DeltaV2QueryContext(catalogTableValue)
  // Merge options: file system options from catalog + user options (user takes precedence)
  // This follows the same pattern as DeltaTableV2 in delta-spark
  private val (options, hadoopConf, kernelContext, tableManager) = {
    val catalogTableOpt = queryContext.catalogTableOpt
    val merged = new HashMap[String, String](
      mapAsJavaMap(
        DeltaFileSystemOptions.extractCatalogTableFsOptions(catalogTableOpt)))
    // User options override catalog properties
    merged.putAll(userOptions)
    val options = Collections.unmodifiableMap[String, String](merged)
    val activeSession = SparkSession.active
    val hadoopConf = activeSession.sessionState.newHadoopConfWithOptions(toScalaMap(options))
    val tableManager =
      DeltaV2TableManagerCache.forTable(activeSession, tablePath, options, catalogTableOpt)
    (options, hadoopConf, tableManager.kernelContext, tableManager)
  }

  /** Snapshot created during connector setup */
  private val initialSnapshotValue = loadInitialSnapshot(timeTravelVersion)

  private def loadInitialSnapshot(timeTravelVersion: Option[Long]): Snapshot = try {
    if (timeTravelVersion.isDefined) {
      loadSnapshotAtCheckedVersion(tableManager, timeTravelVersion.get)
    } else {
      val loadLatestSnapshot: Supplier[Snapshot] =
        () => tableManager.loadLatestSnapshot(queryContext)
      recordFrameProfileValue("snapshot.loadLatest", loadLatestSnapshot)
    }
  } catch {
    case _: KernelTableNotFoundException =>
      // Rethrow as the Delta-module wrapper so catalog/interop layer never names a Kernel type.
      throw new TableNotFoundException(tablePath)
    case kernelError: KernelException =>
      // Missing earliest commit file surfaces as a base KernelException, so match on its message.
      val reason = kernelError.getMessage
      if (reason != null && reason.contains("Missing delta file")) {
        throw new NoRecreatableHistoryException(tablePath)
      }
      throw kernelError
  }

  private val isCDCRead = CDCReader.isCDCRead(new CaseInsensitiveStringMap(options))
  // True when this table is pinned to a time travel version.
  private val isTimeTravel = timeTravelVersion.isDefined

  private val schemaProvider = {
    val persistedMetadata: Optional[PersistedMetadata] =
      MetadataEvolutionHandler.getPersistedMetadataForMicroBatchStream(
        SparkSession.active,
        initialSnapshotValue,
        options,
        tableManager.snapshotManager(queryContext.catalogTableOpt),
        kernelEngine(),
        queryContext)

    val (rawSchema, partitionColumnNames) = if (persistedMetadata.isPresent) {
      val persisted = persistedMetadata.get()
      (persisted.dataSchema, Arrays.asList(persisted.partitionSchema.fieldNames: _*))
    } else {
      val rawSchema = initialSnapshotValue.schema
      val partitionColumnNames =
        new ArrayList[String](initialSnapshotValue.metadata.getPartitionColumns)
      (rawSchema, partitionColumnNames)
    }
    // Schema-related metadata is lazily computed on first access within SchemaProvider
    new SchemaProvider(SparkSession.active, rawSchema, partitionColumnNames)
  }

  /**
   * Creates a DeltaV2Table from a filesystem path without a catalog table.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  def this(identifier: Identifier, tablePath: String) =
    this(identifier, tablePath, Collections.emptyMap(), Optional.empty(), None)

  /**
   * Creates a DeltaV2Table from a filesystem path with options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @throws NullPointerException if identifier or tablePath is null
   */
  def this(identifier: Identifier, tablePath: String, options: JMap[String, String]) =
    this(identifier, tablePath, options, Optional.empty(), None)

  /**
   * Path-based constructor pinned to an explicit time travel version (if present).
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @param timeTravelVersion the table version to pin the initial snapshot to
   */
  def this(
      identifier: Identifier,
      tablePath: String,
      options: JMap[String, String],
      timeTravelVersion: Option[Long]) =
    this(identifier, tablePath, options, Optional.empty(), timeTravelVersion)

  /**
   * Catalog-table constructor pinned to an explicit time travel version (if present).
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @param options user-provided options to override catalog properties
   * @param timeTravelVersion the table version to pin the initial snapshot to
   */
  def this(
      identifier: Identifier,
      catalogTable: CatalogTable,
      options: JMap[String, String],
      timeTravelVersion: Option[Long]) =
    this(
      identifier,
      DeltaV2Table.getDecodedPath(requireNonNull(catalogTable, "catalogTable is null").location),
      options,
      Optional.of(catalogTable),
      timeTravelVersion)

  /**
   * Constructor that accepts a Spark CatalogTable and user-provided options. Extracts the table
   * location and storage properties from the catalog table, then merges with user options. User
   * options take precedence over catalog properties in case of conflicts.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @param options user-provided options to override catalog properties
   */
  def this(
      identifier: Identifier,
      catalogTable: CatalogTable,
      options: JMap[String, String]) =
    this(identifier, catalogTable, options, None)

  /**
   * Returns the CatalogTable if this DeltaV2Table was created from a catalog table.
   *
   * @return Optional containing the CatalogTable, or empty if this table was created from a path
   */
  override def getCatalogTable(): Optional[CatalogTable] = catalogTableValue

  /**
   * Returns the Path to the Delta table root.
   *
   * @return Path created from the table path
   */
  override def getTablePath(): Path = new Path(tablePath)

  /**
   * Returns the V2 identifier this table was constructed with.
   *
   * @return Identifier provided at construction
   */
  def getIdentifier(): Identifier = identifier

  /**
   * Returns the merged options (catalog storage + user options) this table was constructed with.
   *
   * @return Map of merged catalog storage and user options
   */
  def getOptions(): JMap[String, String] = options

  /** Returns the immutable request context that resolved this table. */
  def getQueryContext(): DeltaV2QueryContext = queryContext

  /**
   * Returns the table manager backing this table. Catalog-driven features such as read-time CDF
   * (TableCatalog.loadChangelog) use this to resolve versions, timestamps, and snapshots without
   * having to build their own manager.
   */
  def getTableManager(): DeltaV2TableManager = tableManager

  /**
   * Returns a snapshot manager derived from the table manager and the catalog table this table was
   * resolved from. Transitional: callers that still need a snapshot manager use this until they
   * move to the query-context APIs on [[getTableManager]].
   */
  @deprecated("Use the query-context APIs on getTableManager instead")
  def getSnapshotManager(): DeltaV2SnapshotManager =
    tableManager.snapshotManager(queryContext.catalogTableOpt)

  /** The table protocol from the initial snapshot. */
  protected def protocol(): AbstractProtocol = initialSnapshotValue.protocol

  /** The table metadata from the initial snapshot. */
  protected def metadata(): AbstractMetadata = initialSnapshotValue.metadata

  /** Inputs exposed to the Spark-version shim for metadata-only DELETE. */
  protected def kernelEngine(): KernelEngine = kernelContext.getDefaultEngine

  protected def initialSnapshot(): Snapshot = initialSnapshotValue

  protected def catalogTable(): Option[CatalogTable] = queryContext.catalogTableOpt

  /** Returns a copy of this table pinned to {@code version}. */
  def withVersion(version: Long): DeltaV2Table = {
    if (catalogTableValue.isPresent) {
      new DeltaV2Table(identifier, catalogTableValue.get(), options, Some(version))
    } else {
      new DeltaV2Table(identifier, tablePath, options, Some(version))
    }
  }

  /** Returns a copy of this table pinned to the snapshot active at {@code timestampMicros}. */
  def withTimestamp(timestampMicros: Long): DeltaV2Table =
    withVersion(resolveTimestampToVersion(tableManager, timestampMicros, queryContext))

  /**
   * Returns the table name in a format compatible with DeltaTableV2.
   *
   * <p>For catalog-based tables, returns the fully qualified table name (e.g.,
   * "spark_catalog.default.table_name"). For path-based tables, returns the path-based identifier
   * (e.g., "delta.`/path/to/table`").
   *
   * @return the table name string
   */
  override def name(): String =
    catalogTableValue.map[String](ct => ct.identifier.unquotedString)
      .orElse("delta.`" + tablePath + "`")

  override def schema(): StructType = {
    val base = schemaProvider.getPublicSchema()
    if (isCDCRead) CDCSchemaContext.appendCDCColumns(base) else base
  }

  override def columns(): Array[Column] = {
    CatalogV2UtilShims.structTypeToV2Columns(schema())
  }

  override def partitioning(): Array[Transform] = schemaProvider.getPartitionTransforms()

  override def properties(): JMap[String, String] = {
    val props = new HashMap[String, String](initialSnapshotValue.metadata.getConfiguration)
    Collections.unmodifiableMap[String, String](props)
  }

  override def capabilities(): JSet[TableCapability] = {
    // A time travel pin cannot be written to. Dropping the write capabilities makes writes fall
    // back to the v1 path.
    if (isTimeTravel) READ_ONLY_CAPABILITIES else CAPABILITIES
  }

  /**
   * Returns the version of the snapshot loaded when this table was constructed, as a String.
   *
   * <p>Add {@code @Override} annotation eventually: TODO(#7128)
   */
  override def version(): String = JLong.toString(initialSnapshotValue.version)

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
  override def metadataColumns(): Array[MetadataColumn] = {
    val rowTrackingEnabled =
      RowTracking.isEnabled(initialSnapshotValue.protocol, initialSnapshotValue.metadata)

    var metadataType = new StructType()
    FileFormat.BASE_METADATA_FIELDS.foreach { field =>
      metadataType = metadataType.add(field)
    }
    if (rowTrackingEnabled) {
      metadataType = metadataType
        .add(ROW_ID_METADATA_FIELD_NAME, DataTypes.LongType, false)
        .add(ROW_COMMIT_VERSION_METADATA_FIELD_NAME, DataTypes.LongType, false)
    }
    val finalMetadataType = metadataType

    val columns = new Array[MetadataColumn](1)
    columns(0) = new MetadataColumn {
      override def name(): String = METADATA_COLUMN_NAME
      override def dataType(): DataType = finalMetadataType
      override def isNullable(): Boolean = false
    }
    columns
  }

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    val combined = new HashMap[String, String](options)
    combined.putAll(scanOptions.asCaseSensitiveMap())
    val merged = new CaseInsensitiveStringMap(combined)
    val catalogStats: Optional[Statistics] =
      catalogTableValue
        .flatMap[CatalogStatistics](ct => toJavaOptional(ct.stats))
        .map[Statistics](stats =>
          toV2Statistics(
            stats, schemaProvider.getDataSchema(), schemaProvider.getPartitionSchema()))
    DeltaV2ScanUtils.newScanBuilder(
      tableName = name(),
      initialSnapshot = initialSnapshotValue,
      kernelEngine = kernelEngine(),
      catalogTable = toScalaOption(catalogTableValue),
      snapshotManager = getSnapshotManager(),
      dataSchema = schemaProvider.getDataSchema(),
      partitionSchema = schemaProvider.getPartitionSchema(),
      tableSchema = schemaProvider.getRawSchema(),
      catalogStats = toScalaOption(catalogStats),
      options = merged)
  }

  /**
   * Validates that {@code version} exists in the Delta log, then loads the snapshot pinned to it.
   */
  private def loadSnapshotAtCheckedVersion(
      manager: DeltaV2TableManager,
      version: Long): Snapshot = {
    val mustBeRecreatable = true
    val allowOutOfRange = false
    manager.checkVersionExists(version, mustBeRecreatable, allowOutOfRange, queryContext)
    val loadSnapshot: Supplier[Snapshot] = () => manager.loadSnapshotAt(version, queryContext)
    recordFrameProfileValue("snapshot.loadAtVersion", loadSnapshot)
  }


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    requireNonNull(info, "write info is null")
    new DeltaV2WriteBuilder(
      kernelEngine(),
      tablePath,
      hadoopConf,
      initialSnapshotValue,
      catalogTableValue,
      getSnapshotManager(),
      schemaProvider.getDataSchema(),
      schemaProvider.getPartitionSchema(),
      info)
  }

  /**
   * Returns a builder for Delta row-level operations. The builder currently wires Spark planning to
   * Delta's copy-on-write operation shell; the concrete ReplaceData write path is introduced in a
   * follow-up PR.
   */
  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    requireNonNull(info, "row-level operation info is null")
    new DeltaRowLevelOperationBuilder(
      this, kernelEngine(), hadoopConf, initialSnapshotValue, info)
  }

  override def toString(): String = "DeltaV2Table{identifier=" + identifier + '}'

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[DeltaV2Table]) {
      return false
    }
    val that = o.asInstanceOf[DeltaV2Table]
    if (this eq that) {
      return true
    }
    if (getClass != that.getClass) {
      return false
    }
    Objects.equals(identifier, that.identifier) &&
      Objects.equals(tablePath, that.tablePath) &&
      Objects.equals(options, that.options) &&
      Objects.equals(catalogTableValue, that.catalogTableValue) &&
      Objects.equals(
        initialSnapshotValue.dataPath.toString, that.initialSnapshotValue.dataPath.toString) &&
      initialSnapshotValue.version == that.initialSnapshotValue.version &&
      Objects.equals(
        initialSnapshotValue.metadata.getId, that.initialSnapshotValue.metadata.getId)
  }

  override def hashCode(): Int =
    Objects.hash(
      identifier,
      tablePath,
      options,
      catalogTableValue,
      initialSnapshotValue.dataPath.toString,
      Long.box(initialSnapshotValue.version),
      initialSnapshotValue.metadata.getId)
}

private[catalog] object DeltaV2Table {
  private val METADATA_COLUMN_NAME = FileFormat.METADATA_NAME
  private val ROW_ID_METADATA_FIELD_NAME = RowId.ROW_ID
  private val ROW_COMMIT_VERSION_METADATA_FIELD_NAME = RowCommitVersion.METADATA_STRUCT_FIELD_NAME

  private val CAPABILITIES = buildCapabilities()
  private val READ_ONLY_CAPABILITIES = buildReadOnlyCapabilities()

  private def buildCapabilities(): JSet[TableCapability] = {
    val caps = EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE)
    DeltaV2TableShims.schemaEvolutionCapability().foreach(capability => caps.add(capability))
    Collections.unmodifiableSet[TableCapability](caps)
  }

  private def buildReadOnlyCapabilities(): JSet[TableCapability] =
    Collections.unmodifiableSet[TableCapability](
      EnumSet.of(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ))

  /**
   * Helper method to decode URI path handling URL-encoded characters correctly. E.g., converts
   * "spark%25dir%25prefix" to "spark%dir%prefix"
   *
   * <p>Uses Hadoop's Path class to properly handle all URI schemes (file, s3, abfss, gs, hdfs,
   * etc.), not just file:// URIs.
   */
  def getDecodedPath(location: URI): String = {
    val hadoopPath = new Path(location)
    // For local file system paths, return just the path component without the scheme
    // to maintain consistency with path-based table construction where tablePath is a
    // plain filesystem path string.
    if (location.getScheme == null || "file".equals(location.getScheme)) {
      return hadoopPath.toUri.getPath
    }
    hadoopPath.toString
  }

  /**
   * Resolves a time travel timestamp to the active commit version using the Kernel snapshot
   * manager.
   *
   * <p>This loads the latest snapshot more than once (here and inside the Kernel lookup), make it
   * share a singular load once the snapshot manager exposes it TODO(#5999).
   */
  private def resolveTimestampToVersion(
      manager: DeltaV2TableManager,
      timestampMicros: Long,
      queryContext: DeltaV2QueryContext): Long = {
    val timeMillis = timestampMicros / 1000
    val kernelCommit: KernelDeltaHistoryManager.Commit = manager.getActiveCommitAtTime(
      timeMillis,
      /* canReturnLastCommit = */ true,
      /* mustBeRecreatable = */ true,
      /* canReturnEarliestCommit = */ true,
      queryContext)
    val latestVersion = manager.loadLatestSnapshot(queryContext).version
    if (kernelCommit.getTimestamp > timeMillis) {
      // The earliest available commit is younger than the requested time.
      throw new TimestampOutOfRangeException(timeMillis, kernelCommit.getTimestamp, false)
    } else if (kernelCommit.getVersion == latestVersion && kernelCommit.getTimestamp < timeMillis) {
      // The requested time is after the latest commit.
      throw new TimestampOutOfRangeException(timeMillis, kernelCommit.getTimestamp, true)
    }
    kernelCommit.getVersion
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
  private class SchemaProvider(
      sparkSession: SparkSession,
      rawSchema: StructType,
      partColNamesArg: JList[String]) {
    private val partColNames =
      Collections.unmodifiableList[String](new ArrayList[String](partColNamesArg))

    // Lazily computed fields
    private var initialized = false
    private var publicSchema: StructType = null
    private var dataSchema: StructType = null
    private var partitionSchema: StructType = null
    private var partitionTransforms: Array[Transform] = null

    private def ensureInitialized(): Unit = synchronized {
      if (initialized) {
        return
      }

      // Create public schema by removing internal metadata (for schema() method)
      publicSchema = DeltaTableUtils.removeInternalDeltaMetadata(
        sparkSession, DeltaTableUtils.removeInternalWriterMetadata(sparkSession, rawSchema))

      val dataFields = new ArrayList[StructField]()
      val partitionFields = new ArrayList[StructField]()

      // Build a map for O(1) field lookups to improve performance
      // Use rawSchema (with metadata) for deriving data and partition schemas
      val fieldMap = new HashMap[String, StructField]()
      rawSchema.fields.foreach { field =>
        fieldMap.put(field.name, field)
      }

      // IMPORTANT: Add partition fields in the exact order specified by partColNames
      // This is crucial because the order in partColNames may differ from the order
      // in snapshotSchema, and we need to preserve the partColNames order for
      // proper partitioning behavior
      partColNames.forEach { partColName =>
        val field = fieldMap.get(partColName)
        if (field != null) {
          partitionFields.add(field)
        }
      }

      // Add remaining fields as data fields (non-partition columns)
      // These are fields that exist in the schema but are not partition columns
      rawSchema.fields.foreach { field =>
        if (!partColNames.contains(field.name)) {
          dataFields.add(field)
        }
      }
      dataSchema = new StructType(dataFields.toArray(new Array[StructField](0)))
      partitionSchema = new StructType(partitionFields.toArray(new Array[StructField](0)))

      // Each partition column is a single top-level field whose name may contain dots, spaces, or
      // backticks. Expressions.identity parses its argument as a SQL multipart identifier, so
      // quote the name to keep it as one exact name part (e.g. "a.b" must not become [a, b]).
      partitionTransforms = partColNames
        .stream()
        .map[Transform](partColName =>
          Expressions.identity(QuotingUtils.quoteIdentifier(partColName)))
        .toArray[Transform](length => new Array[Transform](length))

      initialized = true
    }

    private def withInit[T](value: => T): T = {
      ensureInitialized()
      value
    }

    def getPublicSchema(): StructType = withInit(publicSchema)
    def getDataSchema(): StructType = withInit(dataSchema)
    def getPartitionSchema(): StructType = withInit(partitionSchema)
    def getRawSchema(): StructType = withInit(rawSchema)
    def getPartitionTransforms(): Array[Transform] = withInit(partitionTransforms)
  }
}

/**
 * Table.version() was added in Spark 4.1.
 * A direct override would not compile before Spark 4.1. This trait supplies the contract
 * independently so the same implementation also overrides the default method in Spark 4.1+.
 */
private[catalog] trait DeltaV2TableVersion {
  def version(): String
}
