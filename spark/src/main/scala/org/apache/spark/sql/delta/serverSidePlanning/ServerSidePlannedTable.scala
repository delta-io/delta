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

package org.apache.spark.sql.delta.serverSidePlanning

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}

/**
 * Companion object for ServerSidePlannedTable with factory methods.
 */
object ServerSidePlannedTable extends DeltaLogging {
  /**
   * Credentials can be provided via table properties in three distinct methods:
   *
   * Method 1: Direct credential values (fixed mode)
   *   - Static credential values set directly in table properties
   *   - Includes legacy keys and Unity Catalog's "option.fs.*" format
   *
   * Method 2: Credential providers (renewable mode)
   *   - Configuration keys indicating credential providers are set up
   *   - Unity Catalog uses these for dynamically refreshed credentials
   *
   * Method 3: Dynamic credential keys (prefix matching)
   *   - Credentials in keys with dynamic names (e.g., Azure SAS with container/account)
   *   - Requires prefix matching rather than exact key matching
   */

  /**
   * Method 1: Property keys for direct credential values (fixed mode).
   * These are static credential values set by catalogs.
   */
  private val FIXED_CREDENTIAL_KEYS = Seq(
    // Legacy/generic credential keys
    "storage.credential",
    "credential",
    // Cloud provider specific credential keys
    "aws.temporary.credentials",
    "azure.temporary.credentials",
    "gcs.temporary.credentials",
    // Unity Catalog fixed mode credentials (prefixed with option.)
    "option.fs.s3a.access.key",
    "option.fs.s3a.secret.key",
    "option.fs.s3a.session.token",
    "option.fs.gs.auth.access.token"
    // Note: Azure SAS handled via prefix matching (see Method 3: DYNAMIC_CREDENTIAL_PREFIXES)
  )

  /**
   * Method 2: Property keys indicating credential providers are configured (renewable mode).
   * Unity Catalog uses these when credentials are refreshed dynamically rather than being
   * static values.
   */
  private val RENEWABLE_CREDENTIAL_KEYS = Seq(
    "option.fs.s3a.aws.credentials.provider",
    "option.fs.azure.sas.token.provider.type",
    "option.fs.gs.auth.access.token.provider"
  )

  /**
   * Method 3: Key prefixes for dynamic credential keys (prefix matching).
   * Azure SAS tokens use dynamic keys with container and account names embedded:
   * option.fs.azure.sas.<container>.<account>.dfs.core.windows.net
   */
  private val DYNAMIC_CREDENTIAL_PREFIXES = Seq(
    "option.fs.azure.sas."
  )

  /**
   * Determine if server-side planning should be used based on catalog type,
   * credential availability, and configuration.
   *
   * Decision logic:
   * - Requires enableServerSidePlanning flag to be enabled (prevents accidental enablement)
   * - In production: Also requires Unity Catalog table that lacks credentials
   * - In test mode: Only requires the enable flag (allows testing without UC setup)
   * - Otherwise use normal table loading path
   *
   * The logic is: ((isUnityCatalog && !hasCredentials) || skipUCRequirementForTests) && enableFlag
   *
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @param hasCredentials Whether the table has credentials available
   * @param enableServerSidePlanning Whether to enable server-side planning (config flag)
   * @param skipUCRequirementForTests Whether to skip Unity Catalog requirement for testing
   *                                   with non-UC tables
   * @return true if server-side planning should be used
   */
  private[serverSidePlanning] def shouldUseServerSidePlanning(
      isUnityCatalog: Boolean,
      hasCredentials: Boolean,
      enableServerSidePlanning: Boolean,
      skipUCRequirementForTests: Boolean): Boolean = {
    ((isUnityCatalog && !hasCredentials) || skipUCRequirementForTests) && enableServerSidePlanning
  }

  /**
   * Try to create a ServerSidePlannedTable if server-side planning is needed.
   * Returns None if not needed or if the planning client factory is not available.
   *
   * This method encapsulates all the logic to decide whether to use server-side planning:
   * - Checks if Unity Catalog table lacks credentials
   * - Checks if server-side planning is enabled via config (required for all cases)
   * - In test mode, Unity Catalog check is bypassed to allow testing
   * - Extracts catalog name and table identifiers
   * - Attempts to create the planning client
   *
   * Test coverage: ServerSidePlanningSuite tests verify the decision logic through
   * shouldUseServerSidePlanning() method with different input combinations.
   *
   * @param spark The SparkSession
   * @param ident The table identifier
   * @param table The loaded table from the delegate catalog
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @return Some(ServerSidePlannedTable) if server-side planning should be used, None otherwise
   */
  def tryCreate(
      spark: SparkSession,
      ident: Identifier,
      table: Table,
      isUnityCatalog: Boolean): Option[ServerSidePlannedTable] = {
    // Check if we should enable server-side planning (for testing)
    val enableServerSidePlanning =
      spark.conf.get(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false").toBoolean
    val hasTableCredentials = hasCredentials(table)

    // Check if we should use server-side planning
    if (shouldUseServerSidePlanning(
        isUnityCatalog, hasTableCredentials, enableServerSidePlanning,
        skipUCRequirementForTests = DeltaUtils.isTesting)) {
      val namespace = ident.namespace().mkString(".")
      val tableName = ident.name()

      // Create metadata from table
      val metadata = ServerSidePlanningMetadata.fromTable(table, spark, ident, isUnityCatalog)

      // Try to create ServerSidePlannedTable with server-side planning
      val plannedTable = tryCreate(spark, namespace, tableName, table.schema(), metadata)
      if (plannedTable.isEmpty) {
        logWarning(
          s"Server-side planning not available for catalog ${metadata.catalogName}. " +
            "Falling back to normal table loading.")
      }
      plannedTable
    } else {
      None
    }
  }

  /**
   * Try to create a ServerSidePlannedTable with server-side planning.
   * Returns None if the planning client factory is not available.
   *
   * @param spark The SparkSession
   * @param databaseName The database name (may include catalog prefix)
   * @param tableName The table name
   * @param tableSchema The table schema
   * @param metadata Metadata extracted from loadTable response
   * @return Some(ServerSidePlannedTable) if successful, None if factory not registered
   */
  private def tryCreate(
      spark: SparkSession,
      databaseName: String,
      tableName: String,
      tableSchema: StructType,
      metadata: ServerSidePlanningMetadata): Option[ServerSidePlannedTable] = {
    try {
      val client = ServerSidePlanningClientFactory.buildClient(spark, metadata)
      Some(new ServerSidePlannedTable(spark, databaseName, tableName, tableSchema, client))
    } catch {
      case _: IllegalStateException =>
        // Factory not registered - this shouldn't happen in production but could during testing
        None
    }
  }

  /**
   * Check if a table has credentials available.
   *
   * Unity Catalog tables may lack credentials when accessed without proper permissions.
   * UC injects credentials as table properties with "option." prefix.
   * See: https://github.com/unitycatalog/unitycatalog/blob/main/connectors/spark/src/main/scala/
   *   io/unitycatalog/spark/UCSingleCatalog.scala#L357
   *   and connectors/spark/src/main/scala/io/unitycatalog/spark/auth/CredPropsUtil.java
   *
   * Checks for credentials using three distinct methods (see constants above):
   * Method 1: Direct credential values (exact key match)
   * Method 2: Credential providers (renewable mode configuration)
   * Method 3: Dynamic credential keys (prefix matching for Azure SAS)
   */
  private def hasCredentials(table: Table): Boolean = {
    val properties = table.properties()

    // Method 1: Direct credential values (exact key match)
    if (FIXED_CREDENTIAL_KEYS.exists(key => properties.containsKey(key))) {
      return true
    }

    // Method 2: Credential providers (renewable mode)
    if (RENEWABLE_CREDENTIAL_KEYS.exists(key => properties.containsKey(key))) {
      return true
    }

    // Method 3: Dynamic credential keys (prefix match)
    // Example: option.fs.azure.sas.<container>.<account>.dfs.core.windows.net
    val hasMatchingPrefix = properties.keySet().asScala.exists { key =>
      DYNAMIC_CREDENTIAL_PREFIXES.exists(prefix => key.startsWith(prefix))
    }
    if (hasMatchingPrefix) {
      return true
    }

    false
  }
}

/**
 * A Spark Table implementation that uses server-side scan planning
 * to get the list of files to read. Used as a fallback when Unity Catalog
 * doesn't provide credentials.
 *
 * Similar to DeltaTableV2, we accept SparkSession as a constructor parameter
 * since Tables are created on the driver and are not serialized to executors.
 */
class ServerSidePlannedTable(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient)
    extends Table with SupportsRead with DeltaLogging {

  // Returns fully qualified name (e.g., "catalog.database.table").
  // The databaseName parameter receives ident.namespace().mkString(".") from DeltaCatalog,
  // which includes the catalog name when present, similar to DeltaTableV2's name() method.
  override def name(): String = s"$databaseName.$tableName"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ServerSidePlannedScanBuilder(spark, databaseName, tableName, tableSchema, planningClient)
  }
}

/**
 * ScanBuilder that uses ServerSidePlanningClient to plan the scan.
 * Implements SupportsPushDownFilters to enable WHERE clause pushdown to the server.
 * Implements SupportsPushDownRequiredColumns to enable column pruning pushdown to the server.
 * Implements SupportsPushDownLimit to enable LIMIT pushdown to the server.
 */
class ServerSidePlannedScanBuilder(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with DeltaLogging {

  // Filters that have been pushed down and will be sent to the server
  private var _pushedFilters: Array[Filter] = Array.empty

  // Required schema (columns to read). Defaults to full table schema.
  private var _requiredSchema: StructType = tableSchema

  // Limit that has been pushed down. None means no limit.
  private var _limit: Option[Int] = None

  /**
   * Push filters to the server-side planning client.
   *
   * Strategy:
   * - If ALL filters convert to server's native format: Returns empty array (no residuals)
   *   This enables Spark to push down LIMIT in addition to filters
   * - If ANY filter fails conversion: Returns all filters as residuals
   *   This falls back to safety mode where Spark re-applies all filters locally
   *
   * The server receives converted filters in both cases, but residuals provide a safety net
   * for correctness if the server silently ignores unsupported filters.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Store filters to send to IRC server
    _pushedFilters = filters

    // Strategy: Check if all filters can be converted upfront
    // Case 1: ALL convert -> return empty residuals -> enables filter+limit pushdown
    // Case 2: ANY fails -> return all residuals -> only filter pushdown (safety mode)

    if (filters.isEmpty) {
      // No filters to push
      return Array.empty
    }

    // Check if all filters are convertible
    val allConvertible = planningClient.canConvertFilters(filters)

    if (allConvertible) {
      // All filters successfully converted to server's native format
      // Trust that the server can handle them - return no residuals
      // This enables Spark to call pushLimit() for combined filter+limit pushdown
      logInfo(s"All ${filters.length} filters convertible, " +
              "returning empty residuals to enable limit pushdown")
      Array.empty
    } else {
      // At least one filter failed to convert
      // Return all filters as residuals for safety (Spark will re-apply)
      // Note: Server will still receive converted filters, but Spark provides safety net
      logWarning(s"Some filters failed to convert, " +
                 "returning all as residuals (limit pushdown disabled)")
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    _requiredSchema = requiredSchema
  }

  override def pushLimit(limit: Int): Boolean = {
    _limit = Some(limit)
    true  // Return true to indicate the limit is fully pushed down to the server
  }

  override def isPartiallyPushed(): Boolean = {
    // Return true if we have a limit - indicates partial pushdown so Spark applies it too
    _limit.isDefined
  }

  override def build(): Scan = {
    new ServerSidePlannedScan(
      spark, databaseName, tableName, tableSchema, planningClient, _pushedFilters, _requiredSchema,
      _limit)
  }
}

/**
 * Scan implementation that calls the server-side planning API to get file list.
 */
class ServerSidePlannedScan(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient,
    pushedFilters: Array[Filter],
    requiredSchema: StructType,
    limit: Option[Int]) extends Scan with Batch {

  override def readSchema(): StructType = requiredSchema

  override def toBatch: Batch = this

  // Convert pushed filters to a single Spark Filter for the API call.
  // If no filters, pass None. If filters exist, combine them into a single filter.
  private val combinedFilter: Option[Filter] = {
    if (pushedFilters.isEmpty) {
      None
    } else if (pushedFilters.length == 1) {
      Some(pushedFilters.head)
    } else {
      // Combine multiple filters with And
      Some(pushedFilters.reduce((left, right) => And(left, right)))
    }
  }

  // Only pass projection if columns are actually pruned (not SELECT *)
  // Extract field names for planning client (server only needs names, not types)
  // Use Spark's SchemaUtils.explodeNestedFieldNames to flatten and escape field names,
  // then filter out parent structs by keeping only fields that have no children.
  // For example, for schema STRUCT<a: STRUCT<b.c: STRING>>:
  //   - explodeNestedFieldNames returns: ["a", "a.`b.c`"]
  //   - We filter to leaf fields only: ["a.`b.c`"]
  // This ensures projections only include actual data columns, not parent containers.
  private val projectionColumnNames: Option[Seq[String]] = {
    if (requiredSchema.fieldNames.toSet == tableSchema.fieldNames.toSet) {
      None
    } else {
      val allFields = SchemaUtils.explodeNestedFieldNames(requiredSchema)
      Some(allFields.filter { field =>
        !allFields.exists(other => other.startsWith(field + "."))
      })
    }
  }

  // Call the server-side planning API to get the scan plan with files AND credentials
  private lazy val scanPlan: ScanPlan = planningClient.planScan(
    databaseName,
    tableName,
    combinedFilter,
    projectionColumnNames,
    limit)

  // Explicitly signal that columnar is unsupported to prevent early enumeration of the partitions
  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.UNSUPPORTED

  override def planInputPartitions(): Array[InputPartition] = {
    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      ServerSidePlannedFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ServerSidePlannedFilePartitionReaderFactory(
      spark, tableSchema, requiredSchema, scanPlan.credentials)
  }
}

/**
 * InputPartition representing a single file from the server-side scan plan.
 */
case class ServerSidePlannedFileInputPartition(
    filePath: String,
    fileSizeInBytes: Long,
    fileFormat: String) extends InputPartition

/**
 * Factory for creating PartitionReaders that read server-side planned files.
 * Builds reader functions on the driver for Parquet files.
 *
 * @param tableSchema The full table schema (all columns in the file)
 * @param requiredSchema The required schema (columns to read after projection pushdown)
 * @param credentials Optional storage credentials from server-side planning response
 */
class ServerSidePlannedFilePartitionReaderFactory(
    spark: SparkSession,
    tableSchema: StructType,
    requiredSchema: StructType,
    credentials: Option[ScanPlanStorageCredentials])
    extends PartitionReaderFactory {

  import org.apache.spark.util.SerializableConfiguration

  // scalastyle:off deltahadoopconfiguration
  // We use sessionState.newHadoopConf() here instead of deltaLog.newDeltaHadoopConf().
  // This means DataFrame options (like custom S3 credentials) passed by users will NOT be
  // included in the Hadoop configuration. This is intentional:
  // - Server-side planning uses server-provided credentials, not user-specified credentials
  // - ServerSidePlannedTable is NOT a Delta table, so we don't want Delta-specific options
  //   from deltaLog.newDeltaHadoopConf()
  // - General Spark options from spark.hadoop.* are included and work for all tables
  private val hadoopConf = {
    val conf = spark.sessionState.newHadoopConf()

    // Inject temporary credentials from IRC server response
    credentials.foreach { creds =>
      creds match {
        case S3Credentials(accessKeyId, secretAccessKey, sessionToken) =>
          conf.set("fs.s3a.access.key", accessKeyId)
          conf.set("fs.s3a.secret.key", secretAccessKey)
          conf.set("fs.s3a.session.token", sessionToken)

        case AzureCredentials(accountName, sasToken, containerName) =>
          // Format: fs.azure.sas.<container>.<account>.dfs.core.windows.net
          val sasKey = s"fs.azure.sas.$containerName.$accountName.dfs.core.windows.net"
          conf.set(sasKey, sasToken)

        case GcsCredentials(oauth2Token) =>
          conf.set("fs.gs.auth.access.token", oauth2Token)
      }
    }

    new SerializableConfiguration(conf)
  }
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader function for Parquet on the driver
  // This function will be serialized and sent to executors
  // tableSchema: All columns in the file (full table schema)
  // requiredSchema: Columns to actually read (after projection pushdown)
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = tableSchema,
    partitionSchema = StructType(Nil),
    requiredSchema = requiredSchema,
    filters = Seq.empty,
    options = Map(
      FileFormat.OPTION_RETURNING_BATCH -> "false"
    ),
    hadoopConf = hadoopConf.value
  )

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val filePartition = partition.asInstanceOf[ServerSidePlannedFileInputPartition]

    // Verify file format is Parquet
    // Scalastyle suppression needed: the caselocale regex incorrectly flags even correct usage
    // of toLowerCase(Locale.ROOT). Similar to PartitionUtils.scala and SchemaUtils.scala.
    // scalastyle:off caselocale
    if (filePartition.fileFormat.toLowerCase(Locale.ROOT) != "parquet") {
    // scalastyle:on caselocale
      throw new UnsupportedOperationException(
        s"File format '${filePartition.fileFormat}' is not supported. Only Parquet is supported.")
    }

    new ServerSidePlannedFilePartitionReader(filePartition, parquetReaderBuilder)
  }
}

/**
 * PartitionReader that reads a single file using a pre-built reader function.
 * The reader function was created on the driver and is executed on the executor.
 */
class ServerSidePlannedFilePartitionReader(
    partition: ServerSidePlannedFileInputPartition,
    readerBuilder: PartitionedFile => Iterator[InternalRow])
    extends PartitionReader[InternalRow] {

  // Create PartitionedFile for this file
  private val partitionedFile = PartitionedFile(
    partitionValues = InternalRow.empty,
    filePath = SparkPath.fromPathString(partition.filePath),
    start = 0,
    length = partition.fileSizeInBytes
  )

  // Track the iterator so we can close it properly
  // Using Option to avoid initializing the iterator if close() is called before next()
  private var readerIterator: Option[Iterator[InternalRow]] = None

  // Get or create the reader iterator
  private def getIterator: Iterator[InternalRow] = {
    readerIterator.getOrElse {
      val iter = readerBuilder(partitionedFile)
      readerIterator = Some(iter)
      iter
    }
  }

  override def next(): Boolean = {
    getIterator.hasNext
  }

  override def get(): InternalRow = {
    getIterator.next()
  }

  override def close(): Unit = {
    // Close the iterator if it implements AutoCloseable (which Parquet iterators do)
    readerIterator.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => // No cleanup needed
    }
  }
}
