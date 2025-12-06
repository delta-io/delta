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
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Companion object for ServerSidePlannedTable with factory methods.
 */
object ServerSidePlannedTable extends DeltaLogging {
  /**
   * Property keys that indicate table credentials are available.
   * Unity Catalog tables may expose temporary credentials via these properties.
   */
  private val CREDENTIAL_PROPERTY_KEYS = Seq(
    "storage.credential",
    "aws.temporary.credentials",
    "azure.temporary.credentials",
    "gcs.temporary.credentials",
    "credential"
  )

  /**
   * Determine if server-side planning should be used based on catalog type,
   * credential availability, and configuration.
   *
   * Decision logic:
   * - Use server-side planning if forceServerSidePlanning is true (config override)
   * - Use server-side planning if Unity Catalog table lacks credentials
   * - Otherwise use normal table loading path
   *
   * @param isUnityCatalog Whether this is a Unity Catalog instance
   * @param hasCredentials Whether the table has credentials available
   * @param forceServerSidePlanning Whether to force server-side planning (config flag)
   * @return true if server-side planning should be used
   */
  private[serverSidePlanning] def shouldUseServerSidePlanning(
      isUnityCatalog: Boolean,
      hasCredentials: Boolean,
      forceServerSidePlanning: Boolean): Boolean = {
    (isUnityCatalog && !hasCredentials) || forceServerSidePlanning
  }

  /**
   * Try to create a ServerSidePlannedTable if server-side planning is needed.
   * Returns None if not needed or if the planning client factory is not available.
   *
   * This method encapsulates all the logic to decide whether to use server-side planning:
   * - Checks if Unity Catalog table lacks credentials
   * - Checks if server-side planning is forced via config (for testing)
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
    // Check if we should force server-side planning (for testing)
    val forceServerSidePlanning =
      spark.conf.get(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false").toBoolean
    val hasTableCredentials = hasCredentials(table)

    // Check if we should use server-side planning
    if (shouldUseServerSidePlanning(isUnityCatalog, hasTableCredentials, forceServerSidePlanning)) {
      val namespace = ident.namespace().mkString(".")
      val tableName = ident.name()

      // Extract catalog name from identifier namespace, or default to spark_catalog
      //
      // Spark Identifier structure:
      // - For "catalog.database.table": namespace() = ["catalog", "database"], name() = "table"
      // - For "database.table":          namespace() = ["database"], name() = "table"
      // - For "table":                   namespace() = [], name() = "table"
      //
      // Note: We check namespace().length > 1 (not >= 1) because a single-element namespace
      // represents just the database name without an explicit catalog, so we use the default.
      // See Spark's LookupCatalog, CatalogAndIdentifier and ResolveSessionCatalog.
      val catalogName = if (ident.namespace().length > 1) {
        ident.namespace().head
      } else {
        "spark_catalog"
      }

      // Try to create ServerSidePlannedTable with server-side planning
      try {
        val client = ServerSidePlanningClientFactory.buildForCatalog(spark, catalogName)
        Some(new ServerSidePlannedTable(spark, namespace, tableName, table.schema(), client))
      } catch {
        case _: IllegalStateException =>
          // Factory not registered - fall through to normal path
          logWarning(s"Server-side planning not available for catalog $catalogName. " +
            "Falling back to normal table loading.")
          None
      }
    } else {
      None
    }
  }

  /**
   * Check if a table has credentials available.
   * Unity Catalog tables may lack credentials when accessed without proper permissions.
   * UC injects credentials as table properties, see:
   * https://github.com/unitycatalog/unitycatalog/blob/main/connectors/spark/src/main/scala/
   *   io/unitycatalog/spark/UCSingleCatalog.scala#L260
   */
  private def hasCredentials(table: Table): Boolean = {
    // Check table properties for credential information
    val properties = table.properties()
    CREDENTIAL_PROPERTY_KEYS.exists(key => properties.containsKey(key))
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
 */
class ServerSidePlannedScanBuilder(
    spark: SparkSession,
    databaseName: String,
    tableName: String,
    tableSchema: StructType,
    planningClient: ServerSidePlanningClient)
  extends ScanBuilder with SupportsPushDownFilters {

  // Filters that have been pushed down and will be sent to the server
  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Store filters to send to catalog, but return all as residuals.
    // Since we don't know what the catalog can handle yet, we conservatively claim we handle
    // none. Even if the catalog applies some filters, Spark will redundantly re-apply them.
    // This is inefficient but guarantees correct results. The alternative (claiming we handle
    // filters we don't support) would produce wrong output, which is unacceptable.
    // TODO: Quantify performance impact and add residual filter handling with catalog capabilities.
    _pushedFilters = filters
    filters  // Return all as residuals
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    new ServerSidePlannedScan(
      spark, databaseName, tableName, tableSchema, planningClient, _pushedFilters)
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
    pushedFilters: Array[Filter]) extends Scan with Batch {

  override def readSchema(): StructType = tableSchema

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

  override def planInputPartitions(): Array[InputPartition] = {
    // Call the server-side planning API to get the scan plan
    val scanPlan = planningClient.planScan(databaseName, tableName, combinedFilter)

    // Convert each file to an InputPartition
    scanPlan.files.map { file =>
      ServerSidePlannedFileInputPartition(file.filePath, file.fileSizeInBytes, file.fileFormat)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ServerSidePlannedFilePartitionReaderFactory(spark, tableSchema)
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
 */
class ServerSidePlannedFilePartitionReaderFactory(
    spark: SparkSession,
    tableSchema: StructType)
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
  private val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  // Pre-build reader function for Parquet on the driver
  // This function will be serialized and sent to executors
  private val parquetReaderBuilder = new ParquetFileFormat().buildReaderWithPartitionValues(
    sparkSession = spark,
    dataSchema = tableSchema,
    partitionSchema = StructType(Nil),
    requiredSchema = tableSchema,
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

  // Call the pre-built reader function with our PartitionedFile
  // This happens on the executor and doesn't need SparkSession
  private lazy val readerIterator: Iterator[InternalRow] = {
    readerBuilder(partitionedFile)
  }

  override def next(): Boolean = {
    readerIterator.hasNext
  }

  override def get(): InternalRow = {
    readerIterator.next()
  }

  override def close(): Unit = {
    // Reader cleanup is handled by Spark
  }
}
