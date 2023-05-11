/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import java.io.Closeable
import java.lang.reflect.InvocationTargetException
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.VacuumCommand.{generateCandidateFileMap, getTouchedFile}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.util._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * Convert an existing parquet table to a delta table by creating delta logs based on
 * existing files. Here are the main components:
 *
 *   - File Listing:      Launch a spark job to list files from a given directory in parallel.
 *
 *   - Schema Inference:  Given an iterator on the file list result, we group the iterator into
 *                        sequential batches and launch a spark job to infer schema for each batch,
 *                        and finally merge schemas from all batches.
 *
 *   - Stats collection:  Again, we group the iterator on file list results into sequential batches
 *                        and launch a spark job to collect stats for each batch.
 *
 *   - Commit the files:  We take the iterator of files with stats and write out a delta
 *                        log file as the first commit. This bypasses the transaction protocol, but
 *                        it's ok as this would be the very first commit.
 *
 * @param tableIdentifier the target parquet table.
 * @param partitionSchema the partition schema of the table, required when table is partitioned.
 * @param collectStats Should collect column stats per file on convert.
 * @param deltaPath if provided, the delta log will be written to this location.
 */
abstract class ConvertToDeltaCommandBase(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType],
    collectStats: Boolean,
    deltaPath: Option[String]) extends LeafRunnableCommand with DeltaCommand {

  protected lazy val statsEnabled: Boolean = conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)

  protected lazy val icebergEnabled: Boolean =
    conf.getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_ENABLED)

  protected def isParquetPathProvider(provider: String): Boolean =
    provider.equalsIgnoreCase("parquet")

  protected def isIcebergPathProvider(provider: String): Boolean =
    icebergEnabled && provider.equalsIgnoreCase("iceberg")

  protected def isSupportedPathTableProvider(provider: String): Boolean = {
    isParquetPathProvider(provider) || isIcebergPathProvider(provider)
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val convertProperties = resolveConvertTarget(spark, tableIdentifier) match {
      case Some(props) if !DeltaSourceUtils.isDeltaTable(props.provider) => props
      case _ =>
        // Make convert to delta idempotent
        logConsole("The table you are trying to convert is already a delta table")
        return Seq.empty[Row]
    }

    val targetTable = getTargetTable(spark, convertProperties)
    val deltaLog = DeltaLog.forTable(spark, deltaPath.getOrElse(convertProperties.targetDir))
    val txn = deltaLog.startTransaction()
    if (txn.readVersion > -1) {
      handleExistingTransactionLog(spark, txn, convertProperties, targetTable.format)
      return Seq.empty[Row]
    }

    performConvert(spark, txn, convertProperties, targetTable)
  }

  /** Given the table identifier, figure out what our conversion target is. */
  private def resolveConvertTarget(
      spark: SparkSession,
      tableIdentifier: TableIdentifier): Option[ConvertTarget] = {
    val v2SessionCatalog =
      spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]

    // TODO: Leverage the analyzer for all this work
    if (isCatalogTable(spark.sessionState.analyzer, tableIdentifier)) {
      val namespace =
          tableIdentifier.database.map(Array(_))
            .getOrElse(spark.sessionState.catalogManager.currentNamespace)
      val ident = Identifier.of(namespace, tableIdentifier.table)
      v2SessionCatalog.loadTable(ident) match {
        case v1: V1Table if v1.catalogTable.tableType == CatalogTableType.VIEW =>
          throw DeltaErrors.operationNotSupportedException(
            "Converting a view to a Delta table",
            tableIdentifier)
        case v1: V1Table =>
          val table = v1.catalogTable
          // Hive adds some transient table properties which should be ignored
          val props = table.properties.filterKeys(_ != "transient_lastDdlTime").toMap
          Some(ConvertTarget(Some(table), table.provider, new Path(table.location).toString, props))
        case _: DeltaTableV2 =>
          // Already a Delta table
          None
      }
    } else {
      Some(ConvertTarget(
        None,
        tableIdentifier.database,
        tableIdentifier.table,
        Map.empty[String, String]))
    }
  }

  /**
   * When converting a table to delta using table name, we should also change the metadata in the
   * catalog table because the delta log should be the source of truth for the metadata rather than
   * the metastore.
   *
   * @param catalogTable metadata of the table to be converted
   * @param sessionCatalog session catalog of the metastore used to update the metadata
   */
  private def convertMetadata(
      catalogTable: CatalogTable,
      sessionCatalog: SessionCatalog): Unit = {
    var newCatalog = catalogTable.copy(
      provider = Some("delta"),
      // TODO: Schema changes unfortunately doesn't get reflected in the HiveMetaStore. Should be
      // fixed in Apache Spark
      schema = new StructType(),
      partitionColumnNames = Seq.empty,
      properties = Map.empty,
      // TODO: Serde information also doesn't get removed
      storage = catalogTable.storage.copy(
        inputFormat = None,
        outputFormat = None,
        serde = None)
    )
    sessionCatalog.alterTable(newCatalog)
    logInfo("Convert to Delta converted metadata")
  }

  /**
   * Calls DeltaCommand.isCatalogTable. With Convert, we may get a format check error in cases where
   * the metastore and the underlying table don't align, e.g. external table where the underlying
   * files are converted to delta but the metadata has not been converted yet. In these cases,
   * catch the error and return based on whether the provided Table Identifier could reasonably be
   * a path
   *
   * @param analyzer The session state analyzer to call
   * @param tableIdent Table Identifier to determine whether is path based or not
   * @return Boolean where true means that the table is a table in a metastore and false means the
   *         table is a path based table
   */
  override def isCatalogTable(analyzer: Analyzer, tableIdent: TableIdentifier): Boolean = {
    try {
      super.isCatalogTable(analyzer, tableIdentifier)
    } catch {
      case e: AnalysisException if e.getMessage.contains("Incompatible format detected") =>
        !isPathIdentifier(tableIdentifier)
      case e: AssertionError if e.getMessage.contains("Conflicting directory structures") =>
        !isPathIdentifier(tableIdentifier)
      case _: NoSuchTableException
          if tableIdent.database.isEmpty && new Path(tableIdent.table).isAbsolute =>
        throw DeltaErrors.missingProviderForConvertException(tableIdent.table)
    }
  }

  /**
   * Override this method since parquet paths are valid for Convert
   *
   * @param tableIdent the provided table or path
   * @return Whether or not the ident provided can refer to a table by path
   */
  override def isPathIdentifier(tableIdent: TableIdentifier): Boolean = {
    val provider = tableIdent.database.getOrElse("")
    // If db doesnt exist or db is called delta/tahoe then check if path exists
    (DeltaSourceUtils.isDeltaDataSourceName(provider) ||
      isSupportedPathTableProvider(provider)) &&
      new Path(tableIdent.table).isAbsolute
  }

  /**
   * If there is already a transaction log we should handle what happens when convert to delta is
   * run once again. It may be the case that the table is entirely converted i.e. the underlying
   * files AND the catalog (if one exists) are updated. Or it may be the case that the table is
   * partially converted i.e. underlying files are converted but catalog (if one exists)
   * has not been updated.
   *
   * @param spark spark session to get session catalog
   * @param txn existing transaction log
   * @param target properties that contains: the provider and the catalogTable when
   * converting using table name
   */
  private def handleExistingTransactionLog(
      spark: SparkSession,
      txn: OptimisticTransaction,
      target: ConvertTarget,
      sourceFormat: String): Unit = {
    // In the case that the table is a delta table but the provider has not been updated we should
    // update table metadata to reflect that the table is a delta table and table properties should
    // also be updated
    if (isParquetCatalogTable(target)) {
      val catalogTable = target.catalogTable
      val tableProps = target.properties
      val deltaLogConfig = txn.metadata.configuration
      val mergedConfig = deltaLogConfig ++ tableProps

      if (mergedConfig != deltaLogConfig) {
        if (deltaLogConfig.nonEmpty &&
            conf.getConf(DeltaSQLConf.DELTA_CONVERT_METADATA_CHECK_ENABLED)) {
          throw DeltaErrors.convertMetastoreMetadataMismatchException(tableProps, deltaLogConfig)
        }
        val newMetadata = txn.metadata.copy(
          configuration = mergedConfig
        )
        txn.commit(
          newMetadata :: Nil,
          DeltaOperations.Convert(
            numFiles = 0L,
            partitionSchema.map(_.fieldNames.toSeq).getOrElse(Nil),
            collectStats = false,
            catalogTable = catalogTable.map(t => t.identifier.toString),
            sourceFormat = Some(sourceFormat)
          ))
      }
      convertMetadata(
        catalogTable.get,
        spark.sessionState.catalog
      )
    } else {
      logConsole("The table you are trying to convert is already a delta table")
    }
  }

  /** Is the target table a parquet table defined in an external catalog. */
  private def isParquetCatalogTable(target: ConvertTarget): Boolean = {
    target.catalogTable match {
      case Some(ct) =>
        ConvertToDeltaCommand.isHiveStyleParquetTable(ct) ||
          target.provider.get.toLowerCase(Locale.ROOT) == "parquet"
      case None => false
    }
  }

  protected def performStatsCollection(
      spark: SparkSession,
      txn: OptimisticTransaction,
      addFiles: Seq[AddFile]): Iterator[AddFile] = {
    val initialSnapshot = new InitialSnapshot(txn.deltaLog.dataPath, txn.deltaLog, txn.metadata)
    ConvertToDeltaCommand.computeStats(txn.deltaLog, initialSnapshot, addFiles)
  }

  /**
   * Given the file manifest, create corresponding AddFile actions for the entire list of files.
   */
  protected def createDeltaActions(
      spark: SparkSession,
      manifest: ConvertTargetFileManifest,
      partitionSchema: StructType,
      txn: OptimisticTransaction,
      fs: FileSystem): Iterator[AddFile] = {
    val shouldCollectStats = collectStats && statsEnabled
    val statsBatchSize = conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_STATS_COLLECTION)
    var numFiles = 0L
    manifest.getFiles.grouped(statsBatchSize).flatMap { batch =>
      val adds = batch.map(
        ConvertToDeltaCommand.createAddFile(
          _, txn.deltaLog.dataPath, fs, conf, Some(partitionSchema), deltaPath.isDefined))
      if (shouldCollectStats) {
        logInfo(s"Collecting stats for a batch of ${batch.size} files; " +
          s"finished $numFiles so far")
        numFiles += statsBatchSize
        performStatsCollection(spark, txn, adds)
      } else if (collectStats) {
        logWarning(s"collectStats is set to true but ${DeltaSQLConf.DELTA_COLLECT_STATS.key}" +
          s" is false. Skip statistics collection")
        adds.toIterator
      } else {
        adds.toIterator
      }
    }
  }

  /** Get the instance of the convert target table, which provides file manifest and schema */
  protected def getTargetTable(spark: SparkSession, target: ConvertTarget): ConvertTargetTable = {
    target.provider match {
      case Some(providerName) => providerName.toLowerCase(Locale.ROOT) match {
        case checkProvider
          if target.catalogTable.exists(ConvertToDeltaCommand.isHiveStyleParquetTable) ||
            isParquetPathProvider(checkProvider) =>
          ConvertToDeltaCommand.getParquetTable(
            spark, target.targetDir, target.catalogTable, partitionSchema)
        case checkProvider if isIcebergPathProvider(checkProvider) =>
          if (partitionSchema.isDefined) {
            throw DeltaErrors.partitionSchemaInIcebergTables
          }
          ConvertToDeltaCommand.getIcebergTable(spark, target.targetDir, None, None)
        case other =>
          throw DeltaErrors.convertNonParquetTablesException(tableIdentifier, other)
      }
      case None =>
        throw DeltaErrors.missingProviderForConvertException(target.targetDir)
    }
  }

  /**
   * Converts the given table to a Delta table. First gets the file manifest for the table. Then
   * in the first pass, it infers the schema of the table. Then in the second pass, it generates
   * the relevant Actions for Delta's transaction log, namely the AddFile actions for each file
   * in the manifest. Once a commit is made, updates an external catalog, e.g. Hive MetaStore,
   * if this table was referenced through a table in a catalog.
   */
  private def performConvert(
      spark: SparkSession,
      txn: OptimisticTransaction,
      convertProperties: ConvertTarget,
      targetTable: ConvertTargetTable): Seq[Row] =
    recordDeltaOperation(txn.deltaLog, "delta.convert") {
    txn.deltaLog.ensureLogDirectoryExist()
    val targetPath = new Path(convertProperties.targetDir)
    // scalastyle:off deltahadoopconfiguration
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val fs = targetPath.getFileSystem(sessionHadoopConf)
    val manifest = targetTable.fileManifest
    try {
      if (!manifest.getFiles.hasNext) {
        throw DeltaErrors.emptyDirectoryException(convertProperties.targetDir)
      }

      val partitionFields = targetTable.partitionSchema
      val schema = targetTable.tableSchema
      val metadata = Metadata(
        schemaString = schema.json,
        partitionColumns = partitionFields.fieldNames,
        configuration = convertProperties.properties ++ targetTable.properties,
        createdTime = Some(System.currentTimeMillis()))
      txn.updateMetadataForNewTable(metadata)

      // TODO: we have not decided on how to implement CONVERT TO DELTA under column mapping modes
      //  for some convert targets so we block this feature for them here
      checkColumnMapping(txn.metadata, targetTable)
      RowId.checkStatsCollectedIfRowTrackingSupported(
        spark,
        txn.protocol,
        collectStats,
        statsEnabled)

      val numFiles = targetTable.numFiles
      val addFilesIter = createDeltaActions(spark, manifest, partitionFields, txn, fs)
      val metrics = Map[String, String](
        "numConvertedFiles" -> numFiles.toString
      )
      val (committedVersion, postCommitSnapshot) = txn.commitLarge(
        spark,
        Iterator.single(txn.protocol) ++ addFilesIter,
        getOperation(numFiles, convertProperties, targetTable.format),
        getContext,
        metrics)
    } finally {
      manifest.close()
    }

    // If there is a catalog table, convert metadata
    if (convertProperties.catalogTable.isDefined) {
      convertMetadata(
        convertProperties.catalogTable.get,
        spark.sessionState.catalog
      )
    }

    Seq.empty[Row]
  }

  protected def getContext: Map[String, String] = {
    Map.empty
  }

  /** Get the operation to store in the commit message. */
  protected def getOperation(
      numFilesConverted: Long,
      convertProperties: ConvertTarget,
      sourceFormat: String): DeltaOperations.Operation = {
    DeltaOperations.Convert(
      numFilesConverted,
      partitionSchema.map(_.fieldNames.toSeq).getOrElse(Nil),
      collectStats = collectStats && statsEnabled,
      convertProperties.catalogTable.map(t => t.identifier.toString),
      sourceFormat = Some(sourceFormat))
  }

  protected case class ConvertTarget(
      catalogTable: Option[CatalogTable],
      provider: Option[String],
      targetDir: String,
      properties: Map[String, String])

  private def checkColumnMapping(
      txnMetadata: Metadata,
      convertTargetTable: ConvertTargetTable): Unit = {
    if (convertTargetTable.requiredColumnMappingMode != txnMetadata.columnMappingMode) {
      throw DeltaErrors.convertToDeltaWithColumnMappingNotSupported(txnMetadata.columnMappingMode)
    }
  }

}

case class ConvertToDeltaCommand(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType],
    collectStats: Boolean,
    deltaPath: Option[String])
  extends ConvertToDeltaCommandBase(tableIdentifier, partitionSchema, collectStats, deltaPath)

/**
 * An interface for the file to be included during conversion.
 *
 * @param fileStatus the file info
 * @param partitionValues partition values of this file that may be available from the source
 *                        table format. If none, the converter will infer partition values from the
 *                        file path, assuming the Hive directory format.
 */
case class ConvertTargetFile(
    fileStatus: SerializableFileStatus,
    partitionValues: Option[Map[String, String]] = None) extends Serializable

/**
 * An interface for the table to be converted to Delta.
 */
trait ConvertTargetTable {
  /** The table schema of the target table */
  def tableSchema: StructType

  /** The table properties of the target table */
  def properties: Map[String, String] = Map.empty

  /** The partition schema of the target table */
  def partitionSchema: StructType

  /** The file manifest of the target table */
  def fileManifest: ConvertTargetFileManifest

  /** The number of files from the target table */
  def numFiles: Long

  /** Whether this table requires column mapping to be converted */
  def requiredColumnMappingMode: DeltaColumnMappingMode = NoMapping

  /* The format of the table */
  def format: String

}

class ParquetTable(
    val spark: SparkSession,
    val basePath: String,
    val catalogTable: Option[CatalogTable],
    val userPartitionSchema: Option[StructType]) extends ConvertTargetTable with DeltaLogging {
  // Validate user provided partition schema if catalogTable is available.
  if (catalogTable.isDefined && userPartitionSchema.isDefined
    && !catalogTable.get.partitionSchema.equals(userPartitionSchema.get)) {
    throw DeltaErrors.unexpectedPartitionSchemaFromUserException(
      catalogTable.get.partitionSchema, userPartitionSchema.get)
  }

  private var _numFiles: Option[Long] = None

  private var _tableSchema: Option[StructType] = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA)) {
      catalogTable.map(_.schema)
    } else {
      None
    }
  }

  protected lazy val serializableConf = {
    // scalastyle:off deltahadoopconfiguration
    new SerializableConfiguration(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }

  override val partitionSchema: StructType = {
    userPartitionSchema.orElse(catalogTable.map(_.partitionSchema)).getOrElse(new StructType())
  }

  def numFiles: Long = {
    if (_numFiles.isEmpty) {
      inferSchema()
    }
    _numFiles.get
  }

  def tableSchema: StructType = {
    if (_tableSchema.isEmpty) {
      inferSchema()
    }
    _tableSchema.get
  }

  override val format: String = "parquet"

  /**
   * This method is forked from [[ParquetFileFormat]]. The only change here is that we use
   * our SchemaMergingUtils.mergeSchemas() instead of StructType.merge(),
   * where we allow upcast between ByteType, ShortType and IntegerType.
   *
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  protected def mergeSchemasInParallel(
      sparkSession: SparkSession,
      filesToTouch: Seq[FileStatus],
      serializedConf: SerializableConfiguration): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
      sparkSession
        .sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          // Reads footers in multi-threaded manner within each task
          val footers =
            DeltaFileOperations.readParquetFootersInParallel(
              serializedConf.value, fakeFileStatuses, ignoreCorruptFiles)

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val converter = new ParquetToSparkSchemaConverter(
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp)
          if (footers.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
            footers.tail.foreach { footer =>
              val schema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
              try {
                mergedSchema = SchemaMergingUtils.mergeSchemas(mergedSchema, schema)
              } catch { case cause: AnalysisException =>
                throw DeltaErrors.failedMergeSchemaFile(
                  footer.getFile.toString, schema.treeString, cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        finalSchema = SchemaMergingUtils.mergeSchemas(finalSchema, schema)
      }
      Some(finalSchema)
    }
  }

  protected def getSchemaForBatch(
      spark: SparkSession,
      batch: Seq[SerializableFileStatus],
      serializedConf: SerializableConfiguration): StructType = {
    mergeSchemasInParallel(spark, batch.map(_.toFileStatus), serializedConf).getOrElse(
      throw DeltaErrors.failedInferSchema)
  }

  private def inferSchema(): Unit = {
    val initialList = fileManifest.getFiles.map(_.fileStatus)
    val schemaBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE)
    var numFiles = 0L
    var dataSchema: StructType = StructType(Seq())
    recordDeltaOperationForTablePath(basePath, "delta.convert.schemaInference") {
      initialList.grouped(schemaBatchSize).foreach { batch =>
        numFiles += batch.size
        // Obtain a union schema from all files.
        // Here we explicitly mark the inferred schema nullable. This also means we don't
        // currently support specifying non-nullable columns after the table conversion.
        val batchSchema =
          getSchemaForBatch(spark, batch, serializableConf).asNullable
        dataSchema = SchemaMergingUtils.mergeSchemas(dataSchema, batchSchema)
      }
    }

    val partitionFields = partitionSchema.fields.toSeq

    _numFiles = Some(numFiles)
    _tableSchema = Some(PartitioningUtils.mergeDataAndPartitionSchema(
      dataSchema,
      StructType(partitionFields),
      spark.sessionState.conf.caseSensitiveAnalysis)._1)
  }

  val fileManifest: ConvertTargetFileManifest = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_METADATA_LOG) &&
      FileStreamSink.hasMetadata(Seq(basePath), serializableConf.value, spark.sessionState.conf)) {
      new MetadataLogFileManifest(spark, basePath)
    } else if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_PARTITIONS) &&
      catalogTable.isDefined) {
      new CatalogFileManifest(spark, basePath, catalogTable.get, serializableConf)
    } else {
      new ManualListingFileManifest(spark, basePath, serializableConf)
    }
  }
}

/** An interface for providing an iterator of files for a table. */
trait ConvertTargetFileManifest extends Closeable {
  /** The base path of a table. Should be a qualified, normalized path. */
  val basePath: String

  /** Return all files as a Dataset for parallelized processing */
  def allFiles: Dataset[ConvertTargetFile]

  /** Return the active files for a table in sequence */
  def getFiles: Iterator[ConvertTargetFile] = allFiles.toLocalIterator().asScala
}

/** A file manifest generated through recursively listing a base path. */
class ManualListingFileManifest(
    spark: SparkSession,
    override val basePath: String,
    serializableConf: SerializableConfiguration) extends ConvertTargetFileManifest {

  protected def doList(): Dataset[SerializableFileStatus] = {
    val conf = spark.sparkContext.broadcast(serializableConf)
    DeltaFileOperations
      .recursiveListDirs(
        spark, Seq(basePath), conf, hiddenDirNameFilter = ConvertToDeltaCommand.hiddenDirNameFilter)
      .where("!isDir")
  }

  private lazy val list: Dataset[SerializableFileStatus] = {
    val ds = doList()
    ds.cache()
    ds
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._
    list.map(ConvertTargetFile(_))
  }

  override def close(): Unit = list.unpersist()
}

/** A file manifest generated through listing partition paths from Metastore catalog. */
class CatalogFileManifest(
  spark: SparkSession,
  override val basePath: String,
  catalogTable: CatalogTable,
  serializableConf: SerializableConfiguration)
  extends ConvertTargetFileManifest {

  // List of partition directories and corresponding partition values.
  private lazy val partitionList = {
    if (catalogTable.partitionSchema.isEmpty) {
      // Not a partitioned table.
      Seq(basePath -> Map.empty[String, String])
    } else {
      val partitions = spark.sessionState.catalog.listPartitions(catalogTable.identifier)
      partitions.map { partition =>
        val partitionDir = partition.storage.locationUri.map(_.toString())
          .getOrElse {
            val partitionDir =
              PartitionUtils.getPathFragment(partition.spec, catalogTable.partitionSchema)
            basePath.stripSuffix("/") + "/" + partitionDir
          }
        partitionDir -> partition.spec
      }
    }
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._
    if (partitionList.isEmpty) {
      throw DeltaErrors.convertToDeltaNoPartitionFound(catalogTable.identifier.unquotedString)
    }

    // Avoid the serialization of this CatalogFileManifest during distributed execution.
    val conf = spark.sparkContext.broadcast(serializableConf)
    val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism
    val rdd = spark.sparkContext.parallelize(partitionList)
      .repartition(math.min(parallelism, partitionList.length))
      .mapPartitions { partitions =>
        partitions.flatMap ( partition =>
          DeltaFileOperations
            .localListDirs(conf.value.value, Seq(partition._1), recursive = false).filter(!_.isDir)
            .map(ConvertTargetFile(_, Some(partition._2)))
        )
    }
    spark.createDataset(rdd).cache()
  }

  override def close(): Unit = allFiles.unpersist()
}

/** A file manifest generated from pre-existing parquet MetadataLog. */
class MetadataLogFileManifest(
    spark: SparkSession,
    override val basePath: String) extends ConvertTargetFileManifest {

  val index = new MetadataLogFileIndex(spark, new Path(basePath), Map.empty, None)

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._

    val rdd = spark.sparkContext.parallelize(index.allFiles).mapPartitions { _
        .map(SerializableFileStatus.fromStatus)
        .map(ConvertTargetFile(_))
    }
    val ds = spark.createDataset(rdd)
    ds.cache()
  }

  override def close(): Unit = allFiles.unpersist()
}

trait ConvertToDeltaCommandUtils extends DeltaLogging {

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  var icebergSparkTableClassPath = "org.apache.spark.sql.delta.IcebergTable"
  var icebergLibTableClassPath = "org.apache.iceberg.Table"

  def createAddFile(
      targetFile: ConvertTargetFile,
      basePath: Path,
      fs: FileSystem,
      conf: SQLConf,
      partitionSchema: Option[StructType],
      useAbsolutePath: Boolean = false): AddFile = {
    val partitionFields = partitionSchema.map(_.fields.toSeq).getOrElse(Nil)
    val partitionColNames = partitionSchema.map(_.fieldNames.toSeq).getOrElse(Nil)
    val physicalPartitionColNames = partitionSchema.map(_.map { f =>
      DeltaColumnMapping.getPhysicalName(f)
    }).getOrElse(Nil)
    val file = targetFile.fileStatus
    val path = file.getHadoopPath
    val partition = targetFile.partitionValues.getOrElse {
      // partition values are not provided by the source table format, so infer from the file path
      val pathStr = file.getHadoopPath.toUri.toString
      val dateFormatter = DateFormatter()
      val timestampFormatter =
        TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
      val resolver = conf.resolver
      val dir = if (file.isDir) file.getHadoopPath else file.getHadoopPath.getParent
      val (partitionOpt, _) = PartitionUtils.parsePartition(
        dir,
        typeInference = false,
        basePaths = Set(basePath),
        userSpecifiedDataTypes = Map.empty,
        validatePartitionColumns = false,
        java.util.TimeZone.getDefault,
        dateFormatter,
        timestampFormatter)

      partitionOpt.map { partValues =>
        if (partitionColNames.size != partValues.columnNames.size) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr, partValues.columnNames, partitionColNames)
        }

        val tz = Option(conf.sessionLocalTimeZone)
        // Check if the partition value can be casted to the provided type
        if (!conf.getConf(DeltaSQLConf.DELTA_CONVERT_PARTITION_VALUES_IGNORE_CAST_FAILURE)) {
          partValues.literals.zip(partitionFields).foreach { case (literal, field) =>
            if (literal.eval() != null &&
                Cast(literal, field.dataType, tz, ansiEnabled = false).eval() == null) {
              val partitionValue = Cast(literal, StringType, tz, ansiEnabled = false).eval()
              val partitionValueStr = Option(partitionValue).map(_.toString).orNull
              throw DeltaErrors.castPartitionValueException(partitionValueStr, field.dataType)
            }
          }
        }

        val values = partValues
          .literals
          .map(l => Cast(l, StringType, tz, ansiEnabled = false).eval())
          .map(Option(_).map(_.toString).orNull)

        partitionColNames.zip(partValues.columnNames).foreach { case (expected, parsed) =>
          if (!resolver(expected, parsed)) {
            throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(
              pathStr, parsed, expected)
          }
        }
        physicalPartitionColNames.zip(values).toMap
      }.getOrElse {
        if (partitionColNames.nonEmpty) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr, Seq.empty, partitionColNames)
        }
        Map[String, String]()
      }
    }

    val pathStrForAddFile = if (!useAbsolutePath) {
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, basePath, path)
      assert(!relativePath.isAbsolute,
        s"Fail to relativize path $path against base path $basePath.")
      relativePath.toUri.toString
    } else {
      path.toUri.toString
    }

    AddFile(pathStrForAddFile, partition, file.length, file.modificationTime, dataChange = true)
  }

  def isHiveStyleParquetTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.provider.contains("hive") && catalogTable.storage.serde.contains(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
  }

  def getQualifiedPath(spark: SparkSession, path: Path): Path = {
    // scalastyle:off deltahadoopconfiguration
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val fs = path.getFileSystem(sessionHadoopConf)
    val qualifiedPath = fs.makeQualified(path)
    if (!fs.exists(qualifiedPath)) {
      throw DeltaErrors.directoryNotFoundException(qualifiedPath.toString)
    }
    qualifiedPath
  }

  def hiddenDirNameFilter(fileName: String): Boolean = {
    // Allow partition column name starting with underscore and dot
    DeltaFileOperations.defaultHiddenFileFilter(fileName) && !fileName.contains("=")
  }

  def computeStats(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      addFiles: Seq[AddFile]): Iterator[AddFile] = {
    import org.apache.spark.sql.functions._
    val filesWithStats = deltaLog.createDataFrame(snapshot, addFiles)
      .groupBy(input_file_name()).agg(to_json(snapshot.statsCollector))

    val pathToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, addFiles)
    filesWithStats.collect().iterator.map { row =>
      val addFile = getTouchedFile(deltaLog.dataPath, row.getString(0), pathToAddFileMap)
      addFile.copy(stats = row.getString(1))
    }
  }

  def getParquetTable(
      spark: SparkSession,
      targetDir: String,
      catalogTable: Option[CatalogTable],
      partitionSchema: Option[StructType]): ConvertTargetTable = {
    val qualifiedDir = ConvertToDeltaCommand.getQualifiedPath(spark, new Path(targetDir)).toString
    new ParquetTable(spark, qualifiedDir, catalogTable, partitionSchema)
  }

  def getIcebergTable(
      spark: SparkSession,
      targetDir: String,
      sparkTable: Option[Table],
      tableSchema: Option[StructType]): ConvertTargetTable = {
    try {
      val clazz = Utils.classForName(icebergSparkTableClassPath)
      if (sparkTable.isDefined) {
        val constFromTable = clazz.getConstructor(
          classOf[SparkSession],
          Utils.classForName(icebergLibTableClassPath),
          classOf[Option[StructType]])
        val method = sparkTable.get.getClass.getMethod("table")
        constFromTable.newInstance(spark, method.invoke(sparkTable.get), tableSchema)
      } else {
        val baseDir = ConvertToDeltaCommand.getQualifiedPath(spark, new Path(targetDir)).toString
        val constFromPath = clazz.getConstructor(
          classOf[SparkSession], classOf[String], classOf[Option[StructType]])
        constFromPath.newInstance(spark, baseDir, tableSchema)
      }
    } catch {
      case e: ClassNotFoundException =>
        logError(s"Failed to find Iceberg class", e)
        throw DeltaErrors.icebergClassMissing(spark.sparkContext.getConf, e)
      case e: InvocationTargetException =>
        logError(s"Got error when creating an Iceberg Converter", e)
        // The better error is within the cause
        throw ExceptionUtils.getRootCause(e)
    }
  }
}

object ConvertToDeltaCommand extends ConvertToDeltaCommandUtils
