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
import org.apache.spark.sql.delta.commands.convert.{ConvertTargetFileManifest, ConvertTargetTable, ConvertUtils}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.util._
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, V1Table}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

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
    val deltaPathToUse = new Path(deltaPath.getOrElse(convertProperties.targetDir))
    val deltaLog = DeltaLog.forTable(spark, deltaPathToUse)
    val txn = deltaLog.startTransaction(convertProperties.catalogTable)
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
    val initialSnapshot = new InitialSnapshot(txn.deltaLog.logPath, txn.deltaLog, txn.metadata)
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
        ConvertUtils.createAddFile(
          _, txn.deltaLog.dataPath, fs, conf, Some(partitionSchema), deltaPath.isDefined))
      if (shouldCollectStats) {
        logInfo(
          log"Collecting stats for a batch of ${MDC(DeltaLogKeys.NUM_FILES, batch.size)} files; " +
          log"finished ${MDC(DeltaLogKeys.NUM_FILES2, numFiles)} so far"
          )
        numFiles += statsBatchSize
        performStatsCollection(spark, txn, adds)
      } else if (collectStats) {
        logWarning(log"collectStats is set to true but ${MDC(DeltaLogKeys.CONFIG,
          DeltaSQLConf.DELTA_COLLECT_STATS.key)}" +
          log" is false. Skip statistics collection")
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
          ConvertUtils.getParquetTable(
            spark, target.targetDir, target.catalogTable, partitionSchema)
        case checkProvider if isIcebergPathProvider(checkProvider) =>
          if (partitionSchema.isDefined) {
            throw DeltaErrors.partitionSchemaInIcebergTables
          }
          ConvertUtils.getIcebergTable(spark, target.targetDir, None, None)
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
    txn.deltaLog.createLogDirectoriesIfNotExists()
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
      RowTracking.checkStatsCollectedIfRowTrackingSupported(
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
        addFilesIter,
        Some(txn.protocol),
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

object ConvertToDeltaCommand extends DeltaLogging {

  def isHiveStyleParquetTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.provider.contains("hive") && catalogTable.storage.serde.contains(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
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
}
