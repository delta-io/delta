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
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaErrors, DeltaLog, DeltaOptions, DeltaTableIdentifier, DeltaTableUtils, OptimisticTransaction, ResolvedPathBasedNonDeltaTable}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.{DeltaTableV2, IcebergTablePlaceHolder}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, ResolvedTable, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Helper trait for all delta commands.
 */
trait DeltaCommand extends DeltaLogging {
  /**
   * Converts string predicates into [[Expression]]s relative to a transaction.
   *
   * @throws AnalysisException if a non-partition column is referenced.
   */
  protected def parsePredicates(
      spark: SparkSession,
      predicate: String): Seq[Expression] = {
    try {
      spark.sessionState.sqlParser.parseExpression(predicate) :: Nil
    } catch {
      case e: ParseException =>
        throw DeltaErrors.failedRecognizePredicate(predicate, e)
      case e: NullPointerException if predicate == null =>
        throw DeltaErrors.failedRecognizePredicate("NULL", e)
    }
  }

  def verifyPartitionPredicates(
      spark: SparkSession,
      partitionColumns: Seq[String],
      predicates: Seq[Expression]): Unit = {

    predicates.foreach { pred =>
      if (SubqueryExpression.hasSubquery(pred)) {
        throw DeltaErrors.unsupportSubqueryInPartitionPredicates()
      }

      pred.references.foreach { col =>
        val colName = col match {
          case u: UnresolvedAttribute =>
            // Note: `UnresolvedAttribute(Seq("a.b"))` and `UnresolvedAttribute(Seq("a", "b"))` will
            // return the same name. We accidentally treated the latter as the same as the former.
            // Because some users may already rely on it, we keep supporting both.
            u.nameParts.mkString(".")
          case _ => col.name
        }
        val nameEquality = spark.sessionState.conf.resolver
        partitionColumns.find(f => nameEquality(f, colName)).getOrElse {
          throw DeltaErrors.nonPartitionColumnReference(colName, partitionColumns)
        }
      }
    }
  }

  /**
   * Generates a map of file names to add file entries for operations where we will need to
   * rewrite files such as delete, merge, update. We expect file names to be unique, because
   * each file contains a UUID.
   */
  def generateCandidateFileMap(
      basePath: Path,
      candidateFiles: Seq[AddFile]): Map[String, AddFile] = {
    val nameToAddFileMap = candidateFiles.map(add =>
      DeltaFileOperations.absolutePath(basePath.toString, add.path).toString -> add).toMap
    assert(nameToAddFileMap.size == candidateFiles.length,
      s"File name collisions found among:\n${candidateFiles.map(_.path).mkString("\n")}")
    nameToAddFileMap
  }

  /**
   * This method provides the RemoveFile actions that are necessary for files that are touched and
   * need to be rewritten in methods like Delete, Update, and Merge.
   *
   * @param deltaLog The DeltaLog of the table that is being operated on
   * @param nameToAddFileMap A map generated using `generateCandidateFileMap`.
   * @param filesToRewrite Absolute paths of the files that were touched. We will search for these
   *                       in `candidateFiles`. Obtained as the output of the `input_file_name`
   *                       function.
   * @param operationTimestamp The timestamp of the operation
   */
  protected def removeFilesFromPaths(
      deltaLog: DeltaLog,
      nameToAddFileMap: Map[String, AddFile],
      filesToRewrite: Seq[String],
      operationTimestamp: Long): Seq[RemoveFile] = {
    filesToRewrite.map { absolutePath =>
      val addFile = getTouchedFile(deltaLog.dataPath, absolutePath, nameToAddFileMap)
      addFile.removeWithTimestamp(operationTimestamp)
    }
  }

  /**
   * Build a base relation of files that need to be rewritten as part of an update/delete/merge
   * operation.
   */
  protected def buildBaseRelation(
      spark: SparkSession,
      txn: OptimisticTransaction,
      actionType: String,
      rootPath: Path,
      inputLeafFiles: Seq[String],
      nameToAddFileMap: Map[String, AddFile]): HadoopFsRelation = {
    val deltaLog = txn.deltaLog
    val scannedFiles = inputLeafFiles.map(f => getTouchedFile(rootPath, f, nameToAddFileMap))
    val fileIndex = new TahoeBatchFileIndex(
      spark, actionType, scannedFiles, deltaLog, rootPath, txn.snapshot)
    HadoopFsRelation(
      fileIndex,
      partitionSchema = txn.metadata.partitionSchema,
      dataSchema = txn.metadata.schema,
      bucketSpec = None,
      deltaLog.fileFormat(txn.protocol, txn.metadata),
      txn.metadata.format.options)(spark)
  }

  /**
   * Find the AddFile record corresponding to the file that was read as part of a
   * delete/update/merge operation.
   *
   * @param basePath The path of the table. Must not be escaped.
   * @param escapedFilePath The path to a file that can be either absolute or relative. All special
   *                        chars in this path must be already escaped by URI standards.
   * @param nameToAddFileMap Map generated through `generateCandidateFileMap()`.
   */
  def getTouchedFile(
      basePath: Path,
      escapedFilePath: String,
      nameToAddFileMap: Map[String, AddFile]): AddFile = {
    val absolutePath =
      DeltaFileOperations.absolutePath(basePath.toString, escapedFilePath).toString
    nameToAddFileMap.getOrElse(absolutePath, {
      throw DeltaErrors.notFoundFileToBeRewritten(absolutePath, nameToAddFileMap.keys)
    })
  }

  /**
   * Use the analyzer to resolve the identifier provided
   * @param analyzer The session state analyzer to call
   * @param identifier Table Identifier to determine whether is path based or not
   * @return
   */
  protected def resolveIdentifier(analyzer: Analyzer, identifier: TableIdentifier): LogicalPlan = {
    EliminateSubqueryAliases(analyzer.execute(UnresolvedRelation(identifier)))
  }

  /**
   * Use the analyzer to see whether the provided TableIdentifier is for a path based table or not
   * @param analyzer The session state analyzer to call
   * @param tableIdent Table Identifier to determine whether is path based or not
   * @return Boolean where true means that the table is a table in a metastore and false means the
   *         table is a path based table
   */
  def isCatalogTable(analyzer: Analyzer, tableIdent: TableIdentifier): Boolean = {
    try {
      resolveIdentifier(analyzer, tableIdent) match {
        // is path
        case LogicalRelationWithTable(HadoopFsRelation(_, _, _, _, _, _), None) => false
        // is table
        case LogicalRelationWithTable(HadoopFsRelation(_, _, _, _, _, _), Some(_)) => true
        // is iceberg table
        case DataSourceV2Relation(_: IcebergTablePlaceHolder, _, _, _, _) => false
        // could not resolve table/db
        case _: UnresolvedRelation =>
          throw new NoSuchTableException(tableIdent.database.getOrElse(""), tableIdent.table)
        // other e.g. view
        case _ => true
      }
    } catch {
      // Checking for table exists/database exists may throw an error in some cases in which case,
      // see if the table is a path-based table, otherwise throw the original error
      case _: AnalysisException if isPathIdentifier(tableIdent) => false
    }
  }

  /**
   * Checks if the given identifier can be for a delta table's path
   * @param tableIdent Table Identifier for which to check
   */
  protected def isPathIdentifier(tableIdent: TableIdentifier): Boolean = {
    val provider = tableIdent.database.getOrElse("")
    // If db doesnt exist or db is called delta/tahoe then check if path exists
    DeltaSourceUtils.isDeltaDataSourceName(provider) && new Path(tableIdent.table).isAbsolute
  }

  /**
   * Utility method to return the [[DeltaLog]] of an existing Delta table referred
   * by either the given [[path]] or [[tableIdentifier]].
   *
   * @param spark [[SparkSession]] reference to use.
   * @param path Table location. Expects a non-empty [[tableIdentifier]] or [[path]].
   * @param tableIdentifier Table identifier. Expects a non-empty [[tableIdentifier]] or [[path]].
   * @param operationName Operation that is getting the DeltaLog, used in error messages.
   * @param hadoopConf Hadoop file system options used to build DeltaLog.
   * @return DeltaLog of the table
   * @throws AnalysisException If either no Delta table exists at the given path/identifier or
   *                           there is neither [[path]] nor [[tableIdentifier]] is provided.
   */
  protected def getDeltaLog(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      operationName: String,
      hadoopConf: Map[String, String] = Map.empty): DeltaLog = {
    val (deltaLog, catalogTable) =
      if (path.nonEmpty) {
        (DeltaLog.forTable(spark, new Path(path.get), hadoopConf), None)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = spark.sessionState.catalog
        lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)

        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.nonEmpty =>
            (DeltaLog.forTable(spark, new Path(id.path.get), hadoopConf), None)
          case Some(id) if id.table.nonEmpty =>
            (DeltaLog.forTable(spark, metadata, hadoopConf), Some(metadata))
          case _ =>
            if (metadata.tableType == CatalogTableType.VIEW) {
              throw DeltaErrors.viewNotSupported(operationName)
            }
            throw DeltaErrors.notADeltaTableException(operationName)
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException(operationName)
      }

    val startTime = Some(System.currentTimeMillis)
    if (deltaLog
        .update(checkIfUpdatedSinceTs = startTime, catalogTableOpt = catalogTable)
        .version < 0) {
      throw DeltaErrors.notADeltaTableException(
        operationName,
        DeltaTableIdentifier(path, tableIdentifier))
    }
    deltaLog
  }

  /**
   * Send the driver-side metrics.
   *
   * This is needed to make the SQL metrics visible in the Spark UI.
   * All metrics are default initialized with 0 so that's what we're
   * reporting in case we skip an already executed action.
   */
  protected def sendDriverMetrics(spark: SparkSession, metrics: Map[String, SQLMetric]): Unit = {
    val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
  }

  /**
   * Extracts the [[DeltaTableV2]] from a LogicalPlan iff the LogicalPlan is a [[ResolvedTable]]
   * with either a [[DeltaTableV2]] or a [[V1Table]] that is referencing a Delta table. In all
   * other cases this method will throw a "Table not found" exception.
   */
  def getDeltaTable(target: LogicalPlan, cmd: String): DeltaTableV2 = {
    // TODO: Remove this wrapper and let former callers invoke DeltaTableV2.extractFrom directly.
    DeltaTableV2.extractFrom(target, cmd)
  }

  /**
   * Extracts [[CatalogTable]] metadata from a LogicalPlan if the plan is a [[ResolvedTable]]. The
   * table can be a non delta table.
   */
  def getTableCatalogTable(target: LogicalPlan, cmd: String): Option[CatalogTable] = {
    target match {
      case ResolvedTable(_, _, d: DeltaTableV2, _) => d.catalogTable
      case ResolvedTable(_, _, t: V1Table, _) => Some(t.catalogTable)
      case _ => None
    }
  }

  /**
   * Helper method to extract the table id or path from a LogicalPlan representing
   * a Delta table. This uses [[DeltaCommand.getDeltaTable]] to convert the LogicalPlan
   * to a [[DeltaTableV2]] and then extracts either the path or identifier from it. If
   * the [[DeltaTableV2]] has a [[CatalogTable]], the table identifier will be returned.
   * Otherwise, the table's path will be returned. Throws an exception if the LogicalPlan
   * does not represent a Delta table.
   */
  def getDeltaTablePathOrIdentifier(
      target: LogicalPlan,
      cmd: String): (Option[TableIdentifier], Option[String]) = {
    val table = getDeltaTable(target, cmd)
    table.catalogTable match {
      case Some(catalogTable)
        => (Some(catalogTable.identifier), None)
      case _ => (None, Some(table.path.toString))
    }
  }

  /**
   * Helper method to extract the table id or path from a LogicalPlan representing a resolved table
   * or path. This calls getDeltaTablePathOrIdentifier if the resolved table is a delta table. For
   * non delta table with identifier, we extract its identifier. For non delta table with path, it
   * expects the path to be wrapped in an ResolvedPathBasedNonDeltaTable and extracts it from there.
   */
  def getTablePathOrIdentifier(
      target: LogicalPlan,
      cmd: String): (Option[TableIdentifier], Option[String]) = {
    target match {
      case ResolvedTable(_, _, t: DeltaTableV2, _) => getDeltaTablePathOrIdentifier(target, cmd)
      case ResolvedTable(_, _, t: V1Table, _) if DeltaTableUtils.isDeltaTable(t.catalogTable) =>
        getDeltaTablePathOrIdentifier(target, cmd)
      case ResolvedTable(_, _, t: V1Table, _) => (Some(t.catalogTable.identifier), None)
      case p: ResolvedPathBasedNonDeltaTable => (None, Some(p.path))
      case _ => (None, None)
    }
  }

  /**
   * Returns true if there is information in the spark session that indicates that this write
   * has already been successfully written.
   */
  protected def hasBeenExecuted(txn: OptimisticTransaction, sparkSession: SparkSession,
    options: Option[DeltaOptions] = None): Boolean = {
    val (txnVersionOpt, txnAppIdOpt, isFromSessionConf) = getTxnVersionAndAppId(
      sparkSession, options)
    // only enter if both txnVersion and txnAppId are set
    for (version <- txnVersionOpt; appId <- txnAppIdOpt) {
      val currentVersion = txn.txnVersion(appId)
      if (currentVersion >= version) {
        logInfo(log"Already completed batch ${MDC(DeltaLogKeys.VERSION, version)} in application " +
          log"${MDC(DeltaLogKeys.APP_ID, appId)}. This will be skipped.")
        if (isFromSessionConf && sparkSession.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_IDEMPOTENT_DML_AUTO_RESET_ENABLED)) {
          // if we got txnAppId and txnVersion from the session config, we reset the
          // version here, after skipping the current transaction, as a safety measure to
          // prevent data loss if the user forgets to manually reset txnVersion
          sparkSession.sessionState.conf.unsetConf(DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_VERSION)
        }
        return true
      }
    }
    false
  }

  /**
   * Returns SetTransaction if a valid app ID and version are present. Otherwise returns
   * an empty list.
   */
  protected def createSetTransaction(
    sparkSession: SparkSession,
    deltaLog: DeltaLog,
    options: Option[DeltaOptions] = None): Option[SetTransaction] = {
    val (txnVersionOpt, txnAppIdOpt, isFromSessionConf) = getTxnVersionAndAppId(
      sparkSession, options)
    // only enter if both txnVersion and txnAppId are set
    for (version <- txnVersionOpt; appId <- txnAppIdOpt) {
      if (isFromSessionConf && sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_IDEMPOTENT_DML_AUTO_RESET_ENABLED)) {
        // if we got txnAppID and txnVersion from the session config, we reset the
        // version here as a safety measure to prevent data loss if the user forgets
        // to manually reset txnVersion
        sparkSession.sessionState.conf.unsetConf(DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_VERSION)
      }
      return Some(SetTransaction(appId, version, Some(deltaLog.clock.getTimeMillis())))
    }
    None
  }

  /**
   * Helper method to retrieve the current txn version and app ID. These are either
   * retrieved from user-provided write options or from session configurations.
   */
  private def getTxnVersionAndAppId(
    sparkSession: SparkSession,
    options: Option[DeltaOptions]): (Option[Long], Option[String], Boolean) = {
    var txnVersion: Option[Long] = None
    var txnAppId: Option[String] = None
    for (o <- options) {
      txnVersion = o.txnVersion
      txnAppId = o.txnAppId
    }

    var numOptions = txnVersion.size + txnAppId.size
    // numOptions can only be 0 or 2, as enforced by
    // DeltaWriteOptionsImpl.validateIdempotentWriteOptions so this
    // assert should never be triggered
    assert(numOptions == 0 || numOptions == 2, s"Only one of txnVersion and txnAppId " +
      s"has been set via dataframe writer options: txnVersion = $txnVersion txnAppId = $txnAppId")
    var fromSessionConf = false
    if (numOptions == 0) {
      txnVersion = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_VERSION)
      // don't need to check for valid conversion to Long here as that
      // is already enforced at set time
      txnAppId = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_APP_ID)
      // check that both session configs are set
      numOptions = txnVersion.size + txnAppId.size
      if (numOptions != 0 && numOptions != 2) {
        throw DeltaErrors.invalidIdempotentWritesOptionsException(
          "Both spark.databricks.delta.write.txnAppId and " +
            "spark.databricks.delta.write.txnVersion must be specified for " +
            "idempotent Delta writes")
      }
      fromSessionConf = true
    }
    (txnVersion, txnAppId, fromSessionConf)
  }

}
