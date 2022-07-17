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

import org.apache.spark.sql.delta.{CommitStats, DeltaErrors, DeltaLog, DeltaOperations, DeltaTableIdentifier, OptimisticTransaction, Serializable, Snapshot}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.FileNames.deltaFile
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

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
  protected def generateCandidateFileMap(
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
      deltaLog.fileFormat(txn.metadata),
      txn.metadata.format.options)(spark)
  }

  /**
   * Find the AddFile record corresponding to the file that was read as part of a
   * delete/update/merge operation.
   *
   * @param filePath The path to a file. Can be either absolute or relative
   * @param nameToAddFileMap Map generated through `generateCandidateFileMap()`
   */
  protected def getTouchedFile(
      basePath: Path,
      filePath: String,
      nameToAddFileMap: Map[String, AddFile]): AddFile = {
    val absolutePath = DeltaFileOperations.absolutePath(basePath.toString, filePath).toString
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
        case LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, None, _) => false
        // is table
        case LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, Some(_), _) =>
          true
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
   * by either the given [[path]] or [[tableIdentifier].
   *
   * @param spark [[SparkSession]] reference to use.
   * @param path Table location. Expects a non-empty [[tableIdentifier]] or [[path]].
   * @param tableIdentifier Table identifier. Expects a non-empty [[tableIdentifier]] or [[path]].
   * @param operationName Operation that is getting the DeltaLog, used in error messages.
   * @return DeltaLog of the table
   * @throws AnalysisException If either no Delta table exists at the given path/identifier or
   *                           there is neither [[path]] nor [[tableIdentifier]] is provided.
   */
  protected def getDeltaLog(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      operationName: String): DeltaLog = {
    val tablePath =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = spark.sessionState.catalog
        lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)

        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.nonEmpty =>
            new Path(id.path.get)
          case Some(id) if id.table.nonEmpty =>
            new Path(metadata.location)
          case _ =>
            if (metadata.tableType == CatalogTableType.VIEW) {
              throw DeltaErrors.viewNotSupported(operationName)
            }
            throw DeltaErrors.notADeltaTableException(operationName)
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException(operationName)
      }

    val deltaLog = DeltaLog.forTable(spark, tablePath)
    if (deltaLog.snapshot.version < 0) {
      throw DeltaErrors.notADeltaTableException(
        operationName,
        DeltaTableIdentifier(path, tableIdentifier))
    }
    deltaLog
  }

}
