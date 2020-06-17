/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Helper trait for all delta commands.
 */
trait DeltaCommand extends DeltaLogging {
  /**
   * Converts string predicates into [[Expression]]s relative to a transaction.
   *
   * @throws AnalysisException if a non-partition column is referenced.
   */
  protected def parsePartitionPredicates(
      spark: SparkSession,
      predicate: String): Seq[Expression] = {
    try {
      spark.sessionState.sqlParser.parseExpression(predicate) :: Nil
    } catch {
      case e: ParseException =>
        throw new AnalysisException(s"Cannot recognize the predicate '$predicate'", cause = Some(e))
    }
  }

  protected def verifyPartitionPredicates(
      spark: SparkSession,
      partitionColumns: Seq[String],
      predicates: Seq[Expression]): Unit = {

    predicates.foreach { pred =>
      if (SubqueryExpression.hasSubquery(pred)) {
        throw new AnalysisException("Subquery is not supported in partition predicates.")
      }

      pred.references.foreach { col =>
        val nameEquality = spark.sessionState.conf.resolver
        partitionColumns.find(f => nameEquality(f, col.name)).getOrElse {
          throw new AnalysisException(
            s"Predicate references non-partition column '${col.name}'. " +
              "Only the partition columns may be referenced: " +
              s"[${partitionColumns.mkString(", ")}]")
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
      deltaLog.snapshot.fileFormat,
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
    val absolutePath = DeltaFileOperations.absolutePath(basePath.toUri.toString, filePath).toString
    nameToAddFileMap.getOrElse(absolutePath, {
      throw new IllegalStateException(s"File ($absolutePath) to be rewritten not found " +
        s"among candidate files:\n${nameToAddFileMap.keys.mkString("\n")}")
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
        case UnresolvedRelation(_) =>
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
}
