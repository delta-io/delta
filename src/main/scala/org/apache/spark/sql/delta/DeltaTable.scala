/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Extractor Object for pulling out the table scan of a Delta table. It could be a full scan
 * or a partial scan.
 */
object DeltaTable {
  def unapply(a: LogicalRelation): Option[TahoeFileIndex] = a match {
    case LogicalRelation(HadoopFsRelation(index: TahoeFileIndex, _, _, _, _, _), _, _, _) =>
      Some(index)
    case _ =>
      None
  }
}

/**
 * Extractor Object for pulling out the full table scan of a Delta table.
 */
object DeltaFullTable {
  def unapply(a: LogicalPlan): Option[TahoeLogFileIndex] = a match {
    case PhysicalOperation(_, filters, DeltaTable(index: TahoeLogFileIndex)) =>
      if (index.partitionFilters.isEmpty && index.versionToUse.isEmpty && filters.isEmpty) {
        Some(index)
      } else if (index.versionToUse.nonEmpty) {
        throw new AnalysisException(
          s"Expect a full scan of the latest version of the Delta source, but found a historical " +
          s"scan of version ${index.versionToUse.get}")
      } else {
        throw new AnalysisException(
          s"Expect a full scan of Delta sources, but found a partial scan. path:${index.path}")
      }
    case _ =>
      None
  }
}

object DeltaTableUtils extends PredicateHelper
  with DeltaLogging {

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(table: CatalogTable): Boolean = DeltaSourceUtils.isDeltaTable(table.provider)

  /**
   * Check whether the provided table name is a Delta table based on information from the Catalog.
   */
  def isDeltaTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTemporaryTable(tableName)
    val tableExists =
      (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
      catalog.tableExists(tableName)
    tableIsNotTemporaryTable && tableExists && isDeltaTable(catalog.getTableMetadata(tableName))
  }

  /** Check if the provided path is the root or the children of a Delta table. */
  def isDeltaTable(spark: SparkSession, path: Path): Boolean = {
    findDeltaTableRoot(spark, path).isDefined
  }

  /**
   * Checks whether TableIdentifier is a path or a table name
   * We assume it is a path unless the table and database both exist in the catalog
   * @param catalog session catalog used to check whether db/table exist
   * @param tableIdent the provided table or path
   * @return true if using table name, false if using path, error otherwise
   */
  def isCatalogTable(catalog: SessionCatalog, tableIdent: TableIdentifier): Boolean = {
    val (dbExists, assumePath) = dbExistsAndAssumePath(catalog, tableIdent)

    // If we don't need to check that the table exists, return false since we think the tableIdent
    // refers to a path at this point, because the database doesn't exist
    if (assumePath) return false

    // check for dbexists otherwise catalog.tableExists may throw NoSuchDatabaseException
    if ((dbExists || tableIdent.database.isEmpty)
        && Try(catalog.tableExists(tableIdent)).getOrElse(false)) {
      true
    } else if (isValidPath(tableIdent)) {
      false
    } else {
      throw new NoSuchTableException(tableIdent.database.getOrElse(""), tableIdent.table)
    }
  }

  /**
   * It's possible that checking whether database exists can throw an exception. In that case,
   * we want to surface the exception only if the provided tableIdentifier cannot be a path.
   *
   * @param catalog session catalog used to check whether db/table exist
   * @param ident the provided table or path
   * @return tuple where first indicates whether database exists and second indicates whether there
   *         is a need to check whether table exists
   */
  private def dbExistsAndAssumePath(
      catalog: SessionCatalog,
      ident: TableIdentifier): (Boolean, Boolean) = {
    Try(ident.database.forall(catalog.databaseExists)) match {
      // DB exists, check table exists only if path is not valid
      case Success(true) => (true, false)
      // DB does not exist, check table exists only if path does not exist
      case Success(false) => (false, new Path(ident.table).isAbsolute)
      // Checking DB exists threw exception, if the path is still valid then check for table exists
      case Failure(_) if isValidPath(ident) => (false, true)
      // Checking DB exists threw exception, path is not valid so throw the initial exception
      case Failure(e) => throw e
    }
  }

  /**
   * @param tableIdent the provided table or path
   * @return whether or not the provided TableIdentifier can specify a path for parquet or delta
   */
  def isValidPath(tableIdent: TableIdentifier): Boolean = {
    // If db doesnt exist or db is called delta/tahoe then check if path exists
    DeltaSourceUtils.isDeltaDataSourceName(tableIdent.database.getOrElse("")) &&
      new Path(tableIdent.table).isAbsolute
  }

  /** Find the root of a Delta table from the provided path. */
  def findDeltaTableRoot(
      spark: SparkSession,
      path: Path,
      options: Map[String, String] = Map.empty): Option[Path] = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConfWithOptions(options))
    var currentPath = path
    while (currentPath != null && currentPath.getName != "_delta_log" &&
        currentPath.getName != "_samples") {
      val deltaLogPath = new Path(currentPath, "_delta_log")
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent
    }
    None
  }

  /** Whether a path should be hidden for delta-related file operations, such as Vacuum and Fsck. */
  def isHiddenDirectory(partitionColumnNames: Seq[String], pathName: String): Boolean = {
    // Names of the form partitionCol=[value] are partition directories, and should be
    // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    // indexes and these must be GCed when the data they are tied to is GCed.
    (pathName.startsWith(".") || pathName.startsWith("_")) &&
      !pathName.startsWith("_delta_index") &&
      !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
  }

  /**
   * Enrich the metadata received from the catalog on Delta tables with the Delta table metadata.
   */
  def combineWithCatalogMetadata(sparkSession: SparkSession, table: CatalogTable): CatalogTable = {
    val deltaLog = DeltaLog.forTable(sparkSession, new Path(table.location))
    val metadata = deltaLog.snapshot.metadata
    table.copy(schema = metadata.schema, partitionColumnNames = metadata.partitionColumns)
  }

  /**
   * Does the predicate only contains partition columns?
   */
  def isPredicatePartitionColumnsOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }

  /**
   * Partition the given condition into two sequence of conjunctive predicates:
   * - predicates that can be evaluated using metadata only.
   * - other predicates.
   */
  def splitMetadataAndDataPredicates(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition(
      isPredicateMetadataOnly(_, partitionColumns, spark))
  }

  /**
   * Check if condition involves a subquery expression.
   */
  def containsSubquery(condition: Expression): Boolean = {
    SubqueryExpression.hasSubquery(condition)
  }

  /**
   * Check if condition can be evaluated using only metadata. In Delta, this means the condition
   * only references partition columns and involves no subquery.
   */
  def isPredicateMetadataOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    isPredicatePartitionColumnsOnly(condition, partitionColumns, spark) &&
      !containsSubquery(condition)
  }

  /**
   * Replace the file index in a logical plan and return the updated plan.
   * It's a common pattern that, in Delta commands, we use data skipping to determine a subset of
   * files that can be affected by the command, so we replace the whole-table file index in the
   * original logical plan with a new index of potentially affected files, while everything else in
   * the original plan, e.g., resolved references, remain unchanged.
   *
   * @param target the logical plan in which we replace the file index
   * @param fileIndex the new file index
   */
  def replaceFileIndex(
      target: LogicalPlan,
      fileIndex: FileIndex): LogicalPlan = {
    target transform {
      case l @ LogicalRelation(hfsr: HadoopFsRelation, _, _, _) =>
        l.copy(relation = hfsr.copy(location = fileIndex)(hfsr.sparkSession))
    }
  }


  /**
   * Check if the given path contains time travel syntax with the `@`. If the path genuinely exists,
   * return `None`. If the path doesn't exist, but is specifying time travel, return the
   * `DeltaTimeTravelSpec` as well as the real path.
   */
  def extractIfPathContainsTimeTravel(
      session: SparkSession,
      path: String): (String, Option[DeltaTimeTravelSpec]) = {
    val conf = session.sessionState.conf
    if (!DeltaTimeTravelSpec.isApplicable(conf, path)) return path -> None

    val maybePath = new Path(path)
    val fs = maybePath.getFileSystem(session.sessionState.newHadoopConf())

    // If the folder really exists, quit
    if (fs.exists(maybePath)) return path -> None

    val (tt, realPath) = DeltaTimeTravelSpec.resolvePath(conf, path)
    realPath -> Some(tt)
  }

  /**
   * Given a time travel node, resolve which version it is corresponding to for the given table and
   * return the resolved version as well as the access type, i.e. by version or timestamp.
   */
  def resolveTimeTravelVersion(
      conf: SQLConf,
      deltaLog: DeltaLog,
      tt: DeltaTimeTravelSpec): (Long, String) = {
    if (tt.version.isDefined) {
      val userVersion = tt.version.get
      deltaLog.history.checkVersionExists(userVersion)
      userVersion -> "version"
    } else {
      val timestamp = tt.getTimestamp(conf.sessionLocalTimeZone)
      deltaLog.history.getActiveCommitAtTime(timestamp, false).version -> "timestamp"
    }
  }
}
