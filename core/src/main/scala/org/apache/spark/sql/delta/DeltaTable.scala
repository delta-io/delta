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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

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
    // `DeltaFullTable` is not only used to match a certain query pattern, but also does
    // some validations to throw errors. We need to match both Project and Filter here,
    // so that we can check if Filter is present or not during validations.
    case NodeWithOnlyDeterministicProjectAndFilter(DeltaTable(index: TahoeLogFileIndex)) =>
      if (!index.deltaLog.tableExists) return None
      val hasFilter = a.find(_.isInstanceOf[Filter]).isDefined
      if (index.partitionFilters.isEmpty && index.versionToUse.isEmpty && !hasFilter) {
        Some(index)
      } else if (index.versionToUse.nonEmpty) {
        throw DeltaErrors.failedScanWithHistoricalVersion(index.versionToUse.get)
      } else {
        throw DeltaErrors.unexpectedPartialScan(index.path)
      }
    // Convert V2 relations to V1 and perform the check
    case DeltaRelation(lr) => unapply(lr)
    case _ =>
      None
  }
}

// TODO: remove this after Spark 3.4 is released.
object NodeWithOnlyDeterministicProjectAndFilter {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case Project(projectList, child) if projectList.forall(_.deterministic) => unapply(child)
    case Filter(cond, child) if cond.deterministic => unapply(child)
    case _ => Some(plan)
  }
}

object DeltaTableUtils extends PredicateHelper
  with DeltaLogging {

  // The valid hadoop prefixes passed through `DeltaTable.forPath` or DataFrame APIs.
  val validDeltaTableHadoopPrefixes: List[String] = List("fs.", "dfs.")

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(table: CatalogTable): Boolean = DeltaSourceUtils.isDeltaTable(table.provider)


  /**
   * Check whether the provided table name is a Delta table based on information from the Catalog.
   */
  def isDeltaTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTempView(tableName)
    val tableExists = {
        (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
        catalog.tableExists(tableName)
    }
    tableIsNotTemporaryTable && tableExists && isDeltaTable(catalog.getTableMetadata(tableName))
  }

  /** Check if the provided path is the root or the children of a Delta table. */
  def isDeltaTable(
      spark: SparkSession,
      path: Path,
      options: Map[String, String] = Map.empty): Boolean = {
    findDeltaTableRoot(spark, path, options).isDefined
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
    def databaseExists = {
          ident.database.forall(catalog.databaseExists)
    }

    Try(databaseExists) match {
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
    // scalastyle:off deltahadoopconfiguration
    val fs = path.getFileSystem(spark.sessionState.newHadoopConfWithOptions(options))
    // scalastyle:on deltahadoopconfiguration


    findDeltaTableRoot(fs, path)
  }

  /** Finds the root of a Delta table given a path if it exists. */
  def findDeltaTableRoot(fs: FileSystem, path: Path): Option[Path] = {
    var currentPath = path
    while (currentPath != null && currentPath.getName != "_delta_log" &&
        currentPath.getName != "_samples") {
      val deltaLogPath = safeConcatPaths(currentPath, "_delta_log")
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
      !pathName.startsWith("_delta_index") && !pathName.startsWith("_change_data") &&
      !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
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
    val (metadataPredicates, dataPredicates) =
      splitConjunctivePredicates(condition).partition(
        isPredicateMetadataOnly(_, partitionColumns, spark))
    // Extra metadata predicates that can partially extracted from `dataPredicates`.
    val extraMetadataPredicates =
      if (dataPredicates.nonEmpty) {
        extractMetadataPredicates(dataPredicates.reduce(And), partitionColumns, spark)
          .map(splitConjunctivePredicates)
          .getOrElse(Seq.empty)
      } else {
        Seq.empty
      }
    (metadataPredicates ++ extraMetadataPredicates, dataPredicates)
  }

  /**
   * Returns a predicate that its reference is a subset of `partitionColumns` and it contains the
   * maximum constraints from `condition`.
   * When there is no such filter, `None` is returned.
   */
  private def extractMetadataPredicates(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Option[Expression] = {
    condition match {
      case And(left, right) =>
        val lhs = extractMetadataPredicates(left, partitionColumns, spark)
        val rhs = extractMetadataPredicates(right, partitionColumns, spark)
        (lhs.toSeq ++ rhs.toSeq).reduceOption(And)

    // The Or predicate is convertible when both of its children can be pushed down.
    // That is to say, if one/both of the children can be partially pushed down, the Or
    // predicate can be partially pushed down as well.
    //
    // Here is an example used to explain the reason.
    // Let's say we have
    // condition: (a1 AND a2) OR (b1 AND b2),
    // outputSet: AttributeSet(a1, b1)
    // a1 and b1 is convertible, while a2 and b2 is not.
    // The predicate can be converted as
    // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
    // As per the logical in And predicate, we can push down (a1 OR b1).
    case Or(left, right) =>
      for {
        lhs <- extractMetadataPredicates(left, partitionColumns, spark)
        rhs <- extractMetadataPredicates(right, partitionColumns, spark)
      } yield Or(lhs, rhs)

    // Here we assume all the `Not` operators is already below all the `And` and `Or` operators
    // after the optimization rule `BooleanSimplification`, so that we don't need to handle the
    // `Not` operators here.
    case other =>
      if (isPredicatePartitionColumnsOnly(other, partitionColumns, spark)) {
        Some(other)
      } else {
        None
      }
    }
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
   * Update FileFormat for a plan and return the updated plan
   *
   * @param target Target plan to update
   * @param updatedFileFormat Updated file format
   * @return Updated logical plan
   */
  def replaceFileFormat(
      target: LogicalPlan,
      updatedFileFormat: FileFormat): LogicalPlan = {
    target transform {
      case l @ LogicalRelation(hfsr: HadoopFsRelation, _, _, _) =>
        l.copy(
          relation = hfsr.copy(fileFormat = updatedFileFormat)(hfsr.sparkSession))
    }
  }

  /**
   * Check if the given path contains time travel syntax with the `@`. If the path genuinely exists,
   * return `None`. If the path doesn't exist, but is specifying time travel, return the
   * `DeltaTimeTravelSpec` as well as the real path.
   */
  def extractIfPathContainsTimeTravel(
      session: SparkSession,
      path: String,
      options: Map[String, String]): (String, Option[DeltaTimeTravelSpec]) = {
    val conf = session.sessionState.conf
    if (!DeltaTimeTravelSpec.isApplicable(conf, path)) return path -> None

    val maybePath = new Path(path)

    // scalastyle:off deltahadoopconfiguration
    val fs = maybePath.getFileSystem(session.sessionState.newHadoopConfWithOptions(options))
    // scalastyle:on deltahadoopconfiguration

    // If the folder really exists, quit
    if (fs.exists(maybePath)) return path -> None

    val (tt, realPath) = DeltaTimeTravelSpec.resolvePath(conf, path)
    realPath -> Some(tt)
  }

  /**
   * Given a time travel node, resolve which version it is corresponding to for the given table and
   * return the resolved version as well as the access type, i.e. by `version` or `timestamp`.
   */
  def resolveTimeTravelVersion(
      conf: SQLConf,
      deltaLog: DeltaLog,
      tt: DeltaTimeTravelSpec,
      canReturnLastCommit: Boolean = false): (Long, String) = {
    if (tt.version.isDefined) {
      val userVersion = tt.version.get
      deltaLog.history.checkVersionExists(userVersion)
      userVersion -> "version"
    } else {
      val timestamp = tt.getTimestamp(conf)
      deltaLog.history.getActiveCommitAtTime(timestamp, canReturnLastCommit).version -> "timestamp"
    }
  }

  def parseColToTransform(col: String): IdentityTransform = {
    IdentityTransform(FieldReference(Seq(col)))
  }

  /**
   * Uses org.apache.hadoop.fs.Path(Path, String) to concatenate a base path
   * and a relative child path and safely handles the case where the base path represents
   * a Uri with an empty path component (e.g. s3://my-bucket, where my-bucket would be
   * interpreted as the Uri authority).
   *
   * In that case, the child path is converted to an absolute path at the root, i.e. /childPath.
   * This prevents a "URISyntaxException: Relative path in absolute URI", which would be thrown
   * by org.apache.hadoop.fs.Path(Path, String) because it tries to convert the base path to a Uri
   * and then resolve the child on top of it. This is invalid for an empty base path and a
   * relative child path according to the Uri specification, which states that if an authority
   * is defined, the path component needs to be either empty or start with a '/'.
   */
  def safeConcatPaths(basePath: Path, relativeChildPath: String): Path = {
    if (basePath.toUri.getPath.isEmpty) {
      new Path(basePath, s"/$relativeChildPath")
    } else {
      new Path(basePath, relativeChildPath)
    }
  }

  /**
   * A list of Spark internal metadata keys that we may save in a Delta table schema
   * unintentionally due to SPARK-43123. We need to remove them before handing over the schema to
   * Spark to avoid Spark interpreting table columns incorrectly.
   *
   * Hard-coded strings are used intentionally as we want to capture possible keys used before
   * SPARK-43123 regardless Spark versions. For example, if Spark changes any key string in future
   * after SPARK-43123, the new string won't be leaked, but we still want to clean up the old key.
   */
  val SPARK_INTERNAL_METADATA_KEYS = Seq(
    "__autoGeneratedAlias",
    "__metadata_col",
    "__supports_qualified_star", // A key used by an old version. Doesn't exist in latest code
    "__qualified_access_only",
    "__file_source_metadata_col",
    "__file_source_constant_metadata_col",
    "__file_source_generated_metadata_col"
  )

  /**
   * Remove leaked metadata keys from the persisted table schema. Old versions might leak metadata
   * intentionally. This method removes all possible metadata keys to avoid Spark interpreting
   * table columns incorrectly.
   */
  def removeInternalMetadata(spark: SparkSession, persistedSchema: StructType): StructType = {
    val schema = ColumnWithDefaultExprUtils.removeDefaultExpressions(persistedSchema)
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_REMOVE_SPARK_INTERNAL_METADATA)) {
      var updated = false
      val updatedSchema = schema.map { field =>
        if (SPARK_INTERNAL_METADATA_KEYS.exists(field.metadata.contains)) {
          updated = true
          val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
          SPARK_INTERNAL_METADATA_KEYS.foreach(newMetadata.remove)
          field.copy(metadata = newMetadata.build())
        } else {
          field
        }
      }
      if (updated) {
        StructType(updatedSchema)
      } else {
        schema
      }
    } else {
      schema
    }
  }
}
