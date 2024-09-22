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
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.{DeltaLogging, LogThrottler}
import org.apache.spark.sql.delta.skipping.clustering.temp.{ClusterByTransform => TempClusterByTransform}
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedLeafNode, UnresolvedTable}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableImplicits._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.planning.NodeWithOnlyDeterministicProjectAndFilter
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Extractor Object for pulling out the file index of a logical relation.
 */
object RelationFileIndex {
  def unapply(a: LogicalRelation): Option[FileIndex] = a match {
    case LogicalRelation(hrel: HadoopFsRelation, _, _, _) => Some(hrel.location)
    case _ => None
  }
}

/**
 * Extractor Object for pulling out the table scan of a Delta table. It could be a full scan
 * or a partial scan.
 */
object DeltaTable {
  def unapply(a: LogicalRelation): Option[TahoeFileIndex] = a match {
    case RelationFileIndex(fileIndex: TahoeFileIndex) => Some(fileIndex)
    case _ => None
  }
}

/**
 * Extractor Object for pulling out the full table scan of a Delta table.
 */
object DeltaFullTable {
  def unapply(a: LogicalPlan): Option[(LogicalRelation, TahoeLogFileIndex)] = a match {
    // `DeltaFullTable` is not only used to match a certain query pattern, but also does
    // some validations to throw errors. We need to match both Project and Filter here,
    // so that we can check if Filter is present or not during validations.
    case NodeWithOnlyDeterministicProjectAndFilter(lr @ DeltaTable(index: TahoeLogFileIndex)) =>
      if (!index.deltaLog.tableExists) return None
      val hasFilter = a.find(_.isInstanceOf[Filter]).isDefined
      if (index.partitionFilters.isEmpty && index.versionToUse.isEmpty && !hasFilter) {
        Some(lr -> index)
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
    findDeltaTableRoot(
      fs,
      path,
      throwOnError = SparkSession.active.conf.get(DeltaSQLConf.DELTA_IS_DELTA_TABLE_THROW_ON_ERROR))
  }

  /** Finds the root of a Delta table given a path if it exists. */
  private[delta] def findDeltaTableRoot(
      fs: FileSystem,
      path: Path,
      throwOnError: Boolean): Option[Path] = {
    if (throwOnError) {
      findDeltaTableRootThrowOnError(fs, path)
    } else {
      findDeltaTableRootNoExceptions(fs, path)
    }
  }

  /**
   * Finds the root of a Delta table given a path if it exists.
   *
   * Does not throw any exceptions, but returns `None` when uncertain (old behaviour).
   */
  private def findDeltaTableRootNoExceptions(fs: FileSystem, path: Path): Option[Path] = {
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

  /**
   * Finds the root of a Delta table given a path if it exists.
   *
   * If there are errors and no root could be found, throw the first error (new behaviour)
   */
  private def findDeltaTableRootThrowOnError(fs: FileSystem, path: Path): Option[Path] = {
    var firstError: Option[Throwable] = None
    // Return `None` if `firstError` is empty, throw `firstError` otherwise.
    def noneOrError(): Option[Path] = {
      firstError match {
        case Some(ex) =>
          throw ex
        case None =>
          None
      }
    }
    var currentPath = path
    while (currentPath != null && currentPath.getName != "_delta_log" &&
        currentPath.getName != "_samples") {
      val deltaLogPath = safeConcatPaths(currentPath, "_delta_log")
      try {
        if (fs.exists(deltaLogPath)) {
          return Option(currentPath)
        }
      } catch {
        case NonFatal(ex) if currentPath == path =>
          // Store errors for the first path, but keep going up the hierarchy,
          // in case the error at this level does not matter and the delta log is found at a parent.
          firstError = Some(ex)
        case NonFatal(ex) =>
          // If we find errors higher up the path we either treat it as a non-Delta table or
          // return the error we found at the original path, if any.
          // This gives us best-effort detection of delta logs in the hierarchy, but with more
          // useful error messages when access was actually missing.
          logThrottler.throttledWithSkippedLogMessage { skippedStr =>
            logWarning(log"Access error while exploring path hierarchy for a delta log."
                + log"original path=${MDC(DeltaLogKeys.PATH, path)}, "
                + log"path with error=${MDC(DeltaLogKeys.PATH2, currentPath)}."
                + skippedStr,
              ex)
          }
          return noneOrError()
      }
      currentPath = currentPath.getParent
    }
    noneOrError()
  }

  private val logThrottler = new LogThrottler()

  /** Whether a path should be hidden for delta-related file operations, such as Vacuum and Fsck. */
  def isHiddenDirectory(
      partitionColumnNames: Seq[String],
      pathName: String,
      shouldIcebergMetadataDirBeHidden: Boolean = true): Boolean = {
    // Names of the form partitionCol=[value] are partition directories, and should be
    // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    // indexes and these must be GCed when the data they are tied to is GCed.
    // metadata name is reserved for converted iceberg metadata with delta universal format
    (shouldIcebergMetadataDirBeHidden && pathName.equals("metadata")) ||
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
   * Many Delta meta-queries involve nondeterminstic functions, which interfere with automatic
   * column pruning, so columns can be manually pruned from the scan. Note that partition columns
   * can never be dropped even if they're not referenced in the rest of the query.
   *
   * @param spark the spark session to use
   * @param target the logical plan in which drop columns
   * @param columnsToDrop columns to drop from the scan
   */
  def dropColumns(
      spark: SparkSession,
      target: LogicalPlan,
      columnsToDrop: Seq[String]): LogicalPlan = {
    val resolver = spark.sessionState.analyzer.resolver

    // Spark does char type read-side padding via an additional Project over the scan node.
    // When char type read-side padding is applied, we need to apply column pruning for the
    // Project as well, otherwise the Project will contain missing attributes.
    val hasChar = target.exists {
      case Project(projectList, _) =>
        def hasCharPadding(e: Expression): Boolean = e.exists {
          case s: StaticInvoke => s.staticObject == classOf[CharVarcharCodegenUtils] &&
            s.functionName == "readSidePadding"
          case _ => false
        }
        projectList.exists {
          case a: Alias => hasCharPadding(a.child) && a.references.size == 1
          case _ => false
        }
      case _ => false
    }

    target transformUp {
      case l@LogicalRelation(hfsr: HadoopFsRelation, _, _, _) =>
        // Prune columns from the scan.
        val prunedOutput = l.output.filterNot { col =>
          columnsToDrop.exists(resolver(_, col.name))
        }

        val prunedSchema = StructType(prunedOutput.map(attr =>
          StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)))
        val newBaseRelation = hfsr.copy(dataSchema = prunedSchema)(hfsr.sparkSession)
        l.copy(relation = newBaseRelation, output = prunedOutput)

      case p @ Project(projectList, child) if hasChar =>
        val newProjectList = projectList.filter { e =>
          e.references.subsetOf(child.outputSet)
        }
        p.copy(projectList = newProjectList)
    }
  }

  /** Finds and returns the file source metadata column from a dataframe */
  def getFileMetadataColumn(df: DataFrame): Column =
    df.metadataColumn(FileFormat.METADATA_NAME)

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

  def parseColsToClusterByTransform(cols: Seq[String]): TempClusterByTransform = {
    TempClusterByTransform(cols.map(FieldReference(_)))
  }

  // Workaround for withActive not being visible in io/delta.
  def withActiveSession[T](spark: SparkSession)(body: => T): T = spark.withActive(body)

  /**
   * Uses org.apache.hadoop.fs.Path.mergePaths to concatenate a base path and a relative child path.
   *
   * This method is designed to address two specific issues in Hadoop Path:
   *
   * Issue 1:
   * When the base path represents a Uri with an empty path component, such as concatenating
   * "s3://my-bucket" and "childPath". In this case, the child path is converted to an absolute
   * path at the root, i.e. /childPath. This prevents a "URISyntaxException: Relative path in
   * absolute URI", which would be thrown by org.apache.hadoop.fs.Path(Path, String) because it
   * tries to convert the base path to a Uri and then resolve the child on top of it. This is
   * invalid for an empty base path and a relative child path according to the Uri specification,
   * which states that if an authority is defined, the path component needs to be either empty or
   * start with a '/'.
   *
   * Issue 2 (only when [[DeltaSQLConf.DELTA_WORK_AROUND_COLONS_IN_HADOOP_PATHS]] is `true`):
   * When the child path contains a special character ':', such as "aaaa:bbbb.csv".
   * This is valid in many file systems such as S3, but is actually ambiguous because it can be
   * parsed either as an absolute path with a scheme ("aaaa") and authority ("bbbb.csv"), or as
   * a relative path with a colon in the name ("aaaa:bbbb.csv"). Hadoop Path will always interpret
   * it as the former, which is not what we want in this case. Therefore, we prepend a '/' to the
   * child path to ensure that it is always interpreted as a relative path.
   * See [[https://issues.apache.org/jira/browse/HDFS-14762]] for more details.
   */
  def safeConcatPaths(basePath: Path, relativeChildPath: String): Path = {
    val useWorkaround = SparkSession.getActiveSession.map(_.sessionState.conf)
      .exists(_.getConf(DeltaSQLConf.DELTA_WORK_AROUND_COLONS_IN_HADOOP_PATHS))
    if (useWorkaround) {
      Path.mergePaths(basePath, new Path(s"/$relativeChildPath"))
    } else {
      if (basePath.toUri.getPath.isEmpty) {
        new Path(basePath, s"/$relativeChildPath")
      } else {
        new Path(basePath, relativeChildPath)
      }
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

sealed abstract class UnresolvedPathBasedDeltaTableBase(path: String) extends UnresolvedLeafNode {
  def identifier: Identifier = Identifier.of(Array(DeltaSourceUtils.ALT_NAME), path)
  def deltaTableIdentifier: DeltaTableIdentifier = DeltaTableIdentifier(Some(path), None)

}

/** Resolves to a [[ResolvedTable]] if the DeltaTable exists */
case class UnresolvedPathBasedDeltaTable(
    path: String,
    options: Map[String, String],
    commandName: String) extends UnresolvedPathBasedDeltaTableBase(path)

/** Resolves to a [[DataSourceV2Relation]] if the DeltaTable exists */
case class UnresolvedPathBasedDeltaTableRelation(
    path: String,
    options: CaseInsensitiveStringMap) extends UnresolvedPathBasedDeltaTableBase(path)

/**
 * This operator represents path-based tables in general including both Delta or non-Delta tables.
 * It resolves to a [[ResolvedTable]] if the path is for delta table,
 * [[ResolvedPathBasedNonDeltaTable]] if the path is for a non-Delta table.
 */
case class UnresolvedPathBasedTable(
    path: String,
    options: Map[String, String],
    commandName: String) extends LeafNode {
  override lazy val resolved: Boolean = false
  override val output: Seq[Attribute] = Nil
}

/**
 * This operator is a placeholder that identifies a non-Delta path-based table. Given the fact
 * that some Delta commands (e.g. DescribeDeltaDetail) support non-Delta table, we introduced
 * ResolvedPathBasedNonDeltaTable as the resolved placeholder after analysis on a non delta path
 * from UnresolvedPathBasedTable.
 */
case class ResolvedPathBasedNonDeltaTable(
    path: String,
    options: Map[String, String],
    commandName: String) extends LeafNode {
  override val output: Seq[Attribute] = Nil
}

/**
 * A helper object with an apply method to transform a path or table identifier to a LogicalPlan.
 * If the path is set, it will be resolved to an [[UnresolvedPathBasedDeltaTable]] whereas if the
 * tableIdentifier is set, the LogicalPlan will be an [[UnresolvedTable]]. If neither of the two
 * options or both of them are set, [[apply]] will throw an exception.
 */
object UnresolvedDeltaPathOrIdentifier {
  def apply(
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      cmd: String): LogicalPlan = {
    (path, tableIdentifier) match {
      case (Some(p), None) => UnresolvedPathBasedDeltaTable(p, Map.empty, cmd)
      case (None, Some(t)) => UnresolvedTable(t.nameParts, cmd)
      case _ => throw new IllegalArgumentException(
        s"Exactly one of path or tableIdentifier must be provided to $cmd")
    }
  }
}

/**
 * A helper object with an apply method to transform a path or table identifier to a LogicalPlan.
 * This is required by Delta commands that can also run against non-Delta tables, e.g. DESC DETAIL,
 * VACUUM command. If the tableIdentifier is set, the LogicalPlan will be an [[UnresolvedTable]].
 * If the tableIdentifier is not set but the path is set, it will be resolved to an
 * [[UnresolvedPathBasedTable]] since we can not tell if the path is for delta table or non delta
 * table at this stage. If neither of the two are set, throws an exception.
 */
object UnresolvedPathOrIdentifier {
  def apply(
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      cmd: String): LogicalPlan = {
    (path, tableIdentifier) match {
      case (_, Some(t)) => UnresolvedTable(t.nameParts, cmd)
      case (Some(p), None) => UnresolvedPathBasedTable(p, Map.empty, cmd)
      case _ => throw new IllegalArgumentException(
        s"At least one of path or tableIdentifier must be provided to $cmd")
    }
  }
}
