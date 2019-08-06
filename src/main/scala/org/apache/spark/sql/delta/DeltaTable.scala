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
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.PartitionUtils
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
  def unapply(a: LogicalRelation): Option[TahoeLogFileIndex] = a match {
    case LogicalRelation(HadoopFsRelation(index: TahoeLogFileIndex, _, _, _, _, _), _, _, _) =>
      if (index.partitionFilters.isEmpty && index.versionToUse.isEmpty) {
        Some(index)
      } else if (index.versionToUse.nonEmpty) {
        throw new AnalysisException(
          s"Expect a full scan of the latest version of the Delta source, but found a historical " +
            s"scan of version ${index.versionToUse.get}")
      } else {
        throw new AnalysisException(
          s"Expect a full scan of Delta sources, but found the partial scan. path:${index.path}")
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

  /** Find the root of a Delta table from the provided path. */
  def findDeltaTableRoot(spark: SparkSession, path: Path): Option[Path] = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    var currentPath = path
    while (currentPath != null && currentPath.getName() != "_delta_log" &&
        currentPath.getName() != "_samples") {
      val deltaLogPath = new Path(currentPath, "_delta_log")
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent()
    }
    None
  }

  /** Find the root of a Delta table from the provided paths.
   * Maybe the paths contain several partition's path like:
   * `Seq("/root/path/fieldOne=1/fieldTwo=2","/root/path/fieldOne=3/fieldTwo=4"`,
   * and "/root/path" shoule be returned.
   */
  def findDeltaTableRoot(spark: SparkSession, paths: Seq[Path]): Path = {
    val rootPaths = paths.map { p =>
      DeltaTableUtils.findDeltaTableRoot(spark, p)
        .getOrElse {
          val fs = p.getFileSystem(spark.sessionState.newHadoopConf())
          if (!fs.exists(p)) {
            throw DeltaErrors.pathNotExistsException(p.getName)
          }
          p
        }
    }.distinct
    assert(rootPaths.length <= 1, s"There are more than one delta root paths" +
      s"${rootPaths.mkString(",")} for ${paths.mkString(",")}")
    rootPaths.head
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
    condition.find(_.isInstanceOf[SubqueryExpression]).isDefined
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
      path: String): Option[(String, DeltaTimeTravelSpec)] = {
    val conf = session.sessionState.conf
    if (!DeltaTimeTravelSpec.isApplicable(conf, path)) return None

    val maybePath = new Path(path)
    val fs = maybePath.getFileSystem(session.sessionState.newHadoopConf())

    // If the folder really exists, quit
    if (fs.exists(maybePath)) return None

    val (tt, realPath) = DeltaTimeTravelSpec.addTimeTravelNode(conf, path)
    Some(realPath -> tt)
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

  /** Extracts whether users provided the option to time travel a relation. */
  def getTimeTravelVersion(parameters: Map[String, String]): Option[DeltaTimeTravelSpec] = {
    val caseInsensitive = CaseInsensitiveMap[String](parameters)
    val tsOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY)
    val versionOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_VERSION_KEY)
    val sourceOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY)

    if (tsOpt.isDefined && versionOpt.isDefined) {
      throw DeltaErrors.provideOneOfInTimeTravel
    } else if (tsOpt.isDefined) {
      Some(DeltaTimeTravelSpec(Some(Literal(tsOpt.get)), None, sourceOpt.orElse(Some("dfReader"))))
    } else if (versionOpt.isDefined) {
      val version = Try(versionOpt.get.toLong) match {
        case Success(v) => v
        case Failure(t) => throw new IllegalArgumentException(
          s"${DeltaDataSource.TIME_TRAVEL_VERSION_KEY} needs to be a valid bigint value.", t)
      }
      Some(DeltaTimeTravelSpec(None, Some(version), sourceOpt.orElse(Some("dfReader"))))
    } else {
      None
    }
  }

  /** extract time travel:
   * First check whether users provided the option to time travel,
   * if not, then check if the given path contains time travel syntax with the `@`
   */
  def getTimeTravel(
      spark: SparkSession,
      maybePathWithTimeTravel: String,
      parameters: Map[String, String])
    : (String, Option[DeltaTimeTravelSpec]) = {
    // Handle time travel
    val maybeTimeTravel =
      DeltaTableUtils.extractIfPathContainsTimeTravel(spark, maybePathWithTimeTravel)
    val (realPath, timeTravelByPath) = maybeTimeTravel.map {
      case (p, tt) => p -> Some(tt)
    }.getOrElse(maybePathWithTimeTravel -> None)
    val timeTravelByParams = getTimeTravelVersion(parameters)

    if (timeTravelByParams.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }

    (realPath, timeTravelByParams.orElse(timeTravelByPath))
  }

  def resolvePathFilters(deltaLog: DeltaLog, partitionFragments: Seq[String]): Seq[Expression] = {
    val snapshot = deltaLog.update()
    val metadata = snapshot.metadata
    val partitionFilters = partitionFragments.map { fragment =>
      val partitions = try {
        PartitionUtils.parsePathFragmentAsSeq(fragment)
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          throw DeltaErrors.partitionPathParseException(fragment)
      }

      val badColumns = partitions.map(_._1).filterNot(metadata.partitionColumns.contains)
      if (badColumns.nonEmpty) {
        throw DeltaErrors.partitionPathInvolvesNonPartitionColumnException(badColumns, fragment)
      }

      partitions.map { case (key, value) =>
        EqualTo(UnresolvedAttribute(key), Literal(value))
      }.reduce(And)
    }

    import org.apache.spark.sql.delta.actions.SingleAction._
    val files = DeltaLog.filterFileList(
      metadata.partitionColumns, snapshot.allFiles.toDF(), partitionFilters).as[AddFile].collect()
    if (files.length == 0) {
      throw DeltaErrors.pathNotExistsException(partitionFragments.mkString(","))
    }
    partitionFilters
  }
}
