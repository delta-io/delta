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

package org.apache.spark.sql.delta.catalog

import java.{util => ju}

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaOptions, DeltaTableIdentifier, DeltaTableUtils, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.GeneratedColumn
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableCatalog, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The data source V2 representation of a Delta table that exists.
 *
 * @param path The path to the table
 * @param tableIdentifier The table identifier for this table
 */
case class DeltaTableV2(
    spark: SparkSession,
    path: Path,
    catalogTable: Option[CatalogTable] = None,
    tableIdentifier: Option[String] = None,
    timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
    options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table
  with SupportsWrite
  with V2TableWithV1Fallback
  with DeltaLogging {

  private lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString)
    }
  }

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = DeltaLog.forTable(spark, rootPath)

  def getTableIdentifierIfExists: Option[TableIdentifier] = tableIdentifier.map(
    spark.sessionState.sqlParser.parseTableIdentifier)

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .orElse(tableIdentifier)
    .getOrElse(s"delta.`${deltaLog.dataPath}`")

  private lazy val timeTravelSpec: Option[DeltaTimeTravelSpec] = {
    if (timeTravelOpt.isDefined && timeTravelByPath.isDefined) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    timeTravelOpt.orElse(timeTravelByPath)
  }

  lazy val snapshot: Snapshot = {
    timeTravelSpec.map { spec =>
      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, deltaLog, spec)
      val source = spec.creationSource.getOrElse("unknown")
      recordDeltaEvent(deltaLog, s"delta.timeTravel.$source", data = Map(
        "tableVersion" -> deltaLog.snapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      deltaLog.getSnapshotAt(version)
    }.getOrElse(deltaLog.update(stalenessAcceptable = true))
  }

  private lazy val tableSchema: StructType =
    GeneratedColumn.removeGenerationExpressions(snapshot.schema)

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    snapshot.metadata.partitionColumns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): ju.Map[String, String] = {
    val base = snapshot.getProperties
    base.put(TableCatalog.PROP_PROVIDER, "delta")
    base.put(TableCatalog.PROP_LOCATION, CatalogUtils.URIToString(path.toUri))
    Option(snapshot.metadata.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    // this reports whether the table is an external or managed catalog table as
    // the old DescribeTable command would
    catalogTable.foreach(table => base.put("Type", table.tableType.name))
    base.asJava
  }

  override def capabilities(): ju.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoDeltaBuilder(deltaLog, info.options)
  }

  /**
   * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
   * paths.
   */
  def toBaseRelation: BaseRelation = {
    if (!deltaLog.tableExists) {
      val id = catalogTable.map(ct => DeltaTableIdentifier(table = Some(ct.identifier)))
        .getOrElse(DeltaTableIdentifier(path = Some(path.toString)))
      throw DeltaErrors.notADeltaTableException(id)
    }
    val partitionPredicates = DeltaDataSource.verifyAndCreatePartitionFilters(
      path.toString, snapshot, partitionFilters)

    deltaLog.createRelation(
      partitionPredicates, Some(snapshot), timeTravelSpec.isDefined, options)
  }

  /**
   * Check the passed in options and existing timeTravelOpt, set new time travel by options.
   */
  def withOptions(options: Map[String, String]): DeltaTableV2 = {
    val ttSpec = DeltaDataSource.getTimeTravelVersion(options)
    if (timeTravelOpt.nonEmpty && ttSpec.nonEmpty) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }
    if (timeTravelOpt.isEmpty && ttSpec.nonEmpty) {
      copy(timeTravelOpt = ttSpec)
    } else {
      this
    }
  }

  override def v1Table: CatalogTable = {
    catalogTable.getOrElse {
      throw new IllegalStateException("v1Table call is not expected with path based DeltaTableV2")
    }
  }
}

private class WriteIntoDeltaBuilder(
    log: DeltaLog,
    writeOptions: CaseInsensitiveStringMap)
  extends WriteBuilder with V1WriteBuilder with SupportsOverwrite with SupportsTruncate {

  private var forceOverwrite = false

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteIntoDeltaBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw new AnalysisException(
        "You can't use replaceWhere in conjunction with an overwrite by filter")
    }
    options.put("replaceWhere", DeltaSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def buildForV1Write(): InsertableRelation = {
    new InsertableRelation {
      override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val session = data.sparkSession

        WriteIntoDelta(
          log,
          if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
          new DeltaOptions(options.toMap, session.sessionState.conf),
          Nil,
          log.snapshot.metadata.configuration,
          data).run(session)

        // TODO: Push this to Apache Spark
        // Re-cache all cached plans(including this relation itself, if it's cached) that refer
        // to this data source relation. This is the behavior for InsertInto
        session.sharedState.cacheManager.recacheByPlan(
          session, LogicalRelation(log.createRelation()))
      }
    }
  }
}
