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

package org.apache.spark.sql.delta.catalog

import java.{util => ju}

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaColumnMapping, DeltaErrors, DeltaLog, DeltaOptions, DeltaTableIdentifier, DeltaTableUtils, DeltaTimeTravelSpec, GeneratedColumn, Snapshot}
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableCatalog, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
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
    options: Map[String, String] = Map.empty,
    cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table
  with SupportsWrite
  with V2TableWithV1Fallback
  with DeltaLogging {

  private lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString, options)
    }
  }


  // This MUST be initialized before the deltaLog object is created, in order to accurately
  // bound the creation time of the table.
  private val creationTimeMs = {
      System.currentTimeMillis()
  }

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = {
      DeltaLog.forTable(spark, rootPath, options)
  }

  def getTableIdentifierIfExists: Option[TableIdentifier] = tableIdentifier.map { tableName =>
    spark.sessionState.sqlParser.parseMultipartIdentifier(tableName).asTableIdentifier
  }

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
      // By default, block using CDF + time-travel
      if (CDCReader.isCDCRead(cdcOptions) &&
          !spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_TIME_TRAVEL_OPTIONS)) {
        throw DeltaErrors.timeTravelNotSupportedException
      }

      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, deltaLog, spec)
      val source = spec.creationSource.getOrElse("unknown")
      recordDeltaEvent(deltaLog, s"delta.timeTravel.$source", data = Map(
        // Log the cached version of the table on the cluster
        "tableVersion" -> deltaLog.unsafeVolatileSnapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      deltaLog.getSnapshotAt(version)
    }.getOrElse(
      deltaLog.update(
        stalenessAcceptable = true,
        checkIfUpdatedSinceTs = Some(creationTimeMs)
      )
    )
  }

  private lazy val tableSchema: StructType =
    DeltaColumnMapping.dropColumnMappingMetadata(
      DeltaTableUtils.removeInternalMetadata(spark, snapshot.schema))

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
    catalogTable.foreach { table =>
      if (table.owner != null && table.owner.nonEmpty) {
        base.put(TableCatalog.PROP_OWNER, table.owner)
      }
      v1Table.storage.properties.foreach { case (key, value) =>
        base.put(TableCatalog.OPTION_PREFIX + key, value)
      }
      if (v1Table.tableType == CatalogTableType.EXTERNAL) {
        base.put(TableCatalog.PROP_EXTERNAL, "true")
      }
    }
    Option(snapshot.metadata.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    base.asJava
  }

  override def capabilities(): ju.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, OVERWRITE_DYNAMIC
  ).asJava

  def tableExists: Boolean = deltaLog.tableExists


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoDeltaBuilder(
      deltaLog, info.options, spark.sessionState.conf.useNullsForMissingDefaultColumnValues)
  }

  /**
   * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
   * paths.
   */
  def toBaseRelation: BaseRelation = {
    // force update() if necessary in DataFrameReader.load code
    snapshot
    if (!tableExists) {
      // special error handling for path based tables
      if (catalogTable.isEmpty
        && !rootPath.getFileSystem(deltaLog.newDeltaHadoopConf()).exists(rootPath)) {
        throw QueryCompilationErrors.dataPathNotExistError(rootPath.toString)
      }

      val id = catalogTable.map(ct => DeltaTableIdentifier(table = Some(ct.identifier)))
        .getOrElse(DeltaTableIdentifier(path = Some(path.toString)))
      throw DeltaErrors.nonExistentDeltaTable(id)
    }
    val partitionPredicates = DeltaDataSource.verifyAndCreatePartitionFilters(
      path.toString, snapshot, partitionFilters)

    deltaLog.createRelation(
      partitionPredicates, Some(snapshot), timeTravelSpec.isDefined, cdcOptions)
  }

  /**
   * Check the passed in options and existing timeTravelOpt, set new time travel by options.
   */
  def withOptions(options: Map[String, String]): DeltaTableV2 = {
    val ttSpec = DeltaDataSource.getTimeTravelVersion(options)
    if (timeTravelOpt.nonEmpty && ttSpec.nonEmpty) {
      throw DeltaErrors.multipleTimeTravelSyntaxUsed
    }

    def checkCDCOptionsValidity(options: CaseInsensitiveStringMap): Unit = {
      // check if we have both version and timestamp parameters
      if (options.containsKey(DeltaDataSource.CDC_START_TIMESTAMP_KEY)
          && options.containsKey(DeltaDataSource.CDC_START_VERSION_KEY)) {
        throw DeltaErrors.multipleCDCBoundaryException("starting")
      }
      if (options.containsKey(DeltaDataSource.CDC_END_VERSION_KEY)
          && options.containsKey(DeltaDataSource.CDC_END_TIMESTAMP_KEY)) {
        throw DeltaErrors.multipleCDCBoundaryException("ending")
      }
      if (!options.containsKey(DeltaDataSource.CDC_START_VERSION_KEY)
          && !options.containsKey(DeltaDataSource.CDC_START_TIMESTAMP_KEY)) {
        throw DeltaErrors.noStartVersionForCDC()
      }
    }

    val caseInsensitiveStringMap = new CaseInsensitiveStringMap(options.asJava)

    if (timeTravelOpt.isEmpty && ttSpec.nonEmpty) {
      copy(timeTravelOpt = ttSpec)
    } else if (CDCReader.isCDCRead(caseInsensitiveStringMap)) {
      checkCDCOptionsValidity(caseInsensitiveStringMap)
      copy(cdcOptions = caseInsensitiveStringMap)
    } else {
      this
    }
  }

  override def v1Table: CatalogTable = {
    if (catalogTable.isEmpty) {
      throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
    }
    if (timeTravelSpec.isDefined) {
      catalogTable.get.copy(stats = None)
    } else {
      catalogTable.get
    }
  }
}

private class WriteIntoDeltaBuilder(
    log: DeltaLog,
    writeOptions: CaseInsensitiveStringMap,
    nullAsDefault: Boolean)
  extends WriteBuilder with SupportsOverwrite with SupportsTruncate with SupportsDynamicOverwrite {

  private var forceOverwrite = false

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteIntoDeltaBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw DeltaErrors.replaceWhereUsedInOverwrite()
    }
    options.put("replaceWhere", DeltaSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    options.put(
      DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION,
      DeltaOptions.PARTITION_OVERWRITE_MODE_DYNAMIC)
    forceOverwrite = true
    this
  }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          // Normal table insertion should be the only place that can use null as the default
          // column value. We put a special option here so that `TransactionalWrite#writeFiles`
          // will recognize it and apply null-as-default.
          if (nullAsDefault) {
            options.put(
              ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION,
              "true"
            )
          }
          // TODO: Get the config from WriteIntoDelta's txn.
          WriteIntoDelta(
            log,
            if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(options.toMap, session.sessionState.conf),
            Nil,
            log.unsafeVolatileSnapshot.metadata.configuration,
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
}
