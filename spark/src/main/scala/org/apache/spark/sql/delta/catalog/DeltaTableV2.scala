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

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedTable}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableImplicits._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableCatalog, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{LogicalRelation, LogicalRelationShims}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{Clock, SystemClock}

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
    options: Map[String, String] = Map.empty)
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


  def hasPartitionFilters: Boolean = partitionFilters.nonEmpty

  // This MUST be initialized before the deltaLog object is created, in order to accurately
  // bound the creation time of the table.
  private val creationTimeMs = {
      System.currentTimeMillis()
  }

  // The loading of the DeltaLog is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val deltaLog: DeltaLog = {
    DeltaTableV2.withEnrichedUnsupportedTableException(catalogTable, tableIdentifier) {
      // Ideally the table storage properties should always be the same as the options load from
      // the Delta log, as Delta CREATE TABLE command guarantees it. However, custom catalogs such
      // as Unity Catalog may add more table storage properties on the fly. We should respect it
      // and merge the table storage properties and Delta options.
      val dataSourceOptions = if (catalogTable.isDefined) {
        // To be safe, here we only extra file system options from table storage properties and
        // the original `options` has higher priority than the table storage properties.
        val fileSystemOptions = catalogTable.get.storage.properties.filter { case (k, _) =>
          DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
        }
        fileSystemOptions ++ options
      } else {
        options
      }
      DeltaLog.forTable(spark, rootPath, dataSourceOptions)
    }
  }

  /**
   * Updates the delta log for this table and returns a new snapshot
   */
  def update(): Snapshot = deltaLog.update(catalogTableOpt = catalogTable)

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

  private lazy val caseInsensitiveOptions = new CaseInsensitiveStringMap(options.asJava)

  /**
   * The snapshot initially associated with this table. It is captured on first access, usually (but
   * not always) shortly after the table was first created, and is immutable once captured.
   *
   * WARNING: This snapshot could be arbitrarily stale for long-lived [[DeltaTableV2]] instances,
   * such as the ones [[DeltaTable]] uses internally. Callers who cannot tolerate this potential
   * staleness should use [[getFreshSnapshot]] instead.
   *
   * WARNING: Because the snapshot is captured lazily, callers should explicitly access the snapshot
   * if they want to be certain it has been captured.
   */
  lazy val initialSnapshot: Snapshot = DeltaTableV2.withEnrichedUnsupportedTableException(
    catalogTable, tableIdentifier) {

    timeTravelSpec.map { spec =>
      // By default, block using CDF + time-travel
      if (CDCReader.isCDCRead(caseInsensitiveOptions) &&
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
      deltaLog.getSnapshotAt(version, catalogTableOpt = catalogTable)
    }.getOrElse(
      deltaLog.update(
        stalenessAcceptable = true,
        checkIfUpdatedSinceTs = Some(creationTimeMs),
        catalogTableOpt = catalogTable
      )
    )
  }

  // We get the cdcRelation ahead of time if this is a CDC read to be able to return the correct
  // schema. The schema for CDC reads are currently convoluted due to column mapping behavior
  private lazy val cdcRelation: Option[BaseRelation] = {
    if (CDCReader.isCDCRead(caseInsensitiveOptions)) {
      recordDeltaEvent(deltaLog, "delta.cdf.read",
        data = caseInsensitiveOptions.asCaseSensitiveMap())
      Some(CDCReader.getCDCRelation(
        spark, initialSnapshot, timeTravelSpec.nonEmpty, spark.sessionState.conf,
        caseInsensitiveOptions))
    } else {
      None
    }
  }

  private lazy val tableSchema: StructType = {
    val baseSchema = cdcRelation.map(_.schema).getOrElse {
      DeltaTableUtils.removeInternalWriterMetadata(spark, initialSnapshot.schema)
    }
    DeltaColumnMapping.dropColumnMappingMetadata(baseSchema)
  }

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    initialSnapshot.metadata.partitionColumns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): ju.Map[String, String] = {
    val base = initialSnapshot.getProperties
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
    // Don't use [[PROP_CLUSTERING_COLUMNS]] from CatalogTable because it may be stale.
    // Since ALTER TABLE updates it using an async post-commit hook.
    clusterBySpec.foreach { clusterBy =>
      ClusterBySpec.toProperties(clusterBy).foreach { case (key, value) =>
        base.put(key, value)
      }
    }
    Option(initialSnapshot.metadata.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    base.asJava
  }

  override def capabilities(): ju.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, OVERWRITE_DYNAMIC
  ).asJava

  def tableExists: Boolean = deltaLog.tableExists


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoDeltaBuilder(
      this, info.options, spark.sessionState.conf.useNullsForMissingDefaultColumnValues)
  }

  /**
   * Starts a transaction for this table, using the snapshot captured during table resolution.
   *
   * WARNING: Caller is responsible to ensure that table resolution was recent (e.g. if working with
   * [[DataFrame]] or [[DeltaTable]] API, where the table could have been resolved long ago).
   */
  def startTransactionWithInitialSnapshot(): OptimisticTransaction =
    startTransaction(Some(initialSnapshot))

  /**
   * Starts a transaction for this table, using Some provided snapshot, or a fresh snapshot if None
   * was provided.
   */
  def startTransaction(snapshotOpt: Option[Snapshot] = None): OptimisticTransaction = {
    deltaLog.startTransaction(catalogTable, snapshotOpt)
  }

  /**
   * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
   * paths.
   */
  lazy val toBaseRelation: BaseRelation = {
    // force update() if necessary in DataFrameReader.load code
    initialSnapshot
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
      path.toString, initialSnapshot, partitionFilters)

    cdcRelation.getOrElse {
      deltaLog.createRelation(
        partitionPredicates, Some(initialSnapshot), catalogTable, timeTravelSpec.isDefined)
    }
  }

  /** Creates a [[LogicalRelation]] that represents this table */
  lazy val toLogicalRelation: LogicalRelation = {
    val relation = this.toBaseRelation
    LogicalRelationShims.newInstance(
      relation, toAttributes(relation.schema), ttSafeCatalogTable, isStreaming = false)
  }

  /** Creates a [[DataFrame]] that uses the requested spark session to read from this table */
  def toDf(sparkSession: SparkSession): DataFrame = {
    val plan = catalogTable.foldLeft[LogicalPlan](toLogicalRelation) { (child, ct) =>
      // Catalog based tables need a SubqueryAlias that carries their fully-qualified name
      SubqueryAlias(ct.identifier.nameParts, child)
    }
    Dataset.ofRows(sparkSession, plan)
  }

  /** Creates a [[DataFrame]] that reads from this table */
  lazy val toDf: DataFrame = toDf(spark)

  /**
   * Check the passed in options and existing timeTravelOpt, set new time travel by options.
   */
  def withOptions(newOptions: Map[String, String]): DeltaTableV2 = {
    val ttSpec = DeltaDataSource.getTimeTravelVersion(newOptions)

    // Spark 4.0 and 3.5 handle time travel options differently.
    DeltaTimeTravelSpecShims.validateTimeTravelSpec(
      currSpecOpt = timeTravelOpt,
      newSpecOpt = ttSpec)

    val caseInsensitiveNewOptions = new CaseInsensitiveStringMap(newOptions.asJava)

    if (timeTravelOpt.isEmpty && ttSpec.nonEmpty) {
      copy(timeTravelOpt = ttSpec)
    } else if (CDCReader.isCDCRead(caseInsensitiveNewOptions)) {
      checkCDCOptionsValidity(caseInsensitiveNewOptions)
      // Do not use statistics during CDF reads
      this.copy(catalogTable = catalogTable.map(_.copy(stats = None)), options = newOptions)
    } else {
      this
    }
  }

  private def checkCDCOptionsValidity(options: CaseInsensitiveStringMap): Unit = {
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

  /** A "clean" version of the catalog table, safe for use with or without time travel. */
  lazy val ttSafeCatalogTable: Option[CatalogTable] = catalogTable match {
    case Some(ct) if timeTravelSpec.isDefined => Some(ct.copy(stats = None))
    case other => other
  }

  override def v1Table: CatalogTable = ttSafeCatalogTable.getOrElse {
    throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
  }

  lazy val clusterBySpec: Option[ClusterBySpec] = {
    // Always get the clustering columns from metadata domain in delta log.
    if (ClusteredTableUtils.isSupported(initialSnapshot.protocol)) {
      val clusteringColumns = ClusteringColumnInfo.extractLogicalNames(
        initialSnapshot)
      Some(ClusterBySpec.fromColumnNames(clusteringColumns))
    } else {
      None
    }
  }
}

object DeltaTableV2 {
  /** Resolves a path into a DeltaTableV2, leveraging standard v2 table resolution. */
  def apply(spark: SparkSession, tablePath: Path, options: Map[String, String], cmd: String)
      : DeltaTableV2 = {
    val unresolved = UnresolvedPathBasedDeltaTable(tablePath.toString, options, cmd)
    extractFrom((new DeltaAnalysis(spark))(unresolved), cmd)
  }

  /** Resolves a table identifier into a DeltaTableV2, leveraging standard v2 table resolution. */
  def apply(spark: SparkSession, tableId: TableIdentifier, cmd: String): DeltaTableV2 = {
    val unresolved = UnresolvedTable(tableId.nameParts, cmd)
    extractFrom(spark.sessionState.analyzer.ResolveRelations(unresolved), cmd)
  }

  /**
   * Extracts the DeltaTableV2 from a resolved Delta table plan node, throwing "table not found" if
   * the node does not actually represent a resolved Delta table.
   */
  def extractFrom(plan: LogicalPlan, cmd: String): DeltaTableV2 =
    maybeExtractFrom(plan).getOrElse(throw DeltaErrors.notADeltaTableException(cmd))

  /**
   * Extracts the DeltaTableV2 from a resolved Delta table plan node, returning None if the node
   * does not actually represent a resolved Delta table.
   */
  def maybeExtractFrom(plan: LogicalPlan): Option[DeltaTableV2] = plan match {
    case ResolvedTable(_, _, d: DeltaTableV2, _) => Some(d)
    case ResolvedTable(_, _, t: V1Table, _) if DeltaTableUtils.isDeltaTable(t.catalogTable) =>
      Some(DeltaTableV2(SparkSession.active, new Path(t.v1Table.location), Some(t.v1Table)))
    case _ => None
  }

  /**
   * When Delta Log throws InvalidProtocolVersionException it doesn't know the table name and uses
   * the data path in the message, this wrapper throw a new InvalidProtocolVersionException with
   * table name and sets its Cause to the original InvalidProtocolVersionException.
   */
  def withEnrichedUnsupportedTableException[T](
      catalogTable: Option[CatalogTable],
      tableName: Option[String] = None)(thunk: => T): T = {

    lazy val tableNameToUse = catalogTable match {
      case Some(ct) => Some(ct.identifier.copy(catalog = None).unquotedString)
      case None => tableName
    }

    try thunk catch {
      case e: InvalidProtocolVersionException if tableNameToUse.exists(_ != e.tableNameOrPath) =>
        throw e.copy(tableNameOrPath = tableNameToUse.get).initCause(e)
      case e: DeltaUnsupportedTableFeatureException if
          tableNameToUse.exists(_ != e.tableNameOrPath) =>
        throw e.copy(tableNameOrPath = tableNameToUse.get).initCause(e)
    }
  }
}

private class WriteIntoDeltaBuilder(
    table: DeltaTableV2,
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
            table.deltaLog,
            if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(options.toMap, session.sessionState.conf),
            Nil,
            table.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
            data,
            table.catalogTable).run(session)

          // TODO: Push this to Apache Spark
          // Re-cache all cached plans(including this relation itself, if it's cached) that refer
          // to this data source relation. This is the behavior for InsertInto
          session.sharedState.cacheManager.recacheByPlan(session, table.toLogicalRelation)
        }
      }
    }
  }
}
