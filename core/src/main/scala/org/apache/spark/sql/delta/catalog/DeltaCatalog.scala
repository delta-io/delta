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

// scalastyle:off import.ordering.noEmptyLine
import java.sql.Timestamp
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors, DeltaTableUtils}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.DeltaTableIdentifier.gluePermissionError
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils, DeltaSQLConf}
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, UnresolvedAttribute, UnresolvedFieldName, UnresolvedFieldPosition}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, QualifiedColType}
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, StagedTable, StagingTableCatalog, SupportsWrite, Table, TableCapability, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Literal, NamedReference, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/**
 * A Catalog extension which can properly handle the interaction between the HiveMetaStore and
 * Delta tables. It delegates all operations DataSources other than Delta to the SparkCatalog.
 */
class DeltaCatalog extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SupportsPathIdentifier
  with DeltaLogging {


  val spark = SparkSession.active

  /**
   * Creates a Delta table
   *
   * @param ident The identifier of the table
   * @param schema The schema of the table
   * @param partitions The partition transforms for the table
   * @param allTableProperties The table properties that configure the behavior of the table or
   *                           provide information about the table
   * @param writeOptions Options specific to the write during table creation or replacement
   * @param sourceQuery A query if this CREATE request came from a CTAS or RTAS
   * @param operation The specific table creation mode, whether this is a Create/Replace/Create or
   *                  Replace
   */
  private def createDeltaTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String],
      writeOptions: Map[String, String],
      sourceQuery: Option[DataFrame],
      operation: TableCreationModes.CreationMode
    ): Table = recordFrameProfile(
        "DeltaCatalog", "createDeltaTable") {
    // These two keys are tableProperties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    val tableProperties = allTableProperties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case TableCatalog.PROP_EXTERNAL => false
      case "path" => false
      case _ => true
    }.toMap
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    var newSchema = schema
    var newPartitionColumns = partitionColumns
    var newBucketSpec = maybeBucketSpec
    val conf = spark.sessionState.conf

    val isByPath = isPathIdentifier(ident)
    if (isByPath && !conf.getConf(DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS)
      && allTableProperties.containsKey("location")
      // The location property can be qualified and different from the path in the identifier, so
      // we check `endsWith` here.
      && Option(allTableProperties.get("location")).exists(!_.endsWith(ident.name()))
    ) {
      throw DeltaErrors.ambiguousPathsInCreateTableException(
        ident.name(), allTableProperties.get("location"))
    }
    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    val id = {
      TableIdentifier(ident.name(), ident.namespace().lastOption)
    }
    var locUriOpt = location.map(CatalogUtils.stringToURI)
    val existingTableOpt = getExistingTableIfExists(id)
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(spark.sessionState.catalog.defaultTablePath(id))
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val commentOpt = Option(allTableProperties.get("comment"))


    var tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tableProperties,
      comment = commentOpt
    )

    val withDb = verifyTableAndSolidify(tableDesc, None)

    val writer = sourceQuery.map { df =>
      WriteIntoDelta(
        DeltaLog.forTable(spark, new Path(loc)),
        operation.mode,
        new DeltaOptions(withDb.storage.properties, spark.sessionState.conf),
        withDb.partitionColumnNames,
        withDb.properties ++ commentOpt.map("comment" -> _),
        df,
        schemaInCatalog = if (newSchema != schema) Some(newSchema) else None)
    }

    CreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      tableByPath = isByPath).run(spark)

    loadTable(ident)
  }

  override def loadTable(ident: Identifier): Table = recordFrameProfile(
      "DeltaCatalog", "loadTable") {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if DeltaTableUtils.isDeltaTable(v1.catalogTable) =>
          DeltaTableV2(
            spark,
            new Path(v1.catalogTable.location),
            catalogTable = Some(v1.catalogTable),
            tableIdentifier = Some(ident.toString))
        case o => o
      }
    } catch {
      case e @ (
        _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException) =>
          if (isPathIdentifier(ident)) {
            newDeltaPathTable(ident)
          } else if (isIcebergPathIdentifier(ident)) {
            IcebergTablePlaceHolder(TableIdentifier(ident.name(), Some("iceberg")))
          } else {
            throw e
          }
      case e: AnalysisException if gluePermissionError(e) && isPathIdentifier(ident) =>
        logWarning("Received an access denied error from Glue. Assuming this " +
          s"identifier ($ident) is path based.", e)
        newDeltaPathTable(ident)
    }
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    loadTableWithTimeTravel(ident, version = None, Some(timestamp))
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    loadTableWithTimeTravel(ident, Some(version), timestamp = None)
  }

  /**
   * Helper method which loads a Delta table with given time travel parameters.
   * Exactly one of the timetravel parameters (version or timestamp) must be present.
   *
   * @param version The table version to load
   * @param timestamp The timestamp for the table to load, in microseconds
   */
  private def loadTableWithTimeTravel(
      ident: Identifier,
      version: Option[String],
      timestamp: Option[Long]): Table = {
    assert(version.isEmpty ^ timestamp.isEmpty,
      "Either the version or timestamp should be provided for time travel")
    val table = loadTable(ident)
    table match {
      case deltaTable: DeltaTableV2 =>
        val ttOpts = Map(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY -> "SQL") ++
          (if (version.isDefined) {
            Map(DeltaDataSource.TIME_TRAVEL_VERSION_KEY -> version.get)
          } else {
            val timestampMs = timestamp.get / 1000
            Map(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY -> new Timestamp(timestampMs).toString)
          })

        deltaTable.withOptions(ttOpts)
      // punt this problem up to the parent
      case _ if version.isDefined => super.loadTable(ident, version.get)
      case _ if timestamp.isDefined => super.loadTable(ident, timestamp.get)
    }
  }


  protected def newDeltaPathTable(ident: Identifier): DeltaTableV2 = {
    DeltaTableV2(spark, new Path(ident.name()))
  }

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider"))
      .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
  }

  private def createCatalogTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
      super.createTable(ident, schema, partitions, properties)
  }


  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]) : Table =
    recordFrameProfile("DeltaCatalog", "createTable") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        createDeltaTable(
          ident,
          schema,
          partitions,
          properties,
          Map.empty,
          sourceQuery = None,
          TableCreationModes.Create
        )
      } else {
        createCatalogTable(ident, schema, partitions, properties
        )
      }
    }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageReplace") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.Replace
        )
      } else {
        super.dropTable(ident)
        val table = createCatalogTable(ident, schema, partitions, properties
        )
        BestEffortStagedTable(ident, table, this)
      }
    }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreateOrReplace") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.CreateOrReplace
        )
      } else {
        try super.dropTable(ident)
        catch {
          case _: NoSuchDatabaseException => // this is fine
          case _: NoSuchTableException => // this is fine
        }
        val table = createCatalogTable(ident, schema, partitions, properties
        )
        BestEffortStagedTable(ident, table, this)
      }
    }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreate") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.Create
        )
      } else {
        val table = createCatalogTable(ident, schema, partitions, properties
        )
        BestEffortStagedTable(ident, table, this)
      }
    }

  // Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, bucketCols, sortCols) =>
        bucketSpec = Some(BucketSpec(
          numBuckets, bucketCols.map(_.fieldNames.head), sortCols.map(_.fieldNames.head)))

      case transform =>
        throw DeltaErrors.operationNotSupportedException(s"Partitioning by expressions")
    }

    (identityCols.toSeq, bucketSpec)
  }

  /** Performs checks on the parameters provided for table creation for a Delta table. */
  def verifyTableAndSolidify(
      tableDesc: CatalogTable,
      query: Option[LogicalPlan]): CatalogTable = {

    if (tableDesc.bucketSpec.isDefined) {
      throw DeltaErrors.operationNotSupportedException("Bucketing", tableDesc.identifier)
    }

    val schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // Delta is case insensitive

    val validatedConfigurations = DeltaConfigs.validateConfigurations(tableDesc.properties)

    val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = validatedConfigurations)
  }

  /** Checks if a table already exists for the provided identifier. */
  def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathIdentifier(table)) return None
    val tableExists = catalog.tableExists(table)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw DeltaErrors.cannotWriteIntoView(table)
      }
      if (!DeltaSourceUtils.isDeltaTable(oldTable.provider)) {
        throw DeltaErrors.notADeltaTable(table.table)
      }
      Some(oldTable)
    } else {
      None
    }
  }

  /**
   * A staged delta table, which creates a HiveMetaStore entry and appends data if this was a
   * CTAS/RTAS command. We have a ugly way of using this API right now, but it's the best way to
   * maintain old behavior compatibility between Databricks Runtime and OSS Delta Lake.
   */
  private class StagedDeltaTableV2(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode
    ) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = Map.empty

    override def partitioning(): Array[Transform] = partitions

    override def commitStagedChanges(): Unit = recordFrameProfile(
        "DeltaCatalog", "commitStagedChanges") {
      val conf = spark.sessionState.conf
      val props = new util.HashMap[String, String]()
      // Options passed in through the SQL API will show up both with an "option." prefix and
      // without in Spark 3.1, so we need to remove those from the properties
      val optionsThroughProperties = properties.asScala.collect {
        case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
      }.toSet
      val sqlWriteOptions = new util.HashMap[String, String]()
      properties.asScala.foreach { case (k, v) =>
        if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
          // Do not add to properties
          props.put(k, v)
        } else if (optionsThroughProperties.contains(k)) {
          sqlWriteOptions.put(k, v)
        }
      }
      if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
        writeOptions = sqlWriteOptions.asScala.toMap
      }
      if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
        // Legacy behavior
        writeOptions.foreach { case (k, v) => props.put(k, v) }
      } else {
        writeOptions.foreach { case (k, v) =>
          // Continue putting in Delta prefixed options to avoid breaking workloads
          if (k.toLowerCase(Locale.ROOT).startsWith("delta.")) {
            props.put(k, v)
          }
        }
      }
      createDeltaTable(
        ident,
        schema,
        partitions,
        props,
        writeOptions,
        asSelectQuery,
        operation
      )
    }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      writeOptions = info.options.asCaseSensitiveMap().asScala.toMap
      new DeltaV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a Delta table.
     */
    private class DeltaV1WriteBuilder extends WriteBuilder {
      override def build(): V1Write = new V1Write {
        override def toInsertableRelation(): InsertableRelation = {
          new InsertableRelation {
            override def insert(data: DataFrame, overwrite: Boolean): Unit = {
              asSelectQuery = Option(data)
            }
          }
        }
      }
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = recordFrameProfile(
      "DeltaCatalog", "alterTable") {
    // We group the table changes by their type, since Delta applies each in a separate action.
    // We also must define an artificial type for SetLocation, since data source V2 considers
    // location just another property but it's special in catalog tables.
    class SetLocation {}
    val grouped = changes.groupBy {
      case s: SetProperty if s.property() == "location" => classOf[SetLocation]
      case c => c.getClass
    }
    val table = loadTable(ident) match {
      case deltaTable: DeltaTableV2 => deltaTable
      case _ => return super.alterTable(ident, changes: _*)
    }

    // Whether this is an ALTER TABLE ALTER COLUMN SYNC IDENTITY command.
    var syncIdentity = false
    val columnUpdates = new mutable.HashMap[Seq[String], (StructField, Option[ColumnPosition])]()

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        val tableToUpdate = table
        AlterTableAddColumnsDeltaCommand(
          tableToUpdate,
          newColumns.asInstanceOf[Seq[AddColumn]].map { col =>
            // Convert V2 `AddColumn` to V1 `QualifiedColType` as `AlterTableAddColumnsDeltaCommand`
            // is a V1 command.
            val name = col.fieldNames()
            val path = if (name.length > 1) Some(UnresolvedFieldName(name.init)) else None
            QualifiedColType(
              path,
              name.last,
              col.dataType(),
              col.isNullable,
              Option(col.comment()),
              Option(col.position()).map(UnresolvedFieldPosition)
            )
          }).run(spark)

      case (t, deleteColumns) if t == classOf[DeleteColumn] =>
        AlterTableDropColumnsDeltaCommand(
          table, deleteColumns.asInstanceOf[Seq[DeleteColumn]].map(_.fieldNames().toSeq)).run(spark)

      case (t, newProperties) if t == classOf[SetProperty] =>
        AlterTableSetPropertiesDeltaCommand(
          table,
          DeltaConfigs.validateConfigurations(
            newProperties.asInstanceOf[Seq[SetProperty]].map { prop =>
              prop.property() -> prop.value()
            }.toMap)
        ).run(spark)

      case (t, oldProperties) if t == classOf[RemoveProperty] =>
        AlterTableUnsetPropertiesDeltaCommand(
          table,
          oldProperties.asInstanceOf[Seq[RemoveProperty]].map(_.property()),
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case (t, columnChanges) if classOf[ColumnChange].isAssignableFrom(t) =>
        def getColumn(fieldNames: Seq[String]): (StructField, Option[ColumnPosition]) = {
          columnUpdates.getOrElseUpdate(fieldNames, {
            // TODO: Theoretically we should be able to fetch the snapshot from a txn.
            val schema = table.snapshot.schema
            val colName = UnresolvedAttribute(fieldNames).name
            val fieldOpt = schema.findNestedField(fieldNames, includeCollections = true,
              spark.sessionState.conf.resolver)
              .map(_._2)
            val field = fieldOpt.getOrElse {
              throw DeltaErrors.nonExistentColumnInSchema(colName, schema.treeString)
            }
            field -> None
          })
        }

        columnChanges.foreach {
          case comment: UpdateColumnComment =>
            val field = comment.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.withComment(comment.newComment()) -> pos

          case dataType: UpdateColumnType =>
            val field = dataType.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(dataType = dataType.newDataType()) -> pos

          case position: UpdateColumnPosition =>
            val field = position.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField -> Option(position.position())

          case nullability: UpdateColumnNullability =>
            val field = nullability.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(nullable = nullability.nullable()) -> pos

          case rename: RenameColumn =>
            val field = rename.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(name = rename.newName()) -> pos


          case other =>
            throw DeltaErrors.unrecognizedColumnChange(s"${other.getClass}")
        }

      case (t, locations) if t == classOf[SetLocation] =>
        if (locations.size != 1) {
          throw DeltaErrors.cannotSetLocationMultipleTimes(
            locations.asInstanceOf[Seq[SetProperty]].map(_.value()))
        }
        if (table.tableIdentifier.isEmpty) {
          throw DeltaErrors.setLocationNotSupportedOnPathIdentifiers()
        }
        AlterTableSetLocationDeltaCommand(
          table,
          locations.head.asInstanceOf[SetProperty].value()).run(spark)

      case (t, constraints) if t == classOf[AddConstraint] =>
        constraints.foreach { constraint =>
          val c = constraint.asInstanceOf[AddConstraint]
          AlterTableAddConstraintDeltaCommand(table, c.constraintName, c.expr).run(spark)
        }

      case (t, constraints) if t == classOf[DropConstraint] =>
        constraints.foreach { constraint =>
          val c = constraint.asInstanceOf[DropConstraint]
          AlterTableDropConstraintDeltaCommand(table, c.constraintName, c.ifExists).run(spark)
        }

    }

    columnUpdates.foreach { case (fieldNames, (newField, newPositionOpt)) =>
      AlterTableChangeColumnDeltaCommand(
        table,
        fieldNames.dropRight(1),
        fieldNames.last,
        newField,
        newPositionOpt,
        syncIdentity = syncIdentity).run(spark)
    }

    loadTable(ident)
  }

  // We want our catalog to handle Delta, therefore for other data sources that want to be
  // created, we just have this wrapper StagedTable to only drop the table if the commit fails.
  private case class BestEffortStagedTable(
      ident: Identifier,
      table: Table,
      catalog: TableCatalog) extends StagedTable with SupportsWrite {
    override def abortStagedChanges(): Unit = catalog.dropTable(ident)

    override def commitStagedChanges(): Unit = {}

    // Pass through
    override def name(): String = table.name()
    override def schema(): StructType = table.schema()
    override def partitioning(): Array[Transform] = table.partitioning()
    override def capabilities(): util.Set[TableCapability] = table.capabilities()
    override def properties(): util.Map[String, String] = table.properties()

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = table match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ => throw DeltaErrors.unsupportedWriteStagedTable(name)
    }
  }
}

/**
 * A trait for handling table access through delta.`/some/path`. This is a stop-gap solution
 * until PathIdentifiers are implemented in Apache Spark.
 */
trait SupportsPathIdentifier extends TableCatalog { self: DeltaCatalog =>

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  protected lazy val catalog: SessionCatalog = spark.sessionState.catalog

  private def hasDeltaNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head)
  }

  private def hasIcebergNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && ident.namespace().head.equalsIgnoreCase("iceberg")
  }

  protected def isIcebergPathIdentifier(ident: Identifier): Boolean = {
    hasIcebergNamespace(ident) && new Path(ident.name()).isAbsolute
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(table.identifier)
  }

  protected def isPathIdentifier(tableIdentifier: TableIdentifier) : Boolean = {
    isPathIdentifier(Identifier.of(tableIdentifier.database.toArray, tableIdentifier.table))
  }

  override def tableExists(ident: Identifier): Boolean = recordFrameProfile(
      "DeltaCatalog", "tableExists") {
    if (isPathIdentifier(ident)) {
      val path = new Path(ident.name())
      // scalastyle:off deltahadoopconfiguration
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      fs.exists(path) && fs.listStatus(path).nonEmpty
    } else {
      super.tableExists(ident)
    }
  }
}

object BucketTransform {
  def unapply(transform: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] = {
    val arguments = transform.arguments()
    if (transform.name() == "sorted_bucket") {
      var posOfLit: Int = -1
      var numOfBucket: Int = -1
      arguments.zipWithIndex.foreach {
        case (literal: Literal[_], i) if literal.dataType() == IntegerType =>
          numOfBucket = literal.value().asInstanceOf[Integer]
          posOfLit = i
        case _ =>
      }
      Some(numOfBucket, arguments.take(posOfLit).map(_.asInstanceOf[NamedReference]),
        arguments.drop(posOfLit + 1).map(_.asInstanceOf[NamedReference]))
    } else if (transform.name() == "bucket") {
      val numOfBucket = arguments(0) match {
        case literal: Literal[_] if literal.dataType() == IntegerType =>
          literal.value().asInstanceOf[Integer]
        case _ => throw new IllegalStateException("invalid bucket transform")
      }
      Some(numOfBucket, arguments.drop(1).map(_.asInstanceOf[NamedReference]),
        Seq.empty[FieldReference])
    } else {
      None
    }
  }
}
