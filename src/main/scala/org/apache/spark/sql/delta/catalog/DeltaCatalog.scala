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
package org.apache.spark.sql.delta.catalog

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.sql.QualifiedColType
import org.apache.spark.sql.delta.{AlterTableAddColumnsDeltaCommand, AlterTableChangeColumnDeltaCommand, AlterTableSetLocationDeltaCommand, AlterTableSetPropertiesDeltaCommand, AlterTableUnsetPropertiesDeltaCommand, DeltaConfigs, DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.delta.commands.CreateDeltaTableCommand
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{V1WriteBuilder, WriteBuilder}

class DeltaCatalog(val spark: SparkSession) extends DelegatingCatalogExtension
    with StagingTableCatalog {
  def this() = {
    this(SparkSession.active)
  }

  // copy of the same lazy val from V2SessionCatalog where it's private
  private lazy val catalog: SessionCatalog = spark.sessionState.catalog

  private def createDeltaTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      sourceQuery: Option[LogicalPlan]): Table = {
    // These two keys are properties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    val tableProperties = properties.asScala.filterKeys {
      case "location" => false
      case "provider" => false
      case _ => true
    }
    // START: This entire block until END is a copy-paste from the super method.
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    val location = Option(properties.get("location"))
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED

    val tableDesc = new CatalogTable(
      identifier = TableIdentifier(ident.name()),
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("delta"),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = spark.sessionState.conf.manageFilesourcePartitions,
      comment = Option(properties.get("comment")))
    // END: copy-paste from the super method finished.

    val withDb = verifyTableAndSolidify(tableDesc, None)
    ParquetSchemaConverter.checkFieldNames(tableDesc.schema.fieldNames)
    CreateDeltaTableCommand(
      withDb, getExistingTableIfExists(tableDesc), SaveMode.ErrorIfExists, sourceQuery).run(spark)

    loadTable(ident)
  }

  private def isPathIdentifier(ident: Identifier): Boolean = {
    try {
      ident.namespace().sameElements(Array("delta")) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if v1.v1Table.provider.contains("delta") =>
          val deltaLog = DeltaLog.forTable(spark, new Path(v1.catalogTable.location))
          DeltaTableV2(deltaLog, tableIdentifier = Some(v1.catalogTable.identifier.unquotedString))
        case o => o
      }
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
        if isPathIdentifier(ident) =>
        val deltaLog = DeltaLog.forTable(spark, new Path(ident.name()))
        DeltaTableV2(deltaLog)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val provider = properties.getOrDefault("provider", null)
    provider match {
      case "delta" => createDeltaTable(ident, schema, partitions, properties, sourceQuery = None)
      case _ => super.createTable(ident, schema, partitions, properties)
    }
  }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    throw new IllegalStateException("not supported yet")
  }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    throw new IllegalStateException("not supported yet")
  }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    // TODO: As discussed, the provider is sometimes not passed here in OSS Spark as of my branch,
    // but we think PR 25669 fixed it.
    // val provider = properties.getOrDefault("provider", null)
    val provider = "delta"
    provider match {
      case "delta" =>
        new StagedDeltaTableV2(ident, schema, partitions, properties)
      case _ =>
        throw new IllegalStateException("not supported yet")
    }
  }

  // Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw new UnsupportedOperationException(
          s"SessionCatalog does not support partition transform: $transform")
    }

    (identityCols, bucketSpec)
  }

  // Copy-pasted from DeltaAnalysis.
  private def verifyTableAndSolidify(
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

  // Copy-pasted from DeltaAnalysis.
  private def getExistingTableIfExists(table: CatalogTable): Option[CatalogTable] = {
    val tableExists = catalog.tableExists(table.identifier)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table.identifier)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(
          s"${table.identifier} is a view. You may not write data into a view.")
      }
      // TODO(burak): Maybe drop old table if mode is overwrite?
      if (!DeltaSourceUtils.isDeltaTable(oldTable.provider)) {
        throw new AnalysisException(s"${table.identifier} is not a Delta table. Please drop this " +
          "table first if you would like to create it with Databricks Delta.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  // We keep this as an internal class because it needs to call into some catalog internals for the
  // eventual table creation.
  private class StagedDeltaTableV2(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String]) extends StagedTable with SupportsWrite {
    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def commitStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = Set(
      ACCEPT_ANY_SCHEMA, BATCH_READ,
      V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
    ).asJava

    override def newWriteBuilder(options: CaseInsensitiveStringMap): V1WriteBuilder = {
      // TODO: is this right? What's the appropriate distinction to keep for properties and options
      val combinedProps = options.asCaseSensitiveMap().asScala ++ properties.asScala
      new DeltaV1WriteBuilder(ident, schema, partitions, combinedProps.asJava)
    }
  }

  /*
   * We have to do extend both classes. Only extending V1WriteBuilder gives
   *
   * Unable to implement a super accessor required by trait V1WriteBuilder unless
   * org.apache.spark.sql.sources.v2.writer.WriteBuilder is directly extended by class
   * DeltaCatalog$DeltaV1WriteBuilder.
   */
  private class DeltaV1WriteBuilder(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]) extends WriteBuilder with V1WriteBuilder {
    override def buildForV1Write(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          createDeltaTable(ident, schema, partitions, properties, Some(data.logicalPlan))
        }
      }
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val provider = loadTable(ident).properties().get("provider")
    // if (provider != "delta") return super.alterTable(ident, changes: _*)
    val deltaIdentifier = DeltaTableIdentifier(spark, TableIdentifier(ident.name())).getOrElse {
      throw new IllegalStateException("Provider was delta, but table is not a Delta table")
    }

    // We group the table changes by their type, since Delta applies each in a separate action.
    // We also must define an artificial type for SetLocation, since data source V2 considers
    // location just another property but it's special in Delta.
    class SetLocation {}
    val grouped = changes.groupBy {
      case s: SetProperty if s.property() == "location" => classOf[SetLocation]
      case c => c.getClass
    }

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        AlterTableAddColumnsDeltaCommand(
          deltaIdentifier,
          newColumns.asInstanceOf[Seq[AddColumn]].map { col =>
            QualifiedColType(col.fieldNames(), col.dataType(), Option(col.comment()))
          }).run(spark)

      case (t, newProperties) if t == classOf[SetProperty] =>
        AlterTableSetPropertiesDeltaCommand(
          deltaIdentifier,
          DeltaConfigs.validateConfigurations(
            newProperties.asInstanceOf[Seq[SetProperty]].map { prop =>
              prop.property() -> prop.value()
            }.toMap)
        ).run(spark)

      case (t, oldProperties) if t == classOf[RemoveProperty] =>
        AlterTableUnsetPropertiesDeltaCommand(
          deltaIdentifier,
          oldProperties.asInstanceOf[Seq[RemoveProperty]].map(_.property()),
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case (t, columnChanges) if t == classOf[UpdateColumnComment] =>
        columnChanges.asInstanceOf[Seq[UpdateColumnComment]].foreach { change =>
          val existing = DeltaLog.forTable(spark, TableIdentifier(ident.name()))
              .snapshot
              .schema
              .findNestedField(change.fieldNames()).getOrElse {
            throw new IllegalStateException(
              s"Can't change comment of non-existing column ${change.fieldNames().mkString(",")}")
          }
          AlterTableChangeColumnDeltaCommand(
            deltaIdentifier,
            change.fieldNames().dropRight(1),
            change.fieldNames().last,
            existing.withComment(change.newComment())).run(spark)
        }

      case (t, columnChanges) if t == classOf[UpdateColumnType] =>
        columnChanges.asInstanceOf[Seq[UpdateColumnType]].foreach { change =>
          val existing = DeltaLog.forTable(spark, TableIdentifier(ident.name()))
            .snapshot
            .schema
            .findNestedField(change.fieldNames()).getOrElse {
            throw new IllegalStateException(
              s"Can't change comment of non-existing column ${change.fieldNames().mkString(",")}")
          }
          AlterTableChangeColumnDeltaCommand(
            deltaIdentifier,
            change.fieldNames().dropRight(1),
            change.fieldNames().last,
            existing.copy(dataType = change.newDataType())).run(spark)
        }

      case (t, locations) if t == classOf[SetLocation] =>
        if (locations.size != 1) {
          throw new IllegalArgumentException(s"Can't set location multiple times. Found " +
            s"${locations.asInstanceOf[Seq[SetProperty]].map(_.value())}")
        }
        AlterTableSetLocationDeltaCommand(
          TableIdentifier(ident.name()),
          locations.head.asInstanceOf[SetProperty].value()).run(spark)
    }

    loadTable(ident)
  }
}
