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

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalog.v2.{Identifier, StagingTableCatalog}
import org.apache.spark.sql.catalog.v2.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors}
import org.apache.spark.sql.delta.commands.CreateDeltaTableCommand
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.sources.v2.{StagedTable, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.sources.v2.writer.{V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaCatalog(val spark: SparkSession) extends V2SessionCatalog(spark.sessionState)
    with StagingTableCatalog{
  def this() = {
    this(SparkSession.active)
    print(s"AAAAAAA instantiated\n")
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
      identifier = ident.asTableIdentifier,
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
    val provider = properties.getOrDefault("provider", null)
    provider match {
      case "delta" =>
        val capabilities = Set[TableCapability](V1_BATCH_WRITE).asJava
        new NoOpStagedTable(ident, schema, partitions, properties, capabilities)
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

  private class NoOpStagedTable(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String],
      override val capabilities: util.Set[TableCapability]) extends StagedTable with SupportsWrite {
    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def commitStagedChanges(): Unit = {}

    override def newWriteBuilder(options: CaseInsensitiveStringMap): V1WriteBuilder = {
      new DeltaV1WriteBuilder(ident, schema, partitions, properties)
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
}
