/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType,
  CatalogUtils
}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableInfo, V1Table}
import org.apache.spark.sql.delta.{
  CatalogOwnedTableFeature,
  MaterializedRowCommitVersion,
  MaterializedRowId
}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{CreateDeltaTableCommand, TableCreationModes}
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedTableUtils,
  CoordinatedCommitsUtils
}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

/**
 * Spark 4.2 routes CREATE TABLE LIKE through TableCatalog#createTableLike.
 */
trait AbstractDeltaCatalogShims { self: AbstractDeltaCatalog =>

  override def createTableLike(
      ident: Identifier,
      tableInfo: TableInfo,
      sourceTable: Table): Table = recordFrameProfile("DeltaCatalog", "createTableLike") {
    val targetProperties = tableInfo.properties().asScala.toMap
    val targetProvider = targetProperties.get(TableCatalog.PROP_PROVIDER)

    sourceTable match {
      case source: DeltaTableV2 =>
        createDeltaTableLike(ident, source, targetProperties)
      case source: V1Table if shouldCreateDeltaTableLike(source.catalogTable, targetProvider) =>
        createDeltaTableLike(ident, source.catalogTable, targetProperties)
      case _ =>
        createTable(ident, tableInfo.schema(), tableInfo.partitions(), tableInfo.properties())
    }
  }

  private def shouldCreateDeltaTableLike(
      sourceTable: CatalogTable,
      targetProvider: Option[String]): Boolean = {
    DeltaSourceUtils.isDeltaTable(sourceTable.provider) ||
      targetProvider.exists(DeltaSourceUtils.isDeltaDataSourceName)
  }

  private def createDeltaTableLike(
      ident: Identifier,
      sourceTable: DeltaTableV2,
      targetProperties: Map[String, String]): Table = {
    val sourceCatalogTable = sourceTable.catalogTable.getOrElse {
      val sourceMetadata = sourceTable.initialSnapshot.metadata
      new CatalogTable(
        identifier = TableIdentifier(sourceTable.path.getName),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty.copy(locationUri = Some(sourceTable.path.toUri)),
        schema = sourceMetadata.schema,
        properties = sourceMetadata.configuration,
        partitionColumnNames = sourceMetadata.partitionColumns,
        provider = Some("delta"),
        comment = Option(sourceMetadata.description))
    }
    createDeltaTableLike(ident, sourceCatalogTable, targetProperties)
  }

  private def createDeltaTableLike(
      ident: Identifier,
      sourceTable: CatalogTable,
      targetProperties: Map[String, String]): Table = {
    val isTableByPath = isPathIdentifier(ident)
    val targetTableIdentifier =
      if (isTableByPath) TableIdentifier(ident.name())
      else TableIdentifier(ident.name(), ident.namespace().lastOption)

    val explicitDeltaProperties = stripReservedTableProperties(targetProperties)
    val locationUri =
      if (isTableByPath) {
        Some(new Path(ident.name()).toUri)
      } else {
        targetProperties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
      }
    val newStorage = sourceTable.storage.copy(locationUri = locationUri)
    val tableType =
      if (newStorage.locationUri.isEmpty && !isTableByPath) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      }

    var isEnablingCatalogOwnedViaExplicitPropertyOverrides = false
    val isDeltaSource = DeltaSourceUtils.isDeltaTable(sourceTable.provider)

    val (catalogTableTarget, protocol) =
      if (isDeltaSource) {
        CatalogOwnedTableUtils.validateUCTableIdNotPresent(property = explicitDeltaProperties)
        if (TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(
            configs = explicitDeltaProperties).contains(CatalogOwnedTableFeature)) {
          isEnablingCatalogOwnedViaExplicitPropertyOverrides = true
        }

        val deltaLogSrc = DeltaTableV2(spark, new Path(sourceTable.location))
        val snapshot = deltaLogSrc.initialSnapshot
        val sourceMetadata = snapshot.metadata
        val filteredConfig = sourceMetadata.configuration.filter { case (key, _) =>
          key != "delta.columnMapping.maxColumnId" &&
            key != MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP &&
            key != MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP &&
            !CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS.contains(key) &&
            key != UC_TABLE_ID_KEY
        }
        val sourceProtocol =
          if (isEnablingCatalogOwnedViaExplicitPropertyOverrides) {
            snapshot.protocol
          } else {
            snapshot.protocol.removeFeature(targetFeature = CatalogOwnedTableFeature)
          }

        (
          new CatalogTable(
            identifier = targetTableIdentifier,
            tableType = tableType,
            storage = newStorage,
            schema = sourceMetadata.schema,
            properties = filteredConfig ++ explicitDeltaProperties,
            partitionColumnNames = sourceMetadata.partitionColumns,
            provider = Some("delta"),
            comment = Option(sourceMetadata.description)),
          Some(sourceProtocol))
      } else {
        (
          new CatalogTable(
            identifier = targetTableIdentifier,
            tableType = tableType,
            storage = newStorage,
            schema = sourceTable.schema,
            properties = sourceTable.properties ++ explicitDeltaProperties,
            partitionColumnNames = sourceTable.partitionColumnNames,
            provider = Some("delta"),
            comment = sourceTable.comment),
          None)
      }

    val existingTableOpt = getExistingTableIfExists(
      targetTableIdentifier,
      identOpt = Some(ident),
      operation = TableCreationModes.Create)
    val newTable = verifyTableAndSolidify(catalogTableTarget, None)

    CreateDeltaTableCommand(
      table = newTable,
      existingTableOpt = existingTableOpt,
      mode = SaveMode.ErrorIfExists,
      query = None,
      operation = TableCreationModes.Create,
      protocol = protocol,
      tableByPath = isTableByPath).run(spark)

    loadTable(ident)
  }

  private def stripReservedTableProperties(
      properties: Map[String, String]): Map[String, String] = {
    properties.filter {
      case (TableCatalog.PROP_LOCATION, _) => false
      case (TableCatalog.PROP_PROVIDER, _) => false
      case (TableCatalog.PROP_COMMENT, _) => false
      case (TableCatalog.PROP_OWNER, _) => false
      case (TableCatalog.PROP_EXTERNAL, _) => false
      case ("path", _) => false
      case ("option.path", _) => false
      case _ => true
    }
  }
}
