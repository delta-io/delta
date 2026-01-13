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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.hooks.{UpdateCatalog, UpdateCatalogFactory}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType

/**
 * A common trait implementing utility functions (e.g. catalog operations) for all commands that
 * create a Delta table.
 */
trait CreateDeltaTableLike extends SQLConfHelper {
  // The table to create.
  val table: CatalogTable

  // The existing table for the same identifier if exists.
  val existingTableOpt: Option[CatalogTable]

  // The table creation mode.
  val operation: TableCreationModes.CreationMode

  // Whether the table is accessed by path.
  val tableByPath: Boolean = false

  // The save mode when writing data. Relevant when the query is empty or set to Ignore with `CREATE
  // TABLE IF NOT EXISTS`.
  val mode: SaveMode

  // Whether the table is UC managed table with catalogManaged feature.
  val allowCatalogManaged: Boolean

  /**
   * Generates a `CatalogTable` with its `locationUri` set appropriately, depending on whether the
   * table already exists or is newly created.
   */
  protected def getCatalogTableWithLocation(sparkSession: SparkSession): CatalogTable = {
    val tableExistsInCatalog = existingTableOpt.isDefined
    if (tableExistsInCatalog) {
      val existingTable = existingTableOpt.get
      table.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          throw DeltaErrors.tableLocationMismatch(table, existingTable)
        case _ =>
      }
      table.copy(
        storage = existingTable.storage,
        tableType = existingTable.tableType)
    } else if (table.storage.locationUri.isEmpty) {
      // We are defining a new managed table
      assert(table.tableType == CatalogTableType.MANAGED)
      val loc = sparkSession.sessionState.catalog.defaultTablePath(table.identifier)
      table.copy(storage = table.storage.copy(locationUri = Some(loc)))
    } else {
      // 1. We are defining a new external table
      // 2. It's a managed table which already has the location populated. This can happen in DSV2
      //    CTAS flow.
      table
    }
  }

  /**
   * Here we disambiguate the catalog alterations we need to do based on the table operation, and
   * whether we have reached here through legacy code or DataSourceV2 code paths.
   */
  protected def updateCatalog(
      spark: SparkSession,
      table: CatalogTable,
      snapshot: Snapshot,
      query: Option[LogicalPlan],
      didNotChangeMetadata: Boolean,
      createTableFunc: Option[CatalogTable => Unit] = None
  ): Unit = {
    val cleaned = cleanupTableDefinition(spark, table, snapshot)
    operation match {
      case _ if tableByPath => // do nothing with the metastore if this is by path
      case TableCreationModes.Create =>
        if (createTableFunc.isDefined) {
          createTableFunc.get.apply(cleaned)
        } else {
          spark.sessionState.catalog.createTable(
            cleaned,
            ignoreIfExists = existingTableOpt.isDefined || mode == SaveMode.Ignore,
            validateLocation = false)
        }
      case TableCreationModes.Replace | TableCreationModes.CreateOrReplace
        if existingTableOpt.isDefined =>
        UpdateCatalogFactory.getUpdateCatalogHook(table, spark).updateSchema(spark, snapshot)
      case TableCreationModes.Replace =>
        val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
        throw DeltaErrors.cannotReplaceMissingTableException(ident)
      case TableCreationModes.CreateOrReplace =>
      spark.sessionState.catalog.createTable(
        cleaned,
        ignoreIfExists = false,
        validateLocation = false)
    }
    if (conf.getConf(DeltaSQLConf.HMS_FORCE_ALTER_TABLE_DATA_SCHEMA)) {
      spark.sessionState.catalog.alterTableDataSchema(cleaned.identifier, cleaned.schema)
    }
  }

  /** Clean up the information we pass on to store in the catalog. */
  private def cleanupTableDefinition(spark: SparkSession, table: CatalogTable, snapshot: Snapshot)
  : CatalogTable = {
    // These actually have no effect on the usability of Delta, but feature flagging legacy
    // behavior for now
    val storageProps = if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
      // Legacy behavior
      table.storage
    } else {
      table.storage.copy(properties = Map.empty)
    }

    // If we have to update the catalog, use the correct schema and table properties, otherwise
    // empty out the schema and property information
    if (conf.getConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED)) {
      val truncationThreshold = spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD)
      val (truncatedSchema, additionalProperties) = UpdateCatalog.truncateSchemaIfNecessary(
        snapshot.schema,
        truncationThreshold)

      table.copy(
        schema = truncatedSchema,
        // Hive does not allow for the removal of partition columns once stored.
        // To avoid returning the incorrect schema when the partition columns change,
        // we store the partition columns as regular data columns.
        partitionColumnNames = Nil,
        properties = UpdateCatalog.updatedProperties(snapshot)
          ++ additionalProperties,
        storage = storageProps,
        tracksPartitionsInCatalog = true)
    } else {
      // Setting table properties is required for creating catalogManaged tables.
      val properties: Map[String, String] =
        if (allowCatalogManaged) UpdateCatalog.updatedProperties(snapshot) else Map.empty
      table.copy(
        schema = new StructType(),
        properties = properties,
        partitionColumnNames = Nil,
        // Remove write specific options when updating the catalog
        storage = storageProps,
        tracksPartitionsInCatalog = true)
    }
  }

  /**
   * Horrible hack to differentiate between DataFrameWriterV1 and V2 so that we can decide
   * what to do with table metadata. In DataFrameWriterV1, mode("overwrite").saveAsTable,
   * behaves as a CreateOrReplace table, but we have asked for "overwriteSchema" as an
   * explicit option to overwrite partitioning or schema information. With DataFrameWriterV2,
   * the behavior asked for by the user is clearer: .createOrReplace(), which means that we
   * should overwrite schema and/or partitioning. Therefore we have this hack.
   */
  protected def isV1Writer: Boolean = {
    Thread.currentThread().getStackTrace.exists(_.toString.contains(
      classOf[org.apache.spark.sql.classic.DataFrameWriter[_]].getCanonicalName + "."))
  }
}
