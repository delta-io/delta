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
package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StructType

/**
* Single entry point for all write or declaration operations for Delta tables accessed through
* the table name.
*
* @param table The table identifier for the Delta table
* @param existingTableOpt The existing table for the same identifier if exists
* @param mode The save mode when writing data. Relevant when the query is empty or set to Ignore
*             with `CREATE TABLE IF NOT EXISTS`.
* @param query The query to commit into the Delta table if it exist. This can come from
*                - CTAS
*                - saveAsTable
*/
case class CreateDeltaTableCommand(
    table: CatalogTable,
    existingTableOpt: Option[CatalogTable],
    mode: SaveMode,
    query: Option[LogicalPlan],
    operation: TableCreationModes.CreationMode = TableCreationModes.CreateTable)
  extends RunnableCommand
  with DeltaLogging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.identifier.database.isDefined, "Database should've been fixed at analysis")
    // There is a subtle race condition here, where the table can be created by someone else
    // while this command is running. Nothing we can do about that though :(
    val tableExists = existingTableOpt.isDefined
    if (mode == SaveMode.Ignore && tableExists) {
      // Early exit on ignore
      return Nil
    } else if (mode == SaveMode.ErrorIfExists && tableExists) {
      throw new AnalysisException(s"Table ${table.identifier.unquotedString} already exists.")
    }

    val tableWithLocation = if (tableExists) {
      val existingTable = existingTableOpt.get
      table.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          val tableName = table.identifier.quotedString
          throw new AnalysisException(
            s"The location of the existing table $tableName is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${table.location}`.")
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
      // We are defining a new external table
      assert(table.tableType == CatalogTableType.EXTERNAL)
      table
    }

    val isManagedTable = tableWithLocation.tableType == CatalogTableType.MANAGED
    val tableLocation = new Path(tableWithLocation.location)
    val fs = tableLocation.getFileSystem(sparkSession.sessionState.newHadoopConf())
    val deltaLog = DeltaLog.forTable(sparkSession, tableLocation)
    recordDeltaOperation(deltaLog, "delta.ddl.createTable") {
      val txn = deltaLog.startTransaction()

      if (query.isDefined) {
        // If the mode is Ignore or ErrorIfExists, the table must not exist, or we would return
        // earlier. And the data should not exist either, to match the behavior of
        // Ignore/ErrorIfExists mode. This means the table path should not exist or is empty.
        if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
          assert(!tableExists)
          // We may have failed a previous write. The retry should still succeed even if we have
          // garbage data
          if (txn.readVersion > -1 || !fs.exists(deltaLog.logPath)) {
            assertPathEmpty(sparkSession, tableWithLocation)
          }
        }
        // We are either appending/overwriting with saveAsTable or creating a new table with CTAS

        val data = Dataset.ofRows(sparkSession, query.get)

        val options = new DeltaOptions(table.properties, sparkSession.sessionState.conf)
        val actions = WriteIntoDelta(
          deltaLog = deltaLog,
          mode = mode,
          options,
          partitionColumns = table.partitionColumnNames,
          configuration = table.properties,
          data = data).write(txn, sparkSession)

        val op = getOperation(txn.readVersion, txn.metadata, isManagedTable, Some(options))
        txn.commit(actions, op)
      } else {
        // We are defining a table using the Create Table statement.
        assert(!tableExists, "Can't recreate a table when it exists")

        if (isManagedTable) {
          // When creating a managed table, the table path should not exist or is empty, or
          // users would be surprised to see the data, or see the data directory being dropped
          // after the table is dropped.
          assertPathEmpty(sparkSession, tableWithLocation)
        }

        // This is either a new table, or, we never defined the schema of the table. While it is
        // unexpected that `txn.metadata.schema` to be empty when txn.readVersion >= 0, we still
        // guard against it, in case of checkpoint corruption bugs.
        val noExistingMetadata = txn.readVersion == -1 || txn.metadata.schema.isEmpty
        if (noExistingMetadata) {
          assertTableSchemaDefined(fs, tableLocation, tableWithLocation, sparkSession)
          assertPathEmpty(sparkSession, tableWithLocation)
          val newMetadata = Metadata(
            description = table.comment.orNull,
            // This is a user provided schema.
            // Doesn't come from a query, Follow nullability invariants.
            schemaString = table.schema.json,
            partitionColumns = table.partitionColumnNames,
            configuration = table.properties)
          val op = getOperation(txn.readVersion, newMetadata, isManagedTable, None)
          txn.commit(newMetadata :: Nil, op)
        } else {
          verifyTableMetadata(txn, tableWithLocation)
        }
      }

      // We would have failed earlier on if we couldn't ignore the existence of the table
      // In addition, we just might using saveAsTable to append to the table, so ignore the creation
      // if it already exists.
      // Note that someone may have dropped and recreated the table in a separate location in the
      // meantime... Unfortunately we can't do anything there at the moment, because Hive sucks.
      val tableWithDefaultOptions = tableWithLocation.copy(
        schema = new StructType(),
        tracksPartitionsInCatalog =
          tableWithLocation.partitionColumnNames.nonEmpty &&
            sparkSession.sessionState.conf.manageFilesourcePartitions)

      updateCatalog(sparkSession, tableWithDefaultOptions)

      Nil
    }
  }


  private def assertPathEmpty(
      sparkSession: SparkSession,
      tableWithLocation: CatalogTable): Unit = {
    val path = new Path(tableWithLocation.location)
    val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
    // Verify that the table location associated with CREATE TABLE doesn't have any data. Note that
    // we intentionally diverge from this behavior w.r.t regular datasource tables (that silently
    // overwrite any previous data)
    if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
      throw new AnalysisException(s"Cannot create table ('${tableWithLocation.identifier}')." +
        s" The associated location ('${tableWithLocation.location}') is not empty.")
    }
  }

  private def assertTableSchemaDefined(
      fs: FileSystem,
      path: Path,
      table: CatalogTable,
      sparkSession: SparkSession): Unit = {
    // Users did not specify the schema. We expect the schema exists in Delta.
    if (table.schema.isEmpty) {
      if (table.tableType == CatalogTableType.EXTERNAL) {
        if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
          throw DeltaErrors.createExternalTableWithoutLogException(
            path, table.identifier.quotedString, sparkSession)
        } else {
          throw DeltaErrors.createExternalTableWithoutSchemaException(
            path, table.identifier.quotedString, sparkSession)
        }
      } else {
        throw DeltaErrors.createManagedTableWithoutSchemaException(
          table.identifier.quotedString, sparkSession)
      }
    }
  }

  /**
   * Verify against our transaction metadata that the user specified the right metadata for the
   * table.
   */
  private def verifyTableMetadata(
     txn: OptimisticTransaction,
     tableDesc: CatalogTable): Unit = {
    val existingMetadata = txn.metadata
    val path = new Path(tableDesc.location)

    // The delta log already exists. If they give any configuration, we'll make sure it all matches.
    // Otherwise we'll just go with the metadata already present in the log.
    // The schema compatibility checks will be made in `WriteIntoDelta` for CreateTable
    // with a query
    if (txn.readVersion > -1) {
      if (tableDesc.schema.nonEmpty && tableDesc.schema != existingMetadata.schema) {
        // We check exact alignment on create table if everything is provided
        throw new AnalysisException(
          s"""
             |The specified schema does not match the existing schema at $path.
             |
             |== Specified ==
             |${tableDesc.schema.treeString}
             |
             |== Existing ==
             |${existingMetadata.schema.treeString}
             """.stripMargin)
      }

      // If schema is specified, we must make sure the partitioning matches, even the partitioning
      // is not specified.
      if (tableDesc.schema.nonEmpty &&
        tableDesc.partitionColumnNames != existingMetadata.partitionColumns) {
        throw new AnalysisException(
          s"""
             |The specified partitioning does not match the existing partitioning at $path.
             |
             |== Specified ==
             |${tableDesc.partitionColumnNames.mkString(", ")}
             |
             |== Existing ==
             |${existingMetadata.partitionColumns.mkString(", ")}
             """.stripMargin)
      }

      if (tableDesc.properties.nonEmpty && tableDesc.properties != existingMetadata.configuration) {
        throw new AnalysisException(
          s"""
             |The specified properties do not match the existing properties at $path.
             |
             |== Specified ==
             |${tableDesc.properties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
             |
             |== Existing ==
             |${existingMetadata.configuration.map { case (k, v) => s"$k=$v" }.mkString("\n")}
             """.stripMargin)
      }
    }
  }

  private def getOperation(
      tableVersion: Long,
      metadata: Metadata,
      isManagedTable: Boolean,
      options: Option[DeltaOptions]): DeltaOperations.Operation = operation match {
    case TableCreationModes.CreateTable if existingTableOpt.isDefined && query.isDefined =>
      DeltaOperations.Write(mode, Option(table.partitionColumnNames), options.get.replaceWhere)

    case TableCreationModes.CreateTable =>
      DeltaOperations.CreateTable(metadata, isManagedTable, query.isDefined)

    case TableCreationModes.ReplaceTable =>
      DeltaOperations.ReplaceTable(metadata, isManagedTable, orCreate = false, query.isDefined)

    case TableCreationModes.CreateOrReplaceTable if options.exists(_.replaceWhere.isDefined) =>
      DeltaOperations.Write(mode, Option(table.partitionColumnNames), options.get.replaceWhere)

    case TableCreationModes.CreateOrReplaceTable =>
      DeltaOperations.ReplaceTable(metadata, isManagedTable, orCreate = true, query.isDefined)
  }

  private def updateCatalog(spark: SparkSession, table: CatalogTable): Unit = operation match {
    case TableCreationModes.CreateTable =>
      spark.sessionState.catalog.createTable(
        table,
        ignoreIfExists = existingTableOpt.isDefined,
        validateLocation = false)
    case TableCreationModes.ReplaceTable if existingTableOpt.isDefined =>
      spark.sessionState.catalog.alterTable(table)
    case TableCreationModes.ReplaceTable =>
      val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
      throw new CannotReplaceMissingTableException(ident)
    case TableCreationModes.CreateOrReplaceTable if existingTableOpt.isDefined =>
      spark.sessionState.catalog.alterTable(table)
    case TableCreationModes.CreateOrReplaceTable =>
      spark.sessionState.catalog.createTable(
        table,
        ignoreIfExists = false,
        validateLocation = false)
  }
}

object TableCreationModes {
  sealed trait CreationMode {
    def mode: SaveMode
  }
  case object CreateTable extends CreationMode {
    override def mode: SaveMode = SaveMode.ErrorIfExists
  }
  case object ReplaceTable extends CreationMode  {
    override def mode: SaveMode = SaveMode.Overwrite
  }
  case object CreateOrReplaceTable extends CreationMode {
    override def mode: SaveMode = SaveMode.Overwrite
  }
}
