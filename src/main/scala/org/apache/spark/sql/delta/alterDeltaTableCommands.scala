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

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData
import org.apache.spark.sql.catalyst.plans.logical.QualifiedColType
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.types._

/**
 * A super trait for alter table commands that modify Delta tables.
 */
trait AlterDeltaTableCommand {

  def deltaTableIdentifier: DeltaTableIdentifier

  def recordDeltaOperation[T](log: DeltaLog, opType: String)(thunk: => T): T = thunk
}

/**
 * A command that sets Delta table configuration.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class AlterTableSetPropertiesDeltaCommand(
    deltaTableIdentifier: DeltaTableIdentifier,
    configuration: Map[String, String])
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = deltaTableIdentifier.getDeltaLog(sparkSession)
    recordDeltaOperation(deltaLog, "delta.ddl.alter.setProperties") {
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
      }

      DeltaConfigs.verifyProtocolVersionRequirements(configuration, txn.protocol)
      val metadata = txn.metadata
      val newMetadata = metadata.copy(configuration = metadata.configuration ++ configuration)
      txn.commit(newMetadata :: Nil, DeltaOperations.SetTableProperties(configuration))

      Seq.empty[Row]
    }
  }
}

/**
 * A command that unsets Delta table configuration.
 * If ifExists is false, each individual key will be checked if it exists or not, it's a
 * one-by-one operation, not an all or nothing check. Otherwise, non-existent keys will be ignored.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class AlterTableUnsetPropertiesDeltaCommand(
    deltaTableIdentifier: DeltaTableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean)
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = deltaTableIdentifier.getDeltaLog(sparkSession)
    recordDeltaOperation(deltaLog, "delta.ddl.alter.unsetProperties") {
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
      }
      val metadata = txn.metadata

      val normalizedKeys = DeltaConfigs.normalizeConfigKeys(propKeys)
      if (!ifExists) {
        normalizedKeys.foreach { k =>
          if (!metadata.configuration.contains(k)) {
            throw new AnalysisException(
              s"Attempted to unset non-existent property '$k' in table $deltaTableIdentifier")
          }
        }
      }

      val newConfiguration = metadata.configuration.filterNot {
        case (key, _) => normalizedKeys.contains(key)
      }
      val newMetadata = metadata.copy(configuration = newConfiguration)
      txn.commit(newMetadata :: Nil, DeltaOperations.UnsetTableProperties(normalizedKeys, ifExists))

      Seq.empty[Row]
    }
  }
}

/**
 * A command that add columns to a Delta table.
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
 * }}}
*/
case class AlterTableAddColumnsDeltaCommand(
    deltaTableIdentifier: DeltaTableIdentifier,
    colsToAddWithPosition: Seq[QualifiedColType])
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = deltaTableIdentifier.getDeltaLog(sparkSession)
    recordDeltaOperation(deltaLog, "delta.ddl.alter.addColumns") {
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
      }

      if (SchemaUtils.filterRecursively(
            StructType(colsToAddWithPosition.map {
              case QualifiedColTypeForDelta(_, column) => column
            }), true)(!_.nullable).nonEmpty) {
        throw DeltaErrors.operationNotSupportedException("NOT NULL in ALTER TABLE ADD COLUMNS")
      }

      // TODO: remove this after auto cache refresh is merged.
      try {
        sparkSession.catalog.uncacheTable(deltaTableIdentifier.quotedString)
      } catch {
        case NonFatal(e) =>
          log.warn(s"Exception when attempting to uncache table $deltaTableIdentifier", e)
      }

      val metadata = txn.metadata
      val oldSchema = metadata.schema

      val resolver = sparkSession.sessionState.conf.resolver
      val newSchema = colsToAddWithPosition.foldLeft(oldSchema) {
        case (schema, QualifiedColTypeForDelta(columnPath, column)) =>
          val (parentPosition, lastSize) =
            SchemaUtils.findColumnPosition(columnPath, schema, resolver)
          SchemaUtils.addColumn(schema, column, parentPosition :+ lastSize)
      }

      SchemaUtils.checkColumnNameDuplication(newSchema, "in adding columns")
      ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.commit(newMetadata :: Nil, DeltaOperations.AddColumns(
        colsToAddWithPosition.map {
          case QualifiedColTypeForDelta(path, col) =>
            DeltaOperations.QualifiedColTypeWithPositionForLog(path, col, None)
        }))

      Seq.empty[Row]
    }
  }

  object QualifiedColTypeForDelta {

    def unapply(col: QualifiedColType): Option[(Seq[String], StructField)] = {
      val builder = new MetadataBuilder
      col.comment.foreach(builder.putString("comment", _))

      val cleanedDataType = HiveStringType.replaceCharType(col.dataType)
      if (col.dataType != cleanedDataType) {
        builder.putString(HIVE_TYPE_STRING, col.dataType.catalogString)
      }

      // TODO: This is trying to read col.nullable, which doesn't exist in OSS
      val field = StructField(col.name.last, cleanedDataType, nullable = true, builder.build())

      Some((col.name.init, field))
    }
  }
}

/**
 * A command to change the column for a Delta table, support changing the comment of a column and
 * reordering columns.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
 *   [FIRST | AFTER column_name];
 * }}}
 */
case class AlterTableChangeColumnDeltaCommand(
    deltaTableIdentifier: DeltaTableIdentifier,
    columnPath: Seq[String],
    columnName: String,
    newColumn: StructField)
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = deltaTableIdentifier.getDeltaLog(sparkSession)
    recordDeltaOperation(deltaLog, "delta.ddl.alter.changeColumns") {
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
      }

      val metadata = txn.metadata
      val oldSchema = metadata.schema

      val resolver = sparkSession.sessionState.conf.resolver
      val (position, _) =
        SchemaUtils.findColumnPosition(columnPath :+ columnName, oldSchema, resolver)
      val (droppedSchema, originalColumn) = SchemaUtils.dropColumn(oldSchema, position)

      if (!resolver(columnName, newColumn.name) ||
          SchemaUtils.canChangeDataType(originalColumn.dataType, newColumn.dataType, resolver,
            columnPath :+ originalColumn.name).nonEmpty ||
          (originalColumn.nullable && !newColumn.nullable)) {
        throw DeltaErrors.alterTableChangeColumnException(
              s"'${UnresolvedAttribute(columnPath :+ originalColumn.name).name}' with type " +
              s"'${originalColumn.dataType}" +
              s" (nullable = ${originalColumn.nullable})'",
              s"'${UnresolvedAttribute(Seq(newColumn.name)).name}' with type " +
              s"'${newColumn.dataType}" +
              s" (nullable = ${newColumn.nullable})'")
      }

      val columnToAdd =
        newColumn.getComment.map(originalColumn.withComment).getOrElse(originalColumn)
        .copy(
          dataType =
            SchemaUtils.changeDataType(originalColumn.dataType, newColumn.dataType, resolver),
          nullable = newColumn.nullable)

      val newSchema = SchemaUtils.addColumn(droppedSchema, columnToAdd, position)

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.commit(newMetadata :: Nil, DeltaOperations.ChangeColumn(
        columnPath, columnName, newColumn, None))

      Seq.empty[Row]
    }
  }
}

/**
 * A command to replace columns for a Delta table, support changing the comment of a column,
 * reordering columns, and loosening nullabilities.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier REPLACE COLUMNS (col_spec[, col_spec ...]);
 * }}}
 */
case class AlterTableReplaceColumnsDeltaCommand(
    deltaTableIdentifier: DeltaTableIdentifier,
    columns: Seq[StructField])
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = deltaTableIdentifier.getDeltaLog(sparkSession)
    recordDeltaOperation(deltaLog, "delta.ddl.alter.replaceColumns") {
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
      }

      val metadata = txn.metadata
      val existingSchema = metadata.schema

      val resolver = sparkSession.sessionState.conf.resolver
      val changingSchema = StructType(columns)

      SchemaUtils.canChangeDataType(existingSchema, changingSchema, resolver).foreach { operation =>
        throw DeltaErrors.alterTableReplaceColumnsException(
          existingSchema, changingSchema, operation)
      }

      val newSchema = SchemaUtils.changeDataType(existingSchema, changingSchema, resolver)
        .asInstanceOf[StructType]

      SchemaUtils.checkColumnNameDuplication(newSchema, "in replacing columns")
      ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.commit(newMetadata :: Nil, DeltaOperations.ReplaceColumns(columns))

      Seq.empty[Row]
    }
  }
}

/**
 * A command to change the location of a Delta table. Effectively, this only changes the symlink
 * in the Hive MetaStore from one Delta table to another.
 *
 * This command errors out if the new location is not a Delta table. By default, the new Delta
 * table must have the same schema as the old table, but we have a SQL conf that allows users
 * to bypass this schema check.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier SET LOCATION 'path/to/new/delta/table';
 * }}}
 */
case class AlterTableSetLocationDeltaCommand(
    tableIdentifier: TableIdentifier,
    location: String)
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  val deltaTableIdentifier: DeltaTableIdentifier = DeltaTableIdentifier(None, Some(tableIdentifier))

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    val locUri = CatalogUtils.stringToURI(location)
    val oldTable = deltaTableIdentifier.getDeltaLog(sparkSession).update()
    if (oldTable.version == -1) {
      throw DeltaErrors.notADeltaTableException(deltaTableIdentifier)
    }
    val oldMetadata = oldTable.metadata

    val newTable = DeltaLog.forTable(sparkSession, location).update()
    if (newTable.version == -1) {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(location)))
    }
    val newMetadata = newTable.metadata
    val bypassSchemaCheck = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK)

    if (!bypassSchemaCheck && !schemasEqual(oldMetadata, newMetadata)) {
      throw DeltaErrors.alterTableSetLocationSchemaMismatchException(
        oldMetadata.schema, newMetadata.schema)
    }
    catalog.alterTable(table.withNewStorage(locationUri = Some(locUri)))

    Seq.empty[Row]
  }

  private def schemasEqual(
      oldMetadata: actions.Metadata, newMetadata: actions.Metadata): Boolean = {
    oldMetadata.schema == newMetadata.schema &&
      oldMetadata.partitionSchema == newMetadata.partitionSchema
  }
}
