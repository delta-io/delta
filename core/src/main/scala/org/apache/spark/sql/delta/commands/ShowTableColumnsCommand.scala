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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

/**
 * The column format of the result returned by the `SHOW COLUMNS` command.
 */
case class TableColumns(columnName: String)

/**
 * A command listing all column names of a table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) (tableName [(FROM | IN) schemaName] | path);
 * }}}
 *
 * @param tableName  (optional) the table name
 * @param path       (optional) the file path where the table located
 * @param schemaName (optional when tableName exists) the schema name (database name) of the table
 */
case class ShowTableColumnsCommand(
    tableName: Option[TableIdentifier],
    path: Option[String],
    schemaName: Option[String])
  extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = ExpressionEncoder[TableColumns]().schema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val lookupTable = schemaName match {
      case None => tableName
      case Some(db) => Some(TableIdentifier(tableName.get.identifier, Some(db)))
    }
    getSchema(sparkSession, path, lookupTable)
      .fieldNames
      .map { x => Row(x) }
      .toSeq
  }

  /**
   * Resolve `path` and `tableIdentifier` to get the underlying storage path if it's not a Delta
   * table, or its `CatalogTable` if it's a non-Delta table, or its description if it is a view.
   * The caller will make sure either `path` or `tableIdentifier` is set but not both. Then return
   * the table schema from storage path, `CatalogTable` or view description.
   */
  protected def getSchema(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier]): StructType = {
    val tablePath =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = spark.sessionState.catalog
        sessionCatalog.getTempView(tableIdentifier.get.table).map { x =>
          // If `path` is empty while `tableIdentifier` is set, we will check if `tableIdentifier`
          // is a view first. If so, return the schema in view description.
          return x.desc.schema
        }.getOrElse {
          lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)
          DeltaTableIdentifier(spark, tableIdentifier.get) match {
            // If `tableIdentifier` is a Delta table, get `tablePath` from `path` or
            // `metadata.location`. If `tableIdentifier` is a non-Delta table, return the schema in
            // catalog table if it exists.
            case Some(id) if id.path.nonEmpty =>
              new Path(id.path.get)
            case Some(id) if id.table.nonEmpty =>
              new Path(metadata.location)
            case _ =>
              return metadata.schema
          }
        }
      } else {
        // If either table identifier and path is empty, raise an error.
        throw DeltaErrors.missingTableIdentifierException("SHOW COLUMNS")
      }

    // Return the schema from snapshot if it is an Delta table. Or raise `fileNotFoundException` if
    // it is a non-Delta table (here the non-Delta table must be represented by `path`).
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    recordDeltaOperation(deltaLog, "delta.ddl.showColumns") {
      if (deltaLog.snapshot.version < 0) {
        throw DeltaErrors.fileNotFoundException(tablePath.toString)
      } else {
        deltaLog.snapshot.schema
      }
    }
  }
}
