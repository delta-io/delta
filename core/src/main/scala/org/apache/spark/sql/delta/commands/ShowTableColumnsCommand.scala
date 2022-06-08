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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.commands.TableColumns.toRow
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

/**
 * The result returned by the `SHOW COLUMNS` command.
 */
case class TableColumns(
     columnName: String)

object TableColumns {
  val schema: StructType = ScalaReflection.schemaFor[TableColumns].dataType.asInstanceOf[StructType]

  private lazy val converter: TableColumns => Row = {
    val toInternalRow = CatalystTypeConverters.createToCatalystConverter(schema)
    val toExternalRow = CatalystTypeConverters.createToScalaConverter(schema)
    toInternalRow.andThen(toExternalRow).asInstanceOf[TableColumns => Row]
  }

  def toRow(table: TableColumns): Row = converter(table)
}

/**
 * A command listing all column names of a table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
 * }}}
 *
 * @param tableName the table name
 * @param path the file path where the table located
 * @param schemaName the schema name / database name of the table
 */
case class ShowTableColumnsCommand(
    tableName: Option[TableIdentifier],
    path: Option[String],
    schemaName: Option[String],
    override val output: Seq[Attribute] = ExpressionEncoder[TableColumns]().schema.toAttributes)
  extends LeafRunnableCommand with DeltaCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (basePath, tableMetadata) = getPathAndTableMetadata(
      sparkSession, path, tableName, schemaName)
    val deltaLog = DeltaLog.forTable(sparkSession, basePath)
    recordDeltaOperation(deltaLog, "delta.ddl.showColumns") {
      val snapshot = deltaLog.snapshot
      if (snapshot.version == -1) {
        // No available snapshot
        if (path.isEmpty) {
          // Non-delta table without path, get column names by metadata
          getColumnsFromSchema(tableMetadata.get.schema)
        } else {
          val fs = new Path(path.get).getFileSystem(deltaLog.newDeltaHadoopConf())
          // Throw FileNotFoundException when the path doesn't exist since there may be a typo
          if (!fs.exists(new Path(path.get))) {
            throw DeltaErrors.fileNotFoundException(path.get)
          }
          getColumnsFromSchema(sparkSession.read.format("parquet").load(path.get).schema)
        }
      } else {
        getColumnsFromSchema(snapshot.schema)
      }
    }
  }

  private def getColumnsFromSchema(schema: StructType): Seq[Row] = {
    schema.fieldNames.map { x =>
      toRow(TableColumns(x))
    }.toSeq
  }

  /**
   * Resolve `path` and `tableIdentifier` to get the underlying storage path, and its `CatalogTable`
   * if it's a table. The caller will make sure either `path` or `tableIdentifier` is set but not
   * both.
   *
   * If `path` is set, return it and an empty `CatalogTable` since it's a physical path. If
   * `tableIdentifier` is set, we will try to see if it's a Delta data source path (such as
   * `delta.<a table path>`). If so, we will return the path and an empty `CatalogTable`. Otherwise,
   * we will use `SessionCatalog` to resolve `tableIdentifier`.
   */
  protected def getPathAndTableMetadata(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      schemaName: Option[String]): (Path, Option[CatalogTable]) = {
    val lookupTable = schemaName match {
      case None => tableIdentifier
      case Some(db) => Option(TableIdentifier(tableIdentifier.get.identifier, Some(db)))
    }
    path.map(new Path(_) -> None).orElse {
      lookupTable.map { i =>
        DeltaTableIdentifier(spark, i) match {
          case Some(id) if id.path.isDefined => new Path(id.path.get) -> None
          case _ =>
            // This should be a catalog table.
            try {
              val metadata = spark.sessionState.catalog.getTableMetadata(i)
              if (metadata.tableType == CatalogTableType.VIEW) {
                throw DeltaErrors.viewInShowColumnsException(i)
              }
              new Path(metadata.location) -> Some(metadata)
            } catch {
              // Better error message if the user tried to SHOW COLUMNS a temp view.
              case _: NoSuchTableException | _: NoSuchDatabaseException
                if spark.sessionState.catalog.getTempView(i.table).isDefined =>
                throw DeltaErrors.viewInShowColumnsException(i)

              // Better error message if an existing database not documented in catalog table.
              // If only the table name is wrong, the thrown `NoSuchDatabaseException` would be
              // misleading in this case.
              case _: NoSuchTableException | _: NoSuchDatabaseException =>
                throw DeltaErrors.tableIdentifierNotFoundInShowColumnsException(i)
            }
        }
      }
    }.getOrElse {
      throw DeltaErrors.missingTableIdentifierException("SHOW COLUMNS")
    }
  }
}
