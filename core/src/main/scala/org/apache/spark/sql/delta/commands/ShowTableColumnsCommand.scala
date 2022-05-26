package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, Snapshot}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

case class TableColumns(
     tableName: String,
     columnName: Seq[String])

object TableColumns {
  val schema = ScalaReflection.schemaFor[TableColumns].dataType.asInstanceOf[StructType]

  private lazy val converter: TableColumns => Row = {
    val toInternalRow = CatalystTypeConverters.createToCatalystConverter(schema)
    val toExternalRow = CatalystTypeConverters.createToScalaConverter(schema)
    toInternalRow.andThen(toExternalRow).asInstanceOf[TableColumns => Row]
  }

  def toRow(table: TableColumns): Row = converter(table)
}

case class ShowTableColumnsCommand(
    table: Option[TableIdentifier],
    path: Option[String],
    database: Option[TableIdentifier]) // TODO: add `database` support for the command
  extends LeafRunnableCommand with DeltaCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (basePath, tableMetadata) = getPathAndTableMetadata(sparkSession, path, table)
    val deltaLog = DeltaLog.forTable(sparkSession, basePath)
    recordDeltaOperation(deltaLog, "delta.ddl.showColumns") {
      val snapshot = deltaLog.snapshot
      if (snapshot.version == -1) {
        // There is no suitable snapshot
        showTableNameByMetaData(tableMetadata)
      } else {
        showTableColumnsBySnapshot(snapshot, tableMetadata)
      }
    }
  }

  private def toRows(detail: TableColumns): Seq[Row] = TableColumns.toRow(detail) :: Nil

  private def showTableColumnsBySnapshot(
      snapshot: Snapshot,
      tableMetadata: Option[CatalogTable]): Seq[Row] = {
    toRows(
      TableColumns(
        snapshot.metadata.name,
        snapshot.metadata.physicalPartitionColumns
      ))
  }

  private def showTableNameByMetaData(tableMetadata: Option[CatalogTable]): Seq[Row] = {
    toRows(
      TableColumns(
        tableMetadata.map(_.qualifiedName).get,
        null
      ))
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
     tableIdentifier: Option[TableIdentifier]): (Path, Option[CatalogTable]) = {
    path.map(new Path(_) -> None).orElse {
      tableIdentifier.map { i =>
        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.isDefined => new Path(id.path.get) -> None
          case _ =>
            // This should be a catalog table.
            try {
              val metadata = spark.sessionState.catalog.getTableMetadata(i)
              if (metadata.tableType == CatalogTableType.VIEW) {
                throw DeltaErrors.viewInDescribeDetailException(i)
              }
              new Path(metadata.location) -> Some(metadata)
            } catch {
              // Better error message if the user tried to DESCRIBE DETAIL a temp view.
              case _: NoSuchTableException | _: NoSuchDatabaseException
                if spark.sessionState.catalog.getTempView(i.table).isDefined =>
                throw DeltaErrors.viewInDescribeDetailException(i)
            }
        }
      }
    }.getOrElse {
      throw DeltaErrors.missingTableIdentifierException("DESCRIBE DETAIL")
    }
  }
}
