/*
 * Copyright (2020) The Delta Lake Project Authors.
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

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StructType

/** The result returned by the `describe detail` command. */
case class TableDetail(
    format: String,
    id: String,
    name: String,
    description: String,
    location: String,
    createdAt: Timestamp,
    lastModified: Timestamp,
    partitionColumns: Seq[String],
    numFiles: java.lang.Long,
    sizeInBytes: java.lang.Long,
    properties: Map[String, String],
    minReaderVersion: java.lang.Integer,
    minWriterVersion: java.lang.Integer)

object TableDetail {
  val schema = ScalaReflection.schemaFor[TableDetail].dataType.asInstanceOf[StructType]

  private lazy val converter: TableDetail => Row = {
    val toInternalRow = CatalystTypeConverters.createToCatalystConverter(schema)
    val toExternalRow = CatalystTypeConverters.createToScalaConverter(schema)
    toInternalRow.andThen(toExternalRow).asInstanceOf[TableDetail => Row]
  }

  def toRow(table: TableDetail): Row = converter(table)
}

/**
 * A command for describing the details of a table such as the format, name, and size.
 */
case class DescribeDeltaDetailCommand(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier]) extends RunnableCommand with DeltaLogging {

  override val output: Seq[Attribute] = TableDetail.schema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (basePath, tableMetadata) = getPathAndTableMetadata(sparkSession, path, tableIdentifier)

    val deltaLog = DeltaLog.forTable(sparkSession, basePath)
    recordDeltaOperation(deltaLog, "delta.ddl.describeDetails") {
      val snapshot = deltaLog.snapshot
      if (snapshot.version == -1) {
        if (path.nonEmpty) {
          val fs = new Path(path.get).getFileSystem(sparkSession.sessionState.newHadoopConf())
          // Throw FileNotFoundException when the path doesn't exist since there may be a typo
          if (!fs.exists(new Path(path.get))) {
            throw new FileNotFoundException(path.get)
          }
          describeNonDeltaPath(path.get)
        } else {
          describeNonDeltaTable(tableMetadata.get)
        }
      } else {
        describeDeltaTable(sparkSession, deltaLog, snapshot, tableMetadata)
      }
    }
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
              case _: NoSuchTableException
                  if spark.sessionState.catalog.getTempView(i.table).isDefined =>
                throw DeltaErrors.viewInDescribeDetailException(i)
            }
        }
      }
    }.getOrElse {
      throw DeltaErrors.missingTableIdentifierException("DESCRIBE DETAIL")
    }
  }

  private def toRows(detail: TableDetail): Seq[Row] = TableDetail.toRow(detail) :: Nil

  private def describeNonDeltaTable(table: CatalogTable): Seq[Row] = {
    toRows(
      TableDetail(
        table.provider.orNull,
        null,
        table.qualifiedName,
        table.comment.getOrElse(""),
        table.storage.locationUri.map(new Path(_).toString).orNull,
        new Timestamp(table.createTime),
        null,
        table.partitionColumnNames,
        null,
        null,
        table.properties,
        null,
        null
      ))
  }

  private def describeNonDeltaPath(path: String): Seq[Row] = {
    toRows(
      TableDetail(
        null,
        null,
        null,
        null,
        path,
        null,
        null,
        null,
        null,
        null,
        Map.empty,
        null,
        null))
  }

  private def describeDeltaTable(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      tableMetadata: Option[CatalogTable]): Seq[Row] = {
    val currentVersionPath = FileNames.deltaFile(deltaLog.logPath, snapshot.version)
    val fs = currentVersionPath.getFileSystem(sparkSession.sessionState.newHadoopConf)
    val tableName = tableMetadata.map(_.qualifiedName).getOrElse(snapshot.metadata.name)
    toRows(
      TableDetail(
        "delta",
        snapshot.metadata.id,
        tableName,
        snapshot.metadata.description,
        deltaLog.dataPath.toString,
        snapshot.metadata.createdTime.map(new Timestamp(_)).orNull,
        new Timestamp(fs.getFileStatus(currentVersionPath).getModificationTime),
        snapshot.metadata.partitionColumns,
        snapshot.numOfFiles,
        snapshot.sizeInBytes,
        snapshot.metadata.configuration,
        snapshot.protocol.minReaderVersion,
        snapshot.protocol.minWriterVersion))
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
