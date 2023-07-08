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

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.LeafRunnableCommand
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
    minWriterVersion: java.lang.Integer,
    tableFeatures: Seq[String]
    )

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
    tableIdentifier: Option[TableIdentifier],
    hadoopConf: Map[String, String]) extends LeafRunnableCommand with DeltaLogging {

  override val output: Seq[Attribute] = TableDetail.schema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (basePath, tableMetadata) = getPathAndTableMetadata(sparkSession, path, tableIdentifier)

    val deltaLog = DeltaLog.forTable(sparkSession, basePath, hadoopConf)
    recordDeltaOperation(deltaLog, "delta.ddl.describeDetails") {
      val snapshot = deltaLog.update()
      if (snapshot.version == -1) {
        if (path.nonEmpty) {
          val fs = new Path(path.get).getFileSystem(deltaLog.newDeltaHadoopConf())
          // Throw FileNotFoundException when the path doesn't exist since there may be a typo
          if (!fs.exists(new Path(path.get))) {
            throw DeltaErrors.fileNotFoundException(path.get)
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
   * if it's a table.
   *
   * If `tableIdentifier` is set and it is not a Delta data source path (such as `delta.<path>`)
   * we will resolve the identifier using the `SessionCatalog`. Otherwise, we will return the path
   * and empty catalog table.
   */
  protected def getPathAndTableMetadata(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier]): (Path, Option[CatalogTable]) = {
    tableIdentifier.map { i =>
      DeltaTableIdentifier(spark, tableIdentifier.get) match {
        case Some(id) if id.path.isDefined => new Path(id.path.get) -> None
        case _ =>
          // This should be a catalog table.
          try {
            val metadata = spark.sessionState.catalog.getTableMetadata(i)
            val isView = metadata.tableType == CatalogTableType.VIEW
            if (isView) {
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
    .orElse(path.map(p => new Path(p) -> None))
    .getOrElse {
      throw DeltaErrors.missingTableIdentifierException("DESCRIBE DETAIL")
    }
  }

  private def toRows(detail: TableDetail): Seq[Row] = TableDetail.toRow(detail) :: Nil

  private def describeNonDeltaTable(table: CatalogTable): Seq[Row] = {
    toRows(
      TableDetail(
        format = table.provider.orNull,
        id = null,
        name = table.qualifiedName,
        description = table.comment.getOrElse(""),
        location = table.storage.locationUri.map(new Path(_).toString).orNull,
        createdAt = new Timestamp(table.createTime),
        lastModified = null,
        partitionColumns = table.partitionColumnNames,
        numFiles = null,
        sizeInBytes = null,
        properties = table.properties,
        minReaderVersion = null,
        minWriterVersion = null,
        tableFeatures = null
      ))
  }

  private def describeNonDeltaPath(path: String): Seq[Row] = {
    toRows(
      TableDetail(
        format = null,
        id = null,
        name = null,
        description = null,
        location = path,
        createdAt = null,
        lastModified = null,
        partitionColumns = null,
        numFiles = null,
        sizeInBytes = null,
        properties = Map.empty,
        minReaderVersion = null,
        minWriterVersion = null,
        tableFeatures = null))
  }

  private def describeDeltaTable(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      tableMetadata: Option[CatalogTable]): Seq[Row] = {
    val currentVersionPath = FileNames.deltaFile(deltaLog.logPath, snapshot.version)
    val fs = currentVersionPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val tableName = tableMetadata.map(_.qualifiedName).getOrElse(snapshot.metadata.name)
    val featureNames = (
      snapshot.protocol.implicitlySupportedFeatures.map(_.name) ++
        snapshot.protocol.readerAndWriterFeatureNames).toSeq.sorted
    toRows(
      TableDetail(
        format = "delta",
        id = snapshot.metadata.id,
        name = tableName,
        description = snapshot.metadata.description,
        location = deltaLog.dataPath.toString,
        createdAt = snapshot.metadata.createdTime.map(new Timestamp(_)).orNull,
        lastModified = new Timestamp(fs.getFileStatus(currentVersionPath).getModificationTime),
        partitionColumns = snapshot.metadata.partitionColumns,
        numFiles = snapshot.numOfFiles,
        sizeInBytes = snapshot.sizeInBytes,
        properties = snapshot.metadata.configuration,
        minReaderVersion = snapshot.protocol.minReaderVersion,
        minWriterVersion = snapshot.protocol.minWriterVersion,
        tableFeatures = featureNames
      ))
  }
}
