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
import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, Snapshot, UnresolvedPathOrIdentifier}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
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
    override val child: LogicalPlan,
    hadoopConf: Map[String, String])
  extends RunnableCommand
    with UnaryNode
    with DeltaLogging
    with DeltaCommand
{
  override val output: Seq[Attribute] = toAttributes(TableDetail.schema)

  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeDeltaDetailCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableMetadata = getTableCatalogTable(child, DescribeDeltaDetailCommand.CMD_NAME)
    val (_, path) = getTablePathOrIdentifier(child, DescribeDeltaDetailCommand.CMD_NAME)
    val basePath = tableMetadata match {
      case Some(metadata) => new Path(metadata.location)
      case _ if path.isDefined => new Path(path.get)
      case _ =>
        throw DeltaErrors.missingTableIdentifierException(DescribeDeltaDetailCommand.CMD_NAME)
    }
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

object DescribeDeltaDetailCommand {
  val CMD_NAME = "DESCRIBE DETAIL"
  def apply(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    hadoopConf: Map[String, String]
  ): DescribeDeltaDetailCommand = {
    val plan = UnresolvedPathOrIdentifier(
      path,
      tableIdentifier,
      CMD_NAME
    )
    DescribeDeltaDetailCommand(plan, hadoopConf)
  }
}
