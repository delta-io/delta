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

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.RunnableCommand

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

/**
 * A command for describing the details of a table such as the format, name, and size.
 */
abstract class DescribeDeltaDetailCommandBase(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier]) extends RunnableCommand with DeltaLogging {

  private val encoder = ExpressionEncoder[TableDetail]()

  private val rowEncoder = RowEncoder(encoder.schema).resolveAndBind()

  override val output: Seq[Attribute] = encoder.schema.toAttributes

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
   */
  protected def getPathAndTableMetadata(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier]): (Path, Option[CatalogTable])

  private def describeNonDeltaTable(table: CatalogTable): Seq[Row] = {
    Seq(rowEncoder.fromRow(encoder.toRow(
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
      ))))
  }

  private def describeNonDeltaPath(path: String): Seq[Row] = {
    Seq(rowEncoder.fromRow(encoder.toRow(
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
        null))))
  }

  private def describeDeltaTable(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      tableMetadata: Option[CatalogTable]): Seq[Row] = {
    val currentVersionPath = FileNames.deltaFile(deltaLog.logPath, snapshot.version)
    val fs = currentVersionPath.getFileSystem(sparkSession.sessionState.newHadoopConf)
    val tableName = tableMetadata.map(_.qualifiedName).getOrElse(snapshot.metadata.name)
    Seq(rowEncoder.fromRow(encoder.toRow(
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
        snapshot.protocol.minWriterVersion))))
  }
}
