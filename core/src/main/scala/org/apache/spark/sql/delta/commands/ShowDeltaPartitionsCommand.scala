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
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
 * A command for users to list the partition names of a delta table. If partition spec is specified,
 * partitions that match the spec are returned. Otherwise an empty result set is returned.
 */
case class ShowDeltaPartitionsCommand(
     path: Option[String],
     tableIdentifier: Option[TableIdentifier]
     // partitionKeys: Option[Map[String, String]]
                                     )
  extends DeltaCommandWithoutViewsSupport(path, tableIdentifier) with DeltaLogging {

  val PARTITION_VALUES = "partitionValues"

  override val output: Seq[Attribute] = getPartitionSchema.toAttributes

  override def run(spark: SparkSession): Seq[Row] = {

    val basePath = getPathAndTableMetadata(spark, "SHOW PARTITIONS")._1
    val deltaLog = DeltaLog.forTable(spark, basePath)

    recordDeltaOperation(deltaLog, "delta.ddl.showPartitions") {
      val projection = deltaLog.snapshot.metadata.partitionSchema.map{ field =>
        col(PARTITION_VALUES + "." + field.name).cast(field.dataType)
      }

      deltaLog.snapshot.allFiles
        .select(projection: _*)
        .distinct()
        .collect()
        .toSeq
    }
  }

  override def viewNotSupportedException(view: TableIdentifier): Throwable = {
    DeltaErrors.viewInShowPartitionsException(view)
  }

  protected def getPartitionSchema: StructType = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    val basePath = getPathAndTableMetadata(spark, "SHOW PARTITIONS")._1
    val deltaLog = DeltaLog.forTable(spark, basePath)
    deltaLog.snapshot.metadata.partitionSchema
  }
}

abstract class DeltaCommandWithoutViewsSupport(
        path: Option[String],
        tableIdentifier: Option[TableIdentifier]) extends LeafRunnableCommand {

  def viewNotSupportedException(view: TableIdentifier) : Throwable

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
  def getPathAndTableMetadata(
         spark: SparkSession,
         command: String): (Path, Option[CatalogTable]) = {
    path.map(new Path(_) -> None).orElse {
      tableIdentifier.map { i =>
        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.isDefined => new Path(id.path.get) -> None
          case _ =>
            // This should be a catalog table.
            try {
              val metadata = spark.sessionState.catalog.getTableMetadata(i)
              if (metadata.tableType == CatalogTableType.VIEW) {
                throw viewNotSupportedException(i)
              }
              new Path(metadata.location) -> Some(metadata)
            } catch {
              case _: NoSuchTableException | _: NoSuchDatabaseException
                if spark.sessionState.catalog.getTempView(i.table).isDefined =>
                throw viewNotSupportedException(i)
            }
        }
      }
    }.getOrElse {
      throw DeltaErrors.missingTableIdentifierException(command)
    }
  }
}
