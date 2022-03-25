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


import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaTableIdentifier, Snapshot}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 *
 */
case class ShowDeltaPartitionsCommand(
     path: Option[String],
     tableIdentifier: Option[TableIdentifier],
     partitionKeys: Option[Map[String, String]]) extends LeafRunnableCommand with DeltaCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val basePath =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = spark.sessionState.catalog
        lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)

        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.nonEmpty =>
            new Path(id.path.get)
          case Some(id) if id.table.nonEmpty =>
            new Path(metadata.location)
          case _ =>
            if (metadata.tableType == CatalogTableType.VIEW) {
              throw DeltaErrors.viewNotSupported("SHOW PARTITIONS")
            }
            throw DeltaErrors.notADeltaTableException("SHOW PARTITIONS")
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException("SHOW PARTITIONS")
      }

    val deltaLog = DeltaLog.forTable(spark, basePath)
    recordDeltaOperation(deltaLog, "delta.ddl.showPartitions") {
      import spark.implicits._

      deltaLog.snapshot.metadata.partitionColumns.toDF().collect()
    }
  }
}
