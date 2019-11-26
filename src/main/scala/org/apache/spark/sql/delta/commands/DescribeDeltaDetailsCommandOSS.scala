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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

case class DescribeDeltaDetailCommandOSS(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier])
  extends DescribeDeltaDetailCommandBase(path, tableIdentifier) {

  /**
   * If `path` is set, return it and an empty `CatalogTable` since it's a physical path. If
   * `tableIdentifier` is set, we will try to see if it's a Delta data source path (such as
   * `delta.<a table path>`). If so, we will return the path and an empty `CatalogTable`. Otherwise,
   * we will use `SessionCatalog` to resolve `tableIdentifier`.
   */
  override protected def getPathAndTableMetadata(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier]): (Path, Option[CatalogTable]) = {
    path.map(new Path(_) -> None).orElse {
      tableIdentifier.map { i =>
        DeltaTableIdentifier(spark, tableIdentifier.get) match {
          case Some(id) if id.path.isDefined => new Path(id.path.get) -> None
          case Some(id) =>
            throw DeltaErrors.tableNotSupportedException("DESCRIBE DETAIL")
          case None =>
            // This is not a Delta table.
            val metadata = spark.sessionState.catalog.getTableMetadata(i)
            new Path(metadata.location) -> Some(metadata)
        }
      }
    }.getOrElse {
      throw DeltaErrors.missingTableIdentifierException("DESCRIBE DETAIL")
    }
  }
}
