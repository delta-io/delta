/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.sql

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalog

import scala.collection.JavaConverters._

/**
 * Delta catalog implementation that returns HybridDeltaTable for all Delta tables.
 *
 * This catalog extends AbstractDeltaCatalog and overrides loadTable to return
 * HybridDeltaTable instances instead of DeltaTableV2. The HybridDeltaTable can
 * then be wrapped by the analyzer rule with context to indicate V1 or V2 usage.
 */
class DeltaHybridCatalog extends AbstractDeltaCatalog {

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if DeltaTableUtils.isDeltaTable(v1.catalogTable) =>
          // Return HybridDeltaTable instead of DeltaTableV2
          new HybridDeltaTable(
            spark = SparkSession.active,
            identifier = ident,
            tablePath = v1.catalogTable.location.toString,
            options = v1.catalogTable.storage.properties
          )
        case o => o
      }
    } catch {
      case e @ (_: org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException |
                _: org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException |
                _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException) =>
        if (isPathIdentifier(ident)) {
          newDeltaPathTableHybrid(ident)
        } else {
          throw e
        }
    }
  }

  /**
   * Creates a HybridDeltaTable for path-based identifiers.
   */
  protected def newDeltaPathTableHybrid(ident: Identifier): HybridDeltaTable = {
    val path = new Path(ident.name())
    new HybridDeltaTable(
      spark = SparkSession.active,
      identifier = ident,
      tablePath = path.toString,
      options = Map.empty[String, String]
    )
  }
}

