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

package org.apache.spark.sql.delta.catalog

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform

/**
 * Test-only catalog that extends [[DeltaCatalog]] and overrides [[loadCatalogTable]]
 * to return [[InMemorySparkTable]].
 */
class InMemoryDeltaCatalog extends DeltaCatalog {
  override def loadCatalogTable(ident: Identifier, catalogTable: CatalogTable): Table = {
    InMemoryDeltaCatalog.getOrCreateTable(ident, catalogTable, spark)
  }
}

object InMemoryDeltaCatalog {

  /**
   * The actual tables.
   * Maps a table name -> [[InMemorySparkTable]].
   */
  private val tables = new ConcurrentHashMap[String, InMemorySparkTable]()

  /**
   * Get or create a table defined by [[ident]].
   * [[catalogTable]] and [[spark]] are used to discover the table schema.
   *
   * NB: Ignores `ident.namespace()`, instead using `ident.name()` as the key.
   */
  def getOrCreateTable(
      ident: Identifier,
      catalogTable: CatalogTable,
      spark: SparkSession): InMemorySparkTable = {
    val tableName = ident.name()
    tables.computeIfAbsent(tableName, _ => {
      val deltaTable = DeltaTableV2(
        spark, new Path(catalogTable.location), catalogTable = Some(catalogTable))
      val props = new java.util.HashMap[String, String](catalogTable.properties.asJava)
      new InMemorySparkTable(
        tableName,
        deltaTable.schema(),
        Array.empty[Transform],
        props)
    })
  }

  /**
   * Reset the catalog, removing all created tables from the storage.
   */
  def reset(): Unit = tables.clear()
}
