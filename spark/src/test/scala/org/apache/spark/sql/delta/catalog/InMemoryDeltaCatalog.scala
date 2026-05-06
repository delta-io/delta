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
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableChange}
import org.apache.spark.sql.connector.expressions.Transform

/**
 * Test-only catalog that extends [[DeltaCatalog]] and overrides [[loadCatalogTable]]
 * to return [[InMemorySparkTable]].
 */
class InMemoryDeltaCatalog extends DeltaCatalog {
  override def dropTable(ident: Identifier): Boolean = {
    val removedInMemory = InMemoryDeltaCatalog.dropTable(ident)
    val removedUnderlying = super.dropTable(ident)
    removedInMemory || removedUnderlying
  }

  override def loadCatalogTable(ident: Identifier, catalogTable: CatalogTable): Table =
    InMemoryDeltaCatalog.getOrCreateTable(ident, catalogTable, spark)

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    loadTable(ident) match {
      case table: InMemorySparkTable =>
        InMemoryDeltaCatalog.applySchemaChanges(table, ident, changes.toSeq)
        // Also propagate the change to the underlying catalog so the Delta table on disk stays in
        // sync with the in-memory test stand-in.
        super.alterTable(ident, changes: _*)
      case _ => super.alterTable(ident, changes: _*)
    }
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

  def isTestTable(table: Table): Boolean = table.isInstanceOf[InMemorySparkTable]

  /** Applies schema changes to a test table and returns the updated table. */
  def applySchemaChanges(table: Table, ident: Identifier, changes: Seq[TableChange]): Table = {
    val newSchema = CatalogV2Util.applySchemaChanges(
      table.schema(), changes, tableProvider = None, statementType = "ALTER TABLE")

    val javaProps = new java.util.HashMap[String, String](table.properties)
    // Create the new table with the evolved schema and migrate existing data.
    val newTable = table match {
      case t: InMemorySparkTable =>
        val newTable = new InMemorySparkTable(
          ident.name(), newSchema, t.partitioning, javaProps)
        InMemorySparkTableShims.migrateData(newTable, t.data, newSchema)
        newTable
      case _ => throw new IllegalArgumentException(
        s"Expected InMemorySparkTable but got ${table.getClass.getName}")
    }
    tables.put(ident.name(), newTable)
    newTable
  }

  /**
   * Remove a table defined by [[ident]] from the table list.
   * Returns true if there was a table and it was removed, false otherwise.
   */
  def dropTable(ident: Identifier): Boolean =
    tables.remove(ident.name()) != null

  /**
   * Check whether table [[name]] exists here.
   */
  def contains(name: String): Boolean = tables.containsKey(name)

  /**
   * Reset the catalog, removing all created tables from the storage.
   */
  def reset(): Unit = tables.clear()
}
