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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

/**
 * An identifier for a Delta table containing one of the path or the table identifier.
 */
case class DeltaTableIdentifier(
    path: Option[String] = None,
    table: Option[TableIdentifier] = None) {
  assert(path.isDefined ^ table.isDefined, "Please provide one of the path or the table identifier")

  val identifier: String = path.getOrElse(table.get.identifier)

  def database: Option[String] = table.flatMap(_.database)

  def getPath(spark: SparkSession): Path = {
    path.map(new Path(_)).getOrElse {
      new Path(spark.sessionState.catalog.getTableMetadata(table.get).location)
    }
  }

  def getDeltaLog(spark: SparkSession): DeltaLog = {
    DeltaLog.forTable(spark, getPath(spark))
  }

  /**
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier)

    if (replacedDb.isDefined) s"`${replacedDb.get}`.`$replacedId`" else s"`$replacedId`"
  }

  def unquotedString: String = {
    if (database.isDefined) s"${database.get}.$identifier" else identifier
  }

  override def toString: String = quotedString
}

/**
 * Utilities for DeltaTableIdentifier.
 */
object DeltaTableIdentifier {

  /**
   * Check the specified table identifier represents a Delta path.
   */
  def isDeltaPath(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    def tableIsTemporaryTable = catalog.isTemporaryTable(identifier)
    def databaseExists = catalog.databaseExists(identifier.database.get)
    def tableExists = catalog.tableExists(identifier)

    spark.sessionState.conf.runSQLonFile &&
      DeltaSourceUtils.isDeltaTable(identifier.database) &&
      !tableIsTemporaryTable &&
      (!databaseExists || !tableExists) &&
      new Path(identifier.table).isAbsolute
  }

  /**
   * Creates a [[DeltaTableIdentifier]] if the specified table identifier represents a Delta table,
   * otherwise returns [[None]].
   */
  def apply(spark: SparkSession, identifier: TableIdentifier): Option[DeltaTableIdentifier] = {
    if (isDeltaPath(spark, identifier)) {
      Some(DeltaTableIdentifier(path = Option(identifier.table)))
    } else if (DeltaTableUtils.isDeltaTable(spark, identifier)) {
      Some(DeltaTableIdentifier(table = Option(identifier)))
    } else {
      None
    }
  }
}
