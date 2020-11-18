/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
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
 * TODO(burak): Get rid of these utilities. DeltaCatalog should be the skinny-waist for figuring
 * these things out.
 */
object DeltaTableIdentifier extends Logging {

  /**
   * Check the specified table identifier represents a Delta path.
   */
  def isDeltaPath(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    def tableIsTemporaryTable = catalog.isTemporaryTable(identifier)
    def tableExists: Boolean = {
      try {
        catalog.databaseExists(identifier.database.get) && catalog.tableExists(identifier)
      } catch {
        case e: AnalysisException if gluePermissionError(e) =>
          logWarning("Received an access denied error from Glue. Will check to see if this " +
            s"identifier ($identifier) is path based.", e)
          false
      }
    }

    spark.sessionState.conf.runSQLonFile &&
      DeltaSourceUtils.isDeltaTable(identifier.database) &&
      !tableIsTemporaryTable &&
      !tableExists &&
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

  /**
   * When users try to access Delta tables by path, e.g. delta.`/some/path`, we need to first check
   * if such a table exists in the MetaStore (due to Spark semantics :/). The Glue MetaStore may
   * return Access Denied errors during this check. This method matches on this failure mode.
   */
  def gluePermissionError(e: AnalysisException): Boolean = e.getCause match {
    case h: Exception if h.getClass.getName == "org.apache.hadoop.hive.ql.metadata.HiveException" =>
      Seq("AWSGlue", "AccessDeniedException").forall { kw =>
        h.getMessage.contains(kw)
      }
    case _ => false
  }
}
