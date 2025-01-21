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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
// scalastyle:on import.ordering.noEmptyLine

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
      val metadata = spark.sessionState.catalog.getTableMetadata(table.get)
      new Path(metadata.location)
    }
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
object DeltaTableIdentifier extends DeltaLogging {

  /**
   * Check the specified table identifier represents a Delta path.
   */
  def isDeltaPath(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    def tableIsTemporaryTable = catalog.isTempView(identifier)
    def tableExists: Boolean = {
      try {
        catalog.databaseExists(identifier.database.get) && catalog.tableExists(identifier)
      } catch {
        case e: AnalysisException if gluePermissionError(e) =>
          logWarning(log"Received an access denied error from Glue. Will check to see if this " +
            log"identifier (${MDC(DeltaLogKeys.TABLE_NAME, identifier)}) is path based.", e)
          false
      }
    }

    spark.sessionState.conf.runSQLonFile &&
      new Path(identifier.table).isAbsolute &&
      DeltaSourceUtils.isDeltaTable(identifier.database) &&
      !tableIsTemporaryTable &&
      !tableExists
  }

  /**
   * Creates a [[DeltaTableIdentifier]] if the specified table identifier represents a Delta table,
   * otherwise returns [[None]].
   */
  def apply(spark: SparkSession, identifier: TableIdentifier)
      : Option[DeltaTableIdentifier] = recordFrameProfile(
          "DeltaAnalysis", "DeltaTableIdentifier.resolve") {
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
