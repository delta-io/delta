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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Replaces [[UnresolvedTable]]s if the plan is for direct query on files.
 */
case class ResolveDeltaPathTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  import ResolveDeltaPathTable._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {

    // Attempts to resolve a path-based Delta table, returning ResolvedPathBasedNonDeltaTable if
    // unsuccessful. This is needed as DESCRIBE DETAIL supports both Delta and non-Delta paths.
    case u: UnresolvedPathBasedTable =>
      resolveAsPathTable(sparkSession, multipartIdentifier(u.path)).getOrElse {
        // Resolve it as a placeholder, to identify it as a non-Delta table.
        ResolvedPathBasedNonDeltaTable(u.path, u.commandName)
      }

    // Resolves a known path-based Delta table as a Table.
    case u: UnresolvedPathBasedDeltaTable =>
      resolveAsPathTable(sparkSession, multipartIdentifier(u.path)).getOrElse {
        throw DeltaErrors.notADeltaTableException(u.commandName, u.deltaTableIdentifier)
      }

    // Resolves a known path-based Delta table as a Relation.
    case u: UnresolvedPathBasedDeltaTableRelation =>
      resolveAsPathTableRelation(sparkSession, multipartIdentifier(u.path), u.options).getOrElse {
        throw DeltaErrors.notADeltaTableException(u.deltaTableIdentifier)
      }

    // Resolves delta.`/path/to/table` as a path-based Delta table, now that catalog lookup failed.
    case u: UnresolvedTable if maybeSQLFile(sparkSession, u.multipartIdentifier) =>
      resolveAsPathTable(sparkSession, u.multipartIdentifier).getOrElse(u)
  }

  // Helper that converts a path into the multipartIdentifier for a path-based Delta table
  private def multipartIdentifier(path: String): Seq[String] = Seq(DeltaSourceUtils.ALT_NAME, path)
}

object ResolveDeltaPathTable {

  /** Adapted from spark's [[ResolveSQLOnFile#maybeSQLFile]] */
  def maybeSQLFile(sparkSession: SparkSession, multipartIdentifier: Seq[String]): Boolean = {
    sparkSession.sessionState.conf.runSQLonFile && multipartIdentifier.size == 2
  }

  /** Convenience wrapper for UnresolvedRelation */
  private[delta] def resolveAsPathTableRelation(
      sparkSession: SparkSession,
      u: UnresolvedRelation) : Option[DataSourceV2Relation] = {
    resolveAsPathTableRelation(sparkSession, u.multipartIdentifier, u.options)
  }

  /**
   * Try resolving the input table as a Path table.
   * If the path table exists, return a [[DataSourceV2Relation]] instance. Otherwise, return None.
   */
  private[delta] def resolveAsPathTableRelation(
      sparkSession: SparkSession,
      multipartIdentifier: Seq[String],
      options: CaseInsensitiveStringMap) : Option[DataSourceV2Relation] = {
    // NOTE: [[ResolvedTable]] always provides a [[TableCatalog]], even for path-based tables, but
    // we ignore it here because [[ResolvedRelation]] for Delta tables do not specify a catalog.
    resolveAsPathTable(sparkSession, multipartIdentifier, options.asScala.toMap).map { resolved =>
      DataSourceV2Relation.create(resolved.table, None, Some(resolved.identifier), options)
    }
  }

  /**
   * Try resolving the input table as a Path table.
   * If the path table exists, return a [[ResolvedTable]] instance. Otherwise, return None.
   */
  private[delta] def resolveAsPathTable(
      sparkSession: SparkSession,
      multipartIdentifier: Seq[String],
      options: Map[String, String] = Map.empty): Option[ResolvedTable] = {
    val tableId = multipartIdentifier.asTableIdentifier
    if (DeltaTableUtils.isValidPath(tableId)) {
      val deltaTableV2 = DeltaTableV2(sparkSession, new Path(tableId.table), options = options)
      if (deltaTableV2.tableExists) {
        val identifier = multipartIdentifier.asIdentifier
        val catalog = sparkSession.sessionState.catalogManager.v2SessionCatalog.asTableCatalog
        return Some(ResolvedTable.create(catalog, identifier, deltaTableV2))
      }
    }
    None // Not a Delta table
  }
}
