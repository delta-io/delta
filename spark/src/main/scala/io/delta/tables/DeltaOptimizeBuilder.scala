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

package io.delta.tables

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeltaOptimizeContext
import org.apache.spark.sql.delta.commands.OptimizeTableCommand
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.annotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedAttribute}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

/**
 * Builder class for constructing OPTIMIZE command and executing.
 *
 * @param sparkSession SparkSession to use for execution
 * @param tableIdentifier Id of the table on which to
 *        execute the optimize
 * @param options Hadoop file system options for read and write.
 * @since 2.0.0
 */
class DeltaOptimizeBuilder private(table: DeltaTableV2) extends AnalysisHelper {
  private var partitionFilter: Seq[String] = Seq.empty

  private lazy val tableIdentifier: String =
    table.tableIdentifier.getOrElse(s"delta.`${table.deltaLog.dataPath.toString}`")

  /**
   * Apply partition filter on this optimize command builder to limit
   * the operation on selected partitions.
   * @param partitionFilter The partition filter to apply
   * @return [[DeltaOptimizeBuilder]] with partition filter applied
   * @since 2.0.0
   */
  def where(partitionFilter: String): DeltaOptimizeBuilder = {
    this.partitionFilter = this.partitionFilter :+ partitionFilter
    this
  }

  /**
   * Compact the small files in selected partitions.
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.0.0
   */
  def executeCompaction(): DataFrame = {
    execute(Seq.empty)
  }

   /**
   * Z-Order the data in selected partitions using the given columns.
   * @param columns Zero or more columns to order the data
   *                using Z-Order curves
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def executeZOrderBy(columns: String *): DataFrame = {
    val attrs = columns.map(c => UnresolvedAttribute(c))
    execute(attrs)
  }

  private def execute(zOrderBy: Seq[UnresolvedAttribute]): DataFrame = {
    val sparkSession = table.spark
    withActiveSession(sparkSession) {
      val tableId: TableIdentifier = sparkSession
        .sessionState
        .sqlParser
        .parseTableIdentifier(tableIdentifier)
      val id = Identifier.of(tableId.database.toArray, tableId.identifier)
      val catalogPlugin = sparkSession.sessionState.catalogManager.currentCatalog
      val catalog = catalogPlugin match {
        case tableCatalog: TableCatalog => tableCatalog
        case _ => throw new IllegalArgumentException(
          s"Catalog ${catalogPlugin.name} does not support tables")
      }
      val resolvedTable = ResolvedTable.create(catalog, id, table)
      val optimize = OptimizeTableCommand(
        resolvedTable, partitionFilter, DeltaOptimizeContext())(zOrderBy = zOrderBy)
      toDataset(sparkSession, optimize)
    }
  }
}

private[delta] object DeltaOptimizeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(table: DeltaTableV2): DeltaOptimizeBuilder =
    new DeltaOptimizeBuilder(table)
}
