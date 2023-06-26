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

package io.delta.tables.execution

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableImplicits._
import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier, UnresolvedDeltaPathOrIdentifier}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * The `vacuum` command implementation for Spark SQL. Example SQL:
 * {{{
 *    VACUUM ('/path/to/dir' | delta.`/path/to/dir`)
 *    [USING INVENTORY (delta.`/path/to/dir`| ( sub_query ))]
 *    [RETAIN number HOURS] [DRY RUN];
 * }}}
 */
case class VacuumTableCommand(
    override val child: LogicalPlan,
    horizonHours: Option[Double],
    inventoryTable: Option[LogicalPlan],
    inventoryQuery: Option[String],
    dryRun: Boolean) extends RunnableCommand with UnaryNode with DeltaCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("path", StringType, nullable = true)())

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaTable = getDeltaTable(child, "VACUUM")
    // The VACUUM command is only supported on existing delta tables. If the target table doesn't
    // exist or it is based on a partition directory, an exception will be thrown.
    if (!deltaTable.tableExists || deltaTable.hasPartitionFilters) {
      throw DeltaErrors.notADeltaTableException(
        "VACUUM",
        DeltaTableIdentifier(path = Some(deltaTable.path.toString)))
    }
    val inventory = inventoryTable.map(sparkSession.sessionState.analyzer.execute)
        .map(p => Some(getDeltaTable(p, "VACUUM").toDf(sparkSession)))
        .getOrElse(inventoryQuery.map(sparkSession.sql))
    VacuumCommand.gc(sparkSession, deltaTable.deltaLog, dryRun, horizonHours,
      inventory).collect()
  }
}

object VacuumTableCommand {
  def apply(
      path: Option[String],
      table: Option[TableIdentifier],
      inventoryTable: Option[TableIdentifier],
      inventoryQuery: Option[String],
      horizonHours: Option[Double],
      dryRun: Boolean): VacuumTableCommand = {
    val child = UnresolvedDeltaPathOrIdentifier(path, table, "VACUUM")
    val unresolvedInventoryTable = inventoryTable.map(rt => UnresolvedTable(rt.nameParts, "VACUUM"))
    VacuumTableCommand(child, horizonHours, unresolvedInventoryTable, inventoryQuery, dryRun)
  }
}
