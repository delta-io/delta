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

package io.delta.tables.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * The `vacuum` command implementation for Spark SQL. Example SQL:
 * {{{
 *    VACUUM ('/path/to/dir' | delta.`/path/to/dir`) [RETAIN number HOURS] [DRY RUN];
 * }}}
 */
case class VacuumTableCommand(
    path: Option[String],
    table: Option[TableIdentifier],
    horizonHours: Option[Double],
    dryRun: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("path", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val pathToVacuum =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (table.nonEmpty) {
        DeltaTableIdentifier(sparkSession, table.get) match {
          case Some(id) if id.path.nonEmpty =>
            new Path(id.path.get)
          case _ =>
            new Path(sparkSession.sessionState.catalog.getTableMetadata(table.get).location)
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException("VACUUM")
      }
    val baseDeltaPath = DeltaTableUtils.findDeltaTableRoot(sparkSession, pathToVacuum)
    if (baseDeltaPath.isDefined) {
      if (baseDeltaPath.get != pathToVacuum) {
        throw DeltaErrors.vacuumBasePathMissingException(baseDeltaPath.get)
      }
    }
    val deltaLog = DeltaLog.forTable(sparkSession, pathToVacuum)
    if (!deltaLog.tableExists) {
      throw DeltaErrors.notADeltaTableException(
        "VACUUM",
        DeltaTableIdentifier(path = Some(pathToVacuum.toString)))
    }
    VacuumCommand.gc(sparkSession, deltaLog, dryRun, horizonHours).collect()
  }
}
