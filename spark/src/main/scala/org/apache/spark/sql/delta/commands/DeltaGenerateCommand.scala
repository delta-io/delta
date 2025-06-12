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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, UnresolvedDeltaPathOrIdentifier}
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.RunnableCommand

case class DeltaGenerateCommand(override val child: LogicalPlan, modeName: String)
  extends RunnableCommand
  with UnaryNode
  with DeltaCommand {

  import DeltaGenerateCommand._

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!modeNameToGenerationFunc.contains(modeName)) {
      throw DeltaErrors.unsupportedGenerateModeException(modeName)
    }
    val generationFunc = modeNameToGenerationFunc(modeName)
    val table = getDeltaTable(child, COMMAND_NAME)
    generationFunc(sparkSession, table.deltaLog, table.catalogTable)
    Seq.empty
  }
}

object DeltaGenerateCommand {
  val modeNameToGenerationFunc
      : CaseInsensitiveMap[(SparkSession, DeltaLog, Option[CatalogTable]) => Unit] =
    CaseInsensitiveMap(
      Map[String, (SparkSession, DeltaLog, Option[CatalogTable]) => Unit](
        "symlink_format_manifest" -> GenerateSymlinkManifest.generateFullManifest
      )
    )

  val COMMAND_NAME = "GENERATE"

  def apply(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    modeName: String,
    options: Map[String, String]
  ): DeltaGenerateCommand = {
    // Exactly one of path or tableIdentifier should be specified
    val plan = UnresolvedDeltaPathOrIdentifier(
      path.filter(_ => tableIdentifier.isEmpty),
      tableIdentifier,
      options,
      COMMAND_NAME)
    DeltaGenerateCommand(plan, modeName)
  }
}
