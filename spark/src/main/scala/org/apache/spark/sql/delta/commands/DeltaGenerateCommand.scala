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
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.LeafRunnableCommand

case class DeltaGenerateCommand(
    modeName: String,
    tableId: TableIdentifier,
    options: Map[String, String])
  extends LeafRunnableCommand {

  import DeltaGenerateCommand._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!modeNameToGenerationFunc.contains(modeName)) {
      throw DeltaErrors.unsupportedGenerateModeException(modeName)
    }

    val tablePath = DeltaTableIdentifier(sparkSession, tableId) match {
      case Some(id) if id.path.isDefined =>
        new Path(id.path.get)
      case _ =>
        new Path(sparkSession.sessionState.catalog.getTableMetadata(tableId).location)
    }

    val deltaLog = DeltaLog.forTable(sparkSession, tablePath, options)
    if (!deltaLog.tableExists) {
      throw DeltaErrors.notADeltaTableException("GENERATE")
    }
    val generationFunc = modeNameToGenerationFunc(modeName)
    generationFunc(sparkSession, deltaLog)
    Seq.empty
  }
}

object DeltaGenerateCommand {
  val modeNameToGenerationFunc = CaseInsensitiveMap(
    Map[String, (SparkSession, DeltaLog) => Unit](
    "symlink_format_manifest" -> GenerateSymlinkManifest.generateFullManifest
  ))
}
