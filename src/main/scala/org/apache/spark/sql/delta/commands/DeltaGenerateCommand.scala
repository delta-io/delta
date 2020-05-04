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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.RunnableCommand

trait DeltaGenerateCommandBase extends RunnableCommand {

  protected def getPath(spark: SparkSession, tableId: TableIdentifier): Path = {
    DeltaTableIdentifier(spark, tableId) match {
      case Some(id) if id.path.isDefined => new Path(id.path.get)
      case Some(id) =>
        throw DeltaErrors.tableNotSupportedException("DELTA GENERATE")
      case None =>
        // This is not a Delta table.
        val metadata = spark.sessionState.catalog.getTableMetadata(tableId)
        new Path(metadata.location)
    }
  }
}

case class DeltaGenerateCommand(modeName: String, tableId: TableIdentifier)
  extends DeltaGenerateCommandBase {

  import DeltaGenerateCommand._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!modeNameToGenerationFunc.contains(modeName)) {
      throw DeltaErrors.unsupportedGenerateModeException(modeName)
    }
    val tablePath = getPath(sparkSession, tableId)
    val deltaLog = DeltaLog.forTable(sparkSession, tablePath)
    if (deltaLog.snapshot.version < 0) {
      throw new AnalysisException(s"Delta table not found at $tablePath.")
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
