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

import scala.collection.Map

import org.apache.spark.sql.delta.{DeltaErrors, DeltaHistoryManager, DeltaLog, PreprocessTableUpdate}
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeltaGenerateCommand, VacuumCommand}
import org.apache.spark.sql.delta.util.AnalysisHelper
import io.delta.tables.DeltaTable

import org.apache.spark.sql.{functions, Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Interface to provide the actual implementations of DeltaTable operations.
 */
trait DeltaTableOperations extends AnalysisHelper { self: DeltaTable =>

  protected def executeDelete(condition: Option[Expression]): Unit = improveUnsupportedOpError {
    val delete = DeleteFromTable(self.toDF.queryExecution.analyzed, condition)
    toDataset(sparkSession, delete)
  }

  protected def executeHistory(
      deltaLog: DeltaLog,
      limit: Option[Int] = None,
      tableId: Option[TableIdentifier] = None): DataFrame = {
    val history = deltaLog.history
    val spark = self.toDF.sparkSession
    spark.createDataFrame(history.getHistory(limit))
  }

  protected def executeGenerate(tblIdentifier: String, mode: String): Unit = {
    val tableId: TableIdentifier = sparkSession
      .sessionState
      .sqlParser
      .parseTableIdentifier(tblIdentifier)
    val generate = DeltaGenerateCommand(mode, tableId)
    toDataset(sparkSession, generate)
  }

  protected def executeUpdate(
      set: Map[String, Column],
      condition: Option[Column]): Unit = improveUnsupportedOpError {
    val assignments = set.map { case (targetColName, column) =>
      Assignment(UnresolvedAttribute.quotedString(targetColName), column.expr)
    }.toSeq
    val update = UpdateTable(self.toDF.queryExecution.analyzed, assignments, condition.map(_.expr))
    toDataset(sparkSession, update)
  }

  protected def executeVacuum(
      deltaLog: DeltaLog,
      retentionHours: Option[Double],
      tableId: Option[TableIdentifier] = None): DataFrame = {
    VacuumCommand.gc(sparkSession, deltaLog, false, retentionHours)
    sparkSession.emptyDataFrame
  }

  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def sparkSession = self.toDF.sparkSession
}
