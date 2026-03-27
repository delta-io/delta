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

import scala.collection.Map

import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{DeltaGenerateCommand, DescribeDeltaDetailCommand, VacuumCommand}
import org.apache.spark.sql.delta.util.AnalysisHelper
import io.delta.tables.DeltaTable

import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Interface to provide the actual implementations of DeltaTable operations.
 */
trait DeltaTableOperations extends AnalysisHelper { self: DeltaTable =>

  protected def executeDelete(condition: Option[Expression]): Unit = improveUnsupportedOpError {
    withActiveSession(sparkSession) {
      val delete = DeleteFromTable(
        self.toDF.queryExecution.analyzed,
        condition.getOrElse(Literal.TrueLiteral))
      toDataset(sparkSession, delete)
    }
  }

  protected def executeHistory(
      deltaLog: DeltaLog,
      limit: Option[Int] = None,
      tableId: Option[TableIdentifier] = None): DataFrame = withActiveSession(sparkSession) {
    val history = deltaLog.history
    sparkSession.createDataFrame(history.getHistory(limit))
  }

  protected def executeDetails(
      path: String,
      tableIdentifier: Option[TableIdentifier]): DataFrame = withActiveSession(sparkSession) {
    val details = DescribeDeltaDetailCommand(Option(path), tableIdentifier, self.deltaLog.options)
    toDataset(sparkSession, details)
  }

  protected def executeGenerate(tblIdentifier: String, mode: String): Unit =
    withActiveSession(sparkSession) {
      val tableId: TableIdentifier = sparkSession
        .sessionState
        .sqlParser
        .parseTableIdentifier(tblIdentifier)
      val generate = DeltaGenerateCommand(mode, tableId, self.deltaLog.options)
      toDataset(sparkSession, generate)
    }

  protected def executeUpdate(
      set: Map[String, Column],
      condition: Option[Column]): Unit = improveUnsupportedOpError {
    withActiveSession(sparkSession) {
      val assignments = set.map { case (targetColName, column) =>
        Assignment(UnresolvedAttribute.quotedString(targetColName), column.expr)
      }.toSeq
      val update =
        UpdateTable(self.toDF.queryExecution.analyzed, assignments, condition.map(_.expr))
      toDataset(sparkSession, update)
    }
  }

  protected def executeVacuum(
      deltaLog: DeltaLog,
      retentionHours: Option[Double],
      tableId: Option[TableIdentifier] = None): DataFrame = withActiveSession(sparkSession) {
    VacuumCommand.gc(sparkSession, deltaLog, false, retentionHours)
    sparkSession.emptyDataFrame
  }

  protected def executeRestore(
      table: DeltaTableV2,
      versionAsOf: Option[Long],
      timestampAsOf: Option[String]): DataFrame = withActiveSession(sparkSession) {
    val identifier = table.getTableIdentifierIfExists.map(
      id => Identifier.of(id.database.toArray, id.table))
    val sourceRelation = DataSourceV2Relation.create(table, None, identifier)

    val restore = RestoreTableStatement(
      TimeTravel(
        sourceRelation,
        timestampAsOf.map(Literal(_)),
        versionAsOf,
        Some("deltaTable"))
      )
    toDataset(sparkSession, restore)
  }

  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def sparkSession = self.toDF.sparkSession
}
