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
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{DeltaGenerateCommand, DescribeDeltaDetailCommand, VacuumCommand}
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Interface to provide the actual implementations of DeltaTable operations.
 */
trait DeltaTableOperations extends AnalysisHelper { self: io.delta.tables.DeltaTable =>

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
      catalogTable: Option[CatalogTable] = None): DataFrame = withActiveSession(sparkSession) {
    val history = deltaLog.history
    sparkSession.createDataFrame(history.getHistory(limit, catalogTable))
  }

  protected def executeDetails(
      path: String,
      tableIdentifier: Option[TableIdentifier]): DataFrame = withActiveSession(sparkSession) {
    val details = DescribeDeltaDetailCommand(Option(path), tableIdentifier, self.deltaLog.options)
    toDataset(sparkSession, details)
  }

  protected def executeGenerate(
      path: String,
      tableIdentifier: Option[TableIdentifier],
      mode: String): Unit = withActiveSession(sparkSession) {
    val generate = DeltaGenerateCommand(Option(path), tableIdentifier, mode, self.deltaLog.options)
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
      table: DeltaTableV2,
      retentionHours: Option[Double]): DataFrame = withActiveSession(sparkSession) {
    val tableId = table.getTableIdentifierIfExists
    val path = Option.when(tableId.isEmpty)(deltaLog.dataPath.toString)
    val vacuum = VacuumTableCommand(
      path,
      tableId,
      inventoryTable = None,
      inventoryQuery = None,
      retentionHours,
      dryRun = false,
      vacuumType = None,
      deltaLog.options)
    toDataset(sparkSession, vacuum)
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

  protected def executeClone(
      table: DeltaTableV2,
      target: String,
      replace: Boolean,
      properties: Map[String, String],
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None
  ): io.delta.tables.DeltaTable = withActiveSession(sparkSession) {
    val sourceIdentifier = table.getTableIdentifierIfExists.map(id =>
      Identifier.of(id.database.toArray, id.table))
    val sourceRelation = DataSourceV2Relation.create(table, None, sourceIdentifier)

    val maybeTimeTravelSource = if (versionAsOf.isDefined || timestampAsOf.isDefined) {
      TimeTravel(
        sourceRelation,
        timestampAsOf.map(Literal(_)),
        versionAsOf,
        Some("deltaTable")
      )
    } else {
      sourceRelation
    }

    val targetIsAbsolutePath = new Path(target).isAbsolute()
    val targetIdentifier = if (targetIsAbsolutePath) s"delta.`$target`" else target
    val targetRelation = UnresolvedRelation(
      sparkSession.sessionState.sqlParser.parseTableIdentifier(targetIdentifier))

    val clone = CloneTableStatement(
      maybeTimeTravelSource,
      targetRelation,
      ifNotExists = false,
      replace,
      isCreateCommand = true,
      tablePropertyOverrides = properties.toMap,
      targetLocation = None)

    toDataset(sparkSession, clone)

    if (targetIsAbsolutePath) {
      io.delta.tables.DeltaTable.forPath(sparkSession, target)
    } else {
      io.delta.tables.DeltaTable.forName(sparkSession, target)
    }
  }

  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def sparkSession = self.toDF.sparkSession
}
