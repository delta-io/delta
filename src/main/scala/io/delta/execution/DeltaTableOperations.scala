/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.execution

import org.apache.spark.sql.delta.PreprocessTableUpdate
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.commands.DeleteCommand
import io.delta.DeltaTable

import org.apache.spark.sql.{functions, Column, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * Interface to provide operations that can be performed on a Delta Table.
 *    Delete: delete data from table with optional condition.
 *    Update: update data from table with optional condition and set columns rules.
 *    MergeInto:
 */
trait DeltaTableOperations { self: DeltaTable =>

  /**
   * Delete data from the table that match the given `condition`.
   * @param condition
   */
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
   * Delete data from the table that match the given `condition`.
   * @param condition
   */
  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /**
   * Delete data from the table.
   */
  def delete(): Unit = {
    executeDelete(None)
  }

  protected def executeDelete(condition: Option[Expression]): Unit = {
    val sparkSession = self.toDF.sparkSession
    val delete = Delete(self.toDF.queryExecution.analyzed, condition)
    // subquery need to be checked before executePlan,
    // in order to report more meaningful exceptions,
    // which is `subquery not supported.`
    delete.condition match {
      case Some(cond) if SubqueryExpression.hasSubquery(cond) =>
        throw DeltaErrors.subqueryNotSupportedException("DELETE", cond)
      case _ =>
    }
    val qe = sparkSession.sessionState.executePlan(delete)
    val resolvedDelete = qe.analyzed.asInstanceOf[Delete]
    val deleteCommand = DeleteCommand(resolvedDelete)
    deleteCommand.run(sparkSession)
  }

  /**
   * Update data from the table, which performs the rules defined by `set`.
   * @param set
   */
  def update(set: Map[String, Column]): Unit = {
    val setColumns = getSetColumnsForColumn(set)
    executeUpdate(setColumns, None)
  }

  /**
   * Update data from the table, which performs the rules defined by `set`.
   * Java friendly API.
   * @param set
   */
  def update(set: java.util.Map[String, Column]): Unit = {
    val setColumns = getSetColumnsForJavaColumn(set)
    executeUpdate(setColumns, None)
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   * @param condition
   * @param set
   */
  def update(condition: Column, set: Map[String, Column]): Unit = {
    val setColumns = getSetColumnsForColumn(set)
    executeUpdate(setColumns, Some(condition))
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   * Java friendly API.
   * @param condition
   * @param set
   */
  def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    val setColumns = getSetColumnsForJavaColumn(set)
    executeUpdate(setColumns, Some(condition))
  }

  /**
   * Update data from the table, which performs the rules defined by `set`.
   * @param set
   */
  def updateExpr(set: Map[String, String]): Unit = {
    val setColumns = getSetColumns(set)
    executeUpdate(setColumns, None)
  }

  /**
   * Update data from the table, which performs the rules defined by `set`.
   * Java friendly API.
   * @param set
   */
  def updateExpr(set: java.util.Map[String, String]): Unit = {
    val setColumns = getSetColumnsForJava(set)
    executeUpdate(setColumns, None)
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   * @param condition
   * @param set
   */
  def updateExpr(condition: String, set: Map[String, String]): Unit = {
    val setColumns = getSetColumns(set)
    executeUpdate(setColumns, Some(functions.expr(condition)))
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   * Java friendly API.
   * @param condition
   * @param set
   */
  def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    val setColumns = getSetColumnsForJava(set)
    executeUpdate(setColumns, Some(functions.expr(condition)))
  }

  protected def getSetColumns(set: Map[String, String]): Seq[(String, Column)] = {
    var setColumns: Seq[(String, Column)] = Seq()
    for ((col, expr) <- set) {
      setColumns = setColumns :+ ((col, functions.expr(expr)))
    }
    setColumns
  }

  protected def getSetColumnsForColumn(set: Map[String, Column]): Seq[(String, Column)] = {
    var setColumns: Seq[(String, Column)] = Seq()
    for ((col, expr) <- set) {
      setColumns = setColumns :+ ((col, expr))
    }
    setColumns
  }

  protected def getSetColumnsForJava(set: java.util.Map[String, String]): Seq[(String, Column)] = {
    import scala.collection.JavaConverters._
    var setColumns: Seq[(String, Column)] = Seq()
    for ((col, expr) <- set.asScala) {
      setColumns = setColumns :+ ((col, functions.expr(expr)))
    }
    setColumns
  }

  protected def getSetColumnsForJavaColumn(
      set: java.util.Map[String, Column]): Seq[(String, Column)] = {
    import scala.collection.JavaConverters._
    var setColumns: Seq[(String, Column)] = Seq()
    for ((col, expr) <- set.asScala) {
      setColumns = setColumns :+ ((col, expr))
    }
    setColumns
  }

  protected def makeUpdateTable(
      sparkSession: SparkSession,
      target: DeltaTable,
      onCondition: Option[Column],
      setColumns: Seq[(String, Column)]): UpdateTable = {
    val updateColumns = setColumns.map { x => UnresolvedAttribute.quotedString(x._1) }
    val updateExpressions = setColumns.map{ x => x._2.expr }
    onCondition match {
      case Some(c) =>
        new UpdateTable(
          target.toDF.queryExecution.analyzed,
          updateColumns,
          updateExpressions,
          Some(c.expr))
      case None =>
        new UpdateTable(
          target.toDF.queryExecution.analyzed,
          updateColumns,
          updateExpressions,
          None)
    }
  }

  protected def executeUpdate(
      setColumns: Seq[(String, Column)],
      condition: Option[Column]): Unit = {
    val sparkSession = self.toDF.sparkSession
    val updateTable = makeUpdateTable(sparkSession, self, condition, setColumns)
    val qe = sparkSession.sessionState.executePlan(updateTable)
    val resolvedUpdate = qe.analyzed.asInstanceOf[UpdateTable]
    val updateCommand =
      PreprocessTableUpdate(sparkSession.sessionState.conf)(resolvedUpdate)
        .asInstanceOf[RunnableCommand]
    updateCommand.run(sparkSession)
  }
}
