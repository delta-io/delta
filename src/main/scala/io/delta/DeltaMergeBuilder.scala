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

package io.delta

import scala.collection.JavaConverters._
import scala.collection.Map

import org.apache.spark.sql.delta.PreprocessTableMerge
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper => _, _}
import org.apache.spark.sql.functions.expr

case class DeltaMergeBuilder private[delta](
    private val targetTable: DeltaTable,
    private val source: DataFrame,
    private val onCondition: Column,
    private[delta] val whenClauses: Seq[MergeIntoClause]) extends AnalysisHelper {

  def whenMatched(): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, None)
  }

  def whenMatched(condition: String): DeltaMergeMatchedActionBuilder = {
    whenMatched(expr(condition))
  }

  def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, Some(condition))
  }

  def whenNotMatched(): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, None)
  }

  def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder = {
    whenNotMatched(expr(condition))
  }

  def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, Some(condition))
  }

  def execute(): Unit = {
    val sparkSession = targetTable.toDF.sparkSession
    val resolvedMergeInto =
      MergeInto.resolveReferences(mergePlan)(tryResolveReferences(sparkSession) _)
    if (!resolvedMergeInto.resolved) {
      throw DeltaErrors.analysisException("Failed to resolve\n", Some(resolvedMergeInto))
    }
    // Preprocess the actions and verify
    val mergeIntoCommand = PreprocessTableMerge(sparkSession.sessionState.conf)(resolvedMergeInto)
    sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
    mergeIntoCommand.run(sparkSession)
  }

  private def mergePlan: MergeInto = {
    MergeInto(
      targetTable.toDF.queryExecution.analyzed,
      source.queryExecution.analyzed,
      onCondition.expr,
      whenClauses)
  }
}

case class DeltaMergeMatchedActionBuilder private[delta](
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column]) {

  def update(set: Map[String, Column]): DeltaMergeBuilder = {
    updateExpression(set)
  }

  def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    updateExpression(toStrColumnMap(set))
  }

  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    updateExpression(set.asScala)
  }

  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    updateExpression(toStrColumnMap(set.asScala))
  }

  def updateAll(): DeltaMergeBuilder = {
    val updateClause = MergeIntoUpdateClause(
      matchCondition.map(_.expr),
      MergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.copy(whenClauses = mergeBuilder.whenClauses :+ updateClause)
  }

  def delete(): DeltaMergeBuilder = {
    val deleteClause = MergeIntoDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.copy(whenClauses = mergeBuilder.whenClauses :+ deleteClause)
  }

  private def updateExpression(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty) {
      mergeBuilder.copy()
    } else {
      val setActions = set.toSeq
      val updateActions = MergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr))
      val updateClause = MergeIntoUpdateClause(matchCondition.map(_.expr), updateActions)
      mergeBuilder.copy(whenClauses = mergeBuilder.whenClauses :+ updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

case class DeltaMergeNotMatchedActionBuilder private[delta](
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column]) {

  def insert(values: Map[String, Column]): DeltaMergeBuilder = {
    insertExpression(values)
  }

  def insertExpr(values: Map[String, String]): DeltaMergeBuilder = {
    insertExpression(toStrColumnMap(values))
  }

  def insert(values: java.util.Map[String, Column]): DeltaMergeBuilder = {
    insertExpression(values.asScala)
  }

  def insertExpr(values: java.util.Map[String, String]): DeltaMergeBuilder = {
    insertExpression(toStrColumnMap(values.asScala))
  }

  def insertAll(): DeltaMergeBuilder = {
    val insertClause = MergeIntoInsertClause(
      matchCondition.map(_.expr),
      MergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.copy(whenClauses = mergeBuilder.whenClauses :+ insertClause)
  }

  private def insertExpression(setValues: Map[String, Column]): DeltaMergeBuilder = {
    if (setValues.isEmpty) {
      throw DeltaErrors.analysisException(
        "There must be at least one field in INSERT clause in a MERGE statement", None)
    } else {
      val values = setValues.toSeq
      val insertActions = MergeIntoClause.toActions(
        colNames = values.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = values.map(x => x._2.expr))
      val insertClause = MergeIntoInsertClause(matchCondition.map(_.expr), insertActions)
      mergeBuilder.copy(whenClauses = mergeBuilder.whenClauses :+ insertClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}
