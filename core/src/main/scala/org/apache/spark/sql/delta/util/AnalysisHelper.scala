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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.DeltaErrors

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait AnalysisHelper {
  import AnalysisHelper._

  protected def tryResolveReferences(
      sparkSession: SparkSession)(
      expr: Expression,
      planContainingExpr: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(Seq(expr), planContainingExpr.children)
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr, _) =>
        // Return even if it did not successfully resolve
        resolvedExpr.head
      case _ =>
        // This is unexpected
        throw DeltaErrors.analysisException(
          s"Could not resolve expression $expr", plan = Option(planContainingExpr))
    }
  }

  /**
   * Resolve expressions using the attributes provided by `planProvidingAttrs`. Throw an error if
   * failing to resolve any expressions.
   */
  protected def resolveReferencesForExpressions(
      sparkSession: SparkSession,
      exprs: Seq[Expression],
      planProvidingAttrs: LogicalPlan): Seq[Expression] = {
    val newPlan = FakeLogicalPlan(exprs, Seq(planProvidingAttrs))
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExprs, _) =>
        resolvedExprs.foreach { expr =>
          if (!expr.resolved) {
            expr.failAnalysis(s"cannot resolve ${expr.sql} given $planProvidingAttrs")
          }
        }
        resolvedExprs
      case _ =>
        // This is unexpected
        throw DeltaErrors.analysisException(
          s"Could not resolve expressions $exprs", plan = Option(planProvidingAttrs))
    }
  }

  protected def toDataset(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  protected def improveUnsupportedOpError(f: => Unit): Unit = {
    val possibleErrorMsgs = Seq(
      "is only supported with v2 table", // full error: DELETE is only supported with v2 tables
      "is not supported temporarily",    // full error: UPDATE TABLE is not supported temporarily
      "Table does not support read",
      "Table implementation does not support writes"
    ).map(_.toLowerCase())

    def isExtensionOrCatalogError(error: Exception): Boolean = {
      possibleErrorMsgs.exists { m =>
        error.getMessage != null && error.getMessage.toLowerCase().contains(m)
      }
    }

    try { f } catch {
      case e: Exception if isExtensionOrCatalogError(e) =>
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(e)
    }
  }
}

object AnalysisHelper {
  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(exprs: Seq[Expression], children: Seq[LogicalPlan])
    extends LogicalPlan {
    override def output: Seq[Attribute] = Nil

    // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
  }
}
