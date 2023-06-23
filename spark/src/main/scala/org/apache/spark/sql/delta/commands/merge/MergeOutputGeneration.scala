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

package org.apache.spark.sql.delta.commands.merge

import scala.collection.mutable

import org.apache.spark.sql.delta.commands.MergeIntoCommandBase

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._

/**
 * Contains logic to transform the merge clauses into expressions that can be evaluated to obtain
 * the output of the merge operation.
 */
trait MergeOutputGeneration { self: MergeIntoCommandBase =>
  import MergeIntoCommandBase._

  /**
   * Precompute conditions in MATCHED and NOT MATCHED clauses and generate the source
   * data frame with precomputed boolean columns.
   * @param sourceDF the source DataFrame.
   * @param clauses the merge clauses to precompute.
   * @return Generated sourceDF with precomputed boolean columns, matched clauses with
   *         possible rewritten clause conditions, insert clauses with possible rewritten
   *         clause conditions
   */
  protected def generatePrecomputedConditionsAndDF(
      sourceDF: DataFrame,
      clauses: Seq[DeltaMergeIntoClause])
  : (DataFrame, Seq[DeltaMergeIntoClause]) = {
    //
    // ==== Precompute conditions in MATCHED and NOT MATCHED clauses ====
    // If there are conditions in the clauses, each condition will be computed once for every
    // column (and obviously for every row) within the per-column CaseWhen expressions. Since, the
    // conditions can be arbitrarily expensive, it is likely to be more efficient to
    // precompute them into boolean columns and use these new columns in the CaseWhen exprs.
    // Then each condition will be computed only once per row, and the resultant boolean reused
    // for all the columns in the row.
    //
    val preComputedClauseConditions = new mutable.ArrayBuffer[(String, Expression)]()

    // Rewrite clause condition into a simple lookup of precomputed column
    def rewriteCondition[T <: DeltaMergeIntoClause](clause: T): T = {
      clause.condition match {
        case Some(clauseCondition) =>
          val colName =
            s"""_${clause.clauseType}${PRECOMPUTED_CONDITION_COL}
            |${preComputedClauseConditions.length}_
            |""".stripMargin.replaceAll("\n", "") // ex: _update_condition_0_
          preComputedClauseConditions += ((colName, clauseCondition))
          clause.makeCopy(Array(Some(UnresolvedAttribute(colName)), clause.actions)).asInstanceOf[T]
        case None => clause
      }
    }

    // Get the clauses with possible rewritten clause conditions.
    // This will automatically populate the `preComputedClauseConditions`
    val clausesWithPrecompConditions = clauses.map(rewriteCondition)

    // Add the columns to precompute clause conditions
    val sourceWithPrecompConditions = {
      val newCols = preComputedClauseConditions.map { case (colName, conditionExpr) =>
        new Column(conditionExpr).as(colName)
      }
      sourceDF.select(Seq(col("*"))  ++ newCols : _*)
    }
    (sourceWithPrecompConditions, clausesWithPrecompConditions)
  }

}
