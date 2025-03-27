/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Project, OneRowRelation}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.functions.{expr => sparkExpr}

/**
 * Utility object to handle Column to Expression conversions for Delta Lake operations.
 */
object ColumnExpressionUtils {
  /**
   * Converts a Column to an Expression using reflection.
   *
   * @param spark The active SparkSession
   * @param column The Column to convert
   * @return The Expression
   */
  def toExpression(spark: SparkSession, column: Column): Expression = {
    try {
      // Try to use reflection to get .expr directly
      val exprMethod = classOf[Column].getDeclaredMethod("expr")
      exprMethod.setAccessible(true)
      exprMethod.invoke(column).asInstanceOf[Expression]
    } catch {
      case _: Exception =>
        // Fallback: use Spark's internal conversion
        val exprValue = sparkExpr(column.toString)

        // Use reflection to extract the underlying Expression
        val exprField = classOf[Column].getDeclaredField("expr")
        exprField.setAccessible(true)
        exprField.get(exprValue).asInstanceOf[Expression]
    }
  }

  /**
   * Converts multiple Columns to Expressions in one pass.
   *
   * @param spark The active SparkSession
   * @param columns The Columns to convert
   * @return A sequence of Expressions
   */
  def toExpressions(spark: SparkSession, columns: Seq[Column]): Seq[Expression] = {
    columns.map(col => toExpression(spark, col))
  }

  /**
   * Implicit extension method to provide a convenient .deltaExpr method on Column
   */
  implicit class ColumnExpressionOps(column: Column) {
    def deltaExpr(spark: SparkSession): Expression = {
      toExpression(spark, column)
    }
  }
}
