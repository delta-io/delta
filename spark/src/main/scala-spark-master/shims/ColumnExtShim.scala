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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.ExpressionUtils

/**
 * This shim is introduced to due to breaking `Column` API changes in Spark master with
 * apache/spark#47785. It removed the following two APIs:
 * - `Column.expr` - Replaced with `ExpressionUtils.expression(column)`
 * - `Column.apply(Expression)` - Replaced with `ExpressionUtils.column(e)`
 */
object ColumnImplicitsShim {

  /**
   * Extend [[Column]] to provide `expr` method to get the [[Expression]].
   * This avoids changing every `Column.expr` to `ExpressionUtils.expression(column)`.
   *
   * @param column The column to get the expression from.
   */
  implicit class ColumnExprExt(val column: Column) extends AnyVal {
    def expr: Expression = ExpressionUtils.expression(column)
  }

  /**
   * Provide an implicit constructor to create a [[Column]] from an [[Expression]].
   */
  implicit class ColumnConstructorExt(val c: Column.type) extends AnyVal {
    def apply(e: Expression): Column = ExpressionUtils.column(e)
  }

  /**
   * Implicitly convert a [[Column]] to an [[Expression]]. Sometimes the `Column.expr` extension
   * above conflicts other implicit conversions, so this method can be explicitly used.
   */
  def expression(column: Column): Expression = {
    ExpressionUtils.expression(column)
  }
}
