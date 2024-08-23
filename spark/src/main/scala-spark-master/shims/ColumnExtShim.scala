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

object ColumnImplicitsShim {
  implicit class ColumnExprExt(val column: Column) {
    def expr: Expression = ExpressionUtils.expression(column)
  }
  implicit class ColumnConstructorExt(c: Column.type) {
    def apply(e: Expression): Column = ExpressionUtils.column(e)
  }

  implicit def expression(column: Column): Expression = {
    ExpressionUtils.expression(column)
  }
}
