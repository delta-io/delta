/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.catalyst.expressions

/**
 * Shim for date/time expressions in Spark 4.1
 * Note: TimeAdd is removed in Spark 4.1
 */
object DateTimeExpressionShims {
  /**
   * Check if the given expression is a date/time arithmetic expression
   */
  def isDateTimeArithmeticExpression(expr: Expression): Boolean = {
    expr match {
      case _: AddMonthsBase | _: DateAdd | _: DateAddInterval | _: DateDiff | _: DateSub |
           _: DatetimeSub | _: LastDay | _: MonthsBetween | _: NextDay | _: SubtractDates |
           _: SubtractTimestamps | _: TimestampAdd | _: TimestampAddYMInterval |
           _: TimestampDiff | _: TruncInstant => true
      case _ => false
    }
  }
}
