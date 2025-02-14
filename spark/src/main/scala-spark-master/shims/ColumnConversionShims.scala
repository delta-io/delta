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

package org.apache.spark.sql.delta

import org.apache.spark.sql.classic.ClassicConversions
import org.apache.spark.sql.classic.ColumnConversions
import org.apache.spark.sql.classic.ColumnNodeToExpressionConverter

/**
 * Conversions from a [[org.apache.spark.sql.Column]] to an
 * [[org.apache.spark.sql.catalyst.expressions.Expression]], and vice versa.
 *
 * @note [[org.apache.spark.sql.internal.ExpressionUtils#expression]] is a cheap alternative for
 *       [[org.apache.spark.sql.Column]] to [[org.apache.spark.sql.catalyst.expressions.Expression]]
 *       conversions. However this can only be used when the produced expression is used in a Column
 *       later on.
 */
object ClassicColumnConversions
  extends ClassicConversions
  with ColumnConversions {
  override def converter: ColumnNodeToExpressionConverter = ColumnNodeToExpressionConverter
}
