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

/**
 * This shim is introduced to due to breaking `Column` API changes in Spark master with
 * apache/spark#47785. It removed the following two APIs (both of which are already
 * available in 3.5 and not needed to be shimmed):
 * - `Column.expr`
 * - `Column.apply(Expression)`
 */
object ColumnImplicitsShim {
  /**
   * Implicitly convert a [[Column]] to an [[Expression]]. Sometimes the `Column.expr` extension
   * above conflicts other implicit conversions, so this method can be explicitly used.
   */
  def expression(column: Column): Expression = {
    column.expr
  }
}
