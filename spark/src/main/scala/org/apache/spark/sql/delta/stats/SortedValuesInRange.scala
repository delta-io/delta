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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.TypeUtils

/**
 * Data skipping predicate for an IN list: true when at least one of `sortedValues` lies in the
 * closed range [`min`, `max`] (a file's min and max statistics of the column), and null when
 * `min` or `max` is null, like the comparisons used for `column = value`.
 *
 * It finds the smallest value that is not less than `min` with a binary search, so a file costs
 * O(log n) comparisons, and the generated code does not grow with the number of values (one
 * `min <= value AND value <= max` term per value exceeds the 64 KB limit of a generated method
 * at a few hundred values).
 *
 * @param sortedValues distinct, non-null Catalyst values of `min.dataType`, sorted by the
 *                     interpreted ordering of that type.
 */
case class SortedValuesInRange(min: Expression, max: Expression, sortedValues: IndexedSeq[Any])
  extends BinaryExpression with Predicate with CodegenFallback {

  override def left: Expression = min
  override def right: Expression = max

  @transient private lazy val ordering = TypeUtils.getInterpretedOrdering(min.dataType)

  override protected def nullSafeEval(minValue: Any, maxValue: Any): Any = {
    var low = 0
    var high = sortedValues.length
    while (low < high) {
      val mid = (low + high) >>> 1
      if (ordering.lt(sortedValues(mid), minValue)) low = mid + 1 else high = mid
    }
    low < sortedValues.length && ordering.lteq(sortedValues(low), maxValue)
  }

  // Do not print the values: the list can be long.
  override def toString: String = s"sortedValuesInRange($min, $max, ${sortedValues.length} values)"

  override def sql: String =
    s"sorted_values_in_range(${min.sql}, ${max.sql}, ${sortedValues.length} values)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): SortedValuesInRange =
    copy(min = newLeft, max = newRight)
}
