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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.constraints.Constraints

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/** Thrown when the given data doesn't match the rules defined on the table. */
case class InvariantViolationException(message: String) extends RuntimeException(message)

object InvariantViolationException {
  def apply(constraint: Constraints.NotNull): InvariantViolationException = {
    new InvariantViolationException(s"NOT NULL constraint violated for column: " +
      s"${UnresolvedAttribute(constraint.column).name}.\n")
  }

  /**
   * Build an exception to report the current row failed a CHECK constraint.
   *
   * @param constraint the constraint definition
   * @param values a map of full column names to their evaluated values in the failed row
   */
  def apply(
      constraint: Constraints.Check,
      values: Map[String, Any]): InvariantViolationException = {
    val valueLines = values.map {
      case (column, value) =>
        s" - $column : $value"
    }.mkString("\n")
    new InvariantViolationException(
      s"CHECK constraint ${constraint.name} ${constraint.expression.sql} " +
        s"violated by row with values:\n$valueLines")
  }

  /**
   * Columns and values in parallel lists as a shim for Java codegen compatibility.
   */
  def apply(
      constraint: Constraints.Check,
      columns: java.util.List[String],
      values: java.util.List[Any]): InvariantViolationException = {
    apply(constraint, columns.asScala.zip(values.asScala).toMap)
  }
}
