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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaThrowable, DeltaThrowableHelper}
import org.apache.spark.sql.delta.constraints.{CharVarcharConstraint, Constraints}
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/** Thrown when the given data doesn't match the rules defined on the table. */
case class InvariantViolationException(message: String) extends RuntimeException(message)

/**
 * Match a [[SparkException]] and return the root cause Exception if it is a
 * InvariantViolationException.
 */
object InnerInvariantViolationException {
  def unapply(t: Throwable): Option[InvariantViolationException] = t match {
    case s: SparkException =>
      Option(ExceptionUtils.getRootCause(s)) match {
        case Some(i: InvariantViolationException) => Some(i)
        case _ => None
      }
    case _ => None
  }
}

object DeltaInvariantViolationException {
  def apply(constraint: Constraints.NotNull): DeltaInvariantViolationException = {
    new DeltaInvariantViolationException(
      errorClass = "DELTA_NOT_NULL_CONSTRAINT_VIOLATED",
      messageParameters = Array(UnresolvedAttribute(constraint.column).name)
    )
  }

  /**
   * Build an exception to report the current row failed a CHECK constraint.
   *
   * @param constraint the constraint definition
   * @param values a map of full column names to their evaluated values in the failed row
   */
  def apply(
      constraint: Constraints.Check,
      values: Map[String, Any]): DeltaInvariantViolationException = {
    if (constraint.name == CharVarcharConstraint.INVARIANT_NAME) {
      return new DeltaInvariantViolationException(
        errorClass = "DELTA_EXCEED_CHAR_VARCHAR_LIMIT",
        messageParameters = Array(constraint.expression.toString))
    }

    // Sort by the column name to generate consistent error messages in Scala 2.12 and 2.13.
    val valueLines = values.toSeq.sortBy(_._1).map {
      case (column, value) =>
        s" - $column : $value"
    }.mkString("\n")
    new DeltaInvariantViolationException(
      errorClass = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
      messageParameters = Array(constraint.name, constraint.expression.sql, valueLines))
  }

  /**
   * Columns and values in parallel lists as a shim for Java codegen compatibility.
   */
  def apply(
      constraint: Constraints.Check,
      columns: java.util.List[String],
      values: java.util.List[Any]): DeltaInvariantViolationException = {
    apply(constraint, columns.asScala.zip(values.asScala).toMap)
  }
}

class DeltaInvariantViolationException(
    errorClass: String,
    messageParameters: Array[String])
  extends InvariantViolationException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters)) with DeltaThrowable {
  override def getErrorClass: String = errorClass
}
