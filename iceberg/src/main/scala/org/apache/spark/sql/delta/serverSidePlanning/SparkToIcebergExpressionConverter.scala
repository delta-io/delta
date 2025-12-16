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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import shadedForDelta.org.apache.iceberg.expressions.{Expression, Expressions}

/**
 * Converts Spark Filter expressions to Iceberg Expression objects for server-side planning.
 *
 * This is a pure utility class with no side effects. All methods are thread-safe and stateless.
 *
 * Supported filters:
 * - EqualTo, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual
 * - IsNull, IsNotNull
 * - And, Or (for combining filters)
 *
 * Unsupported filters return None and are not pushed to the server.
 * This only affects performance (server returns more data), not correctness
 * (Spark always re-applies all filters locally as residuals).
 *
 * Example usage:
 * {{{
 *   val sparkFilter = EqualTo("id", 5)
 *   val schema = StructType(...)
 *   SparkToIcebergExpressionConverter.convert(sparkFilter, schema) match {
 *     case Some(icebergExpr) => // Use expression
 *     case None => // Filter not supported
 *   }
 * }}}
 */
object SparkToIcebergExpressionConverter {

  /**
   * Convert a Spark Filter to an Iceberg Expression.
   *
   * @param filter The Spark filter to convert
   * @param schema The table schema (currently unused but reserved for future type conversions)
   * @return Some(Expression) if the filter is supported, None otherwise
   */
  def convert(filter: Filter, schema: StructType): Option[Expression] = filter match {
    case EqualTo(attribute, value) =>
      Some(convertEqualTo(attribute, value))

    case LessThan(attribute, value) =>
      Some(convertLessThan(attribute, value))

    case GreaterThan(attribute, value) =>
      Some(convertGreaterThan(attribute, value))

    case LessThanOrEqual(attribute, value) =>
      Some(convertLessThanOrEqual(attribute, value))

    case GreaterThanOrEqual(attribute, value) =>
      Some(convertGreaterThanOrEqual(attribute, value))

    case IsNull(attribute) =>
      Some(Expressions.isNull(attribute))

    case IsNotNull(attribute) =>
      Some(Expressions.notNull(attribute))

    case And(left, right) =>
      for {
        leftExpr <- convert(left, schema)
        rightExpr <- convert(right, schema)
      } yield Expressions.and(leftExpr, rightExpr)

    case Or(left, right) =>
      for {
        leftExpr <- convert(left, schema)
        rightExpr <- convert(right, schema)
      } yield Expressions.or(leftExpr, rightExpr)

    case _ =>
      // Unsupported filter type (e.g., StringStartsWith, In, Not, etc.)
      // Return None to indicate this filter cannot be pushed down to the server.
      // Correctness is preserved because Spark re-applies all filters as residuals.
      None
  }

  /**
   * Convert an array of Spark filters to a single Iceberg Expression.
   * Multiple filters are combined with AND. Unsupported filters are not pushed to the server
   * (affects performance only, not correctness - Spark re-applies all filters as residuals).
   *
   * @param filters Array of Spark filters
   * @param schema The table schema
   * @return Some(Expression) if at least one filter is supported, None if all are unsupported
   */
  def convertFilters(filters: Array[Filter], schema: StructType): Option[Expression] = {
    if (filters.isEmpty) return None

    val convertedExprs = filters.flatMap(f => convert(f, schema))
    if (convertedExprs.isEmpty) return None

    // Combine all expressions with AND
    Some(convertedExprs.reduce((left, right) => Expressions.and(left, right)))
  }

  // Private helper methods for type-specific conversions

  private def convertEqualTo(attribute: String, value: Any): Expression = value match {
    case v: Int => Expressions.equal(attribute, v: Integer)
    case v: Long => Expressions.equal(attribute, v: java.lang.Long)
    case v: Float => Expressions.equal(attribute, v: java.lang.Float)
    case v: Double => Expressions.equal(attribute, v: java.lang.Double)
    case v: String => Expressions.equal(attribute, v)
    case v: Boolean => Expressions.equal(attribute, v: java.lang.Boolean)
    case null => Expressions.isNull(attribute)
    case _ => Expressions.equal(attribute, value.toString)
  }

  private def convertLessThan(attribute: String, value: Any): Expression = value match {
    case v: Int => Expressions.lessThan(attribute, v: Integer)
    case v: Long => Expressions.lessThan(attribute, v: java.lang.Long)
    case v: Float => Expressions.lessThan(attribute, v: java.lang.Float)
    case v: Double => Expressions.lessThan(attribute, v: java.lang.Double)
    case v: String => Expressions.lessThan(attribute, v)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported type for LessThan: ${value.getClass}")
  }

  private def convertGreaterThan(attribute: String, value: Any): Expression = value match {
    case v: Int => Expressions.greaterThan(attribute, v: Integer)
    case v: Long => Expressions.greaterThan(attribute, v: java.lang.Long)
    case v: Float => Expressions.greaterThan(attribute, v: java.lang.Float)
    case v: Double => Expressions.greaterThan(attribute, v: java.lang.Double)
    case v: String => Expressions.greaterThan(attribute, v)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported type for GreaterThan: ${value.getClass}")
  }

  private def convertLessThanOrEqual(attribute: String, value: Any): Expression = value match {
    case v: Int => Expressions.lessThanOrEqual(attribute, v: Integer)
    case v: Long => Expressions.lessThanOrEqual(attribute, v: java.lang.Long)
    case v: Float => Expressions.lessThanOrEqual(attribute, v: java.lang.Float)
    case v: Double => Expressions.lessThanOrEqual(attribute, v: java.lang.Double)
    case v: String => Expressions.lessThanOrEqual(attribute, v)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported type for LessThanOrEqual: ${value.getClass}")
  }

  private def convertGreaterThanOrEqual(attribute: String, value: Any): Expression = value match {
    case v: Int => Expressions.greaterThanOrEqual(attribute, v: Integer)
    case v: Long => Expressions.greaterThanOrEqual(attribute, v: java.lang.Long)
    case v: Float => Expressions.greaterThanOrEqual(attribute, v: java.lang.Float)
    case v: Double => Expressions.greaterThanOrEqual(attribute, v: java.lang.Double)
    case v: String => Expressions.greaterThanOrEqual(attribute, v)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported type for GreaterThanOrEqual: ${value.getClass}")
  }
}
