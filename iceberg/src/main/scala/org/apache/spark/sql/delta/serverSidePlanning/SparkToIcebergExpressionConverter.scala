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
import shadedForDelta.org.apache.iceberg.expressions.{Expression, Expressions}

/**
 * Converts Spark Filter expressions to Iceberg Expression objects for server-side planning.
 *
 * This is a pure utility class with no side effects. All methods are thread-safe and stateless.
 *
 * Filter Mapping Table:
 * {{{
 * +-----------------------+--------------------------------+
 * | Spark Filter          | Iceberg Expression             |
 * +-----------------------+--------------------------------+
 * | EqualTo               | Expressions.equal()            |
 * | NotEqualTo            | Expressions.notEqual()         |
 * | LessThan              | Expressions.lessThan()         |
 * | GreaterThan           | Expressions.greaterThan()      |
 * | LessThanOrEqual       | Expressions.lessThanOrEqual()  |
 * | GreaterThanOrEqual    | Expressions.greaterThanOrEqual()|
 * | In                    | Expressions.in()               |
 * | IsNull                | Expressions.isNull()           |
 * | IsNotNull             | Expressions.notNull()          |
 * | And                   | Expressions.and()              |
 * | Or                    | Expressions.or()               |
 * +-----------------------+--------------------------------+
 * }}}
 *
 * Unsupported filters return None and are not pushed to the server.
 * This only affects performance (server returns more data), not correctness
 * (Spark always re-applies all filters locally as residuals).
 *
 * Example usage:
 * {{{
 *   val sparkFilter = EqualTo("id", 5)
 *   SparkToIcebergExpressionConverter.convert(sparkFilter) match {
 *     case Some(icebergExpr) => // Use expression
 *     case None => // Filter not supported
 *   }
 * }}}
 */
object SparkToIcebergExpressionConverter {

  /**
   * Convert a Spark Filter to an Iceberg Expression.
   *
   * @param sparkFilter The Spark filter to convert
   * @return Some(Expression) if the filter is supported, None otherwise
   */
  private[serverSidePlanning] def convert(sparkFilter: Filter): Option[Expression] =
    sparkFilter match {
    // Equality and Comparison Operators
    case EqualTo(attribute, sparkValue) =>
      Some(convertEqualTo(attribute, sparkValue))
    case Not(EqualTo(attribute, sparkValue)) =>
      Some(convertNotEqualTo(attribute, sparkValue))
    case LessThan(attribute, sparkValue) =>
      Some(convertLessThan(attribute, sparkValue))
    case GreaterThan(attribute, sparkValue) =>
      Some(convertGreaterThan(attribute, sparkValue))
    case LessThanOrEqual(attribute, sparkValue) =>
      Some(convertLessThanOrEqual(attribute, sparkValue))
    case GreaterThanOrEqual(attribute, sparkValue) =>
      Some(convertGreaterThanOrEqual(attribute, sparkValue))
    case In(attribute, values) =>
      Some(convertIn(attribute, values))

    // Null Checks
    case IsNull(attribute) =>
      Some(Expressions.isNull(attribute))
    case IsNotNull(attribute) =>
      Some(Expressions.notNull(attribute))

    // Logical Combinators
    case And(left, right) =>
      for {
        leftIcebergExpr <- convert(left)
        rightIcebergExpr <- convert(right)
      } yield Expressions.and(leftIcebergExpr, rightIcebergExpr)

    case Or(left, right) =>
      for {
        leftIcebergExpr <- convert(left)
        rightIcebergExpr <- convert(right)
      } yield Expressions.or(leftIcebergExpr, rightIcebergExpr)

    // Unsupported Filters
    case _ =>
      // Unsupported filter type (e.g., StringStartsWith, In, Not, etc.)
      // Return None to indicate this filter cannot be pushed down to the server.
      // Correctness is preserved because Spark re-applies all filters as residuals.
      None
  }

  // Private helper methods for type-specific conversions

  private def convertEqualTo(attribute: String, sparkValue: Any): Expression = sparkValue match {
    case v: Int => Expressions.equal(attribute, v: Integer)
    case v: Long => Expressions.equal(attribute, v: java.lang.Long)
    case v: Float => Expressions.equal(attribute, v: java.lang.Float)
    case v: Double => Expressions.equal(attribute, v: java.lang.Double)
    case v: String => Expressions.equal(attribute, v)
    case v: Boolean => Expressions.equal(attribute, v: java.lang.Boolean)
    case null => Expressions.isNull(attribute)
    case _ => Expressions.equal(attribute, sparkValue.toString)
  }

  private def convertNotEqualTo(attribute: String, sparkValue: Any): Expression = sparkValue match {
    case v: Int => Expressions.notEqual(attribute, v: Integer)
    case v: Long => Expressions.notEqual(attribute, v: java.lang.Long)
    case v: Float => Expressions.notEqual(attribute, v: java.lang.Float)
    case v: Double => Expressions.notEqual(attribute, v: java.lang.Double)
    case v: String => Expressions.notEqual(attribute, v)
    case v: Boolean => Expressions.notEqual(attribute, v: java.lang.Boolean)
    case null =>
      throw new IllegalArgumentException(
        "NotEqualTo with null is always false. Use IsNotNull instead.")
    case _ => Expressions.notEqual(attribute, sparkValue.toString)
  }

  private def convertIn(attribute: String, values: Array[Any]): Expression = {
    // Filter out null values (following Iceberg OSS behavior)
    val nonNullValues = values.filter(_ != null).map {
      case v: Int => v: Integer
      case v: Long => v: java.lang.Long
      case v: Float => v: java.lang.Float
      case v: Double => v: java.lang.Double
      case v: String => v
      case v: Boolean => v: java.lang.Boolean
      case v => v
    }
    Expressions.in(attribute, nonNullValues: _*)
  }

  private def convertLessThan(attribute: String, sparkValue: Any): Expression = sparkValue match {
    case v: Int => Expressions.lessThan(attribute, v: Integer)
    case v: Long => Expressions.lessThan(attribute, v: java.lang.Long)
    case v: Float => Expressions.lessThan(attribute, v: java.lang.Float)
    case v: Double => Expressions.lessThan(attribute, v: java.lang.Double)
    case v: String => Expressions.lessThan(attribute, v)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported type for LessThan: ${sparkValue.getClass}")
  }

  private def convertGreaterThan(attribute: String, sparkValue: Any): Expression =
    sparkValue match {
    case v: Int => Expressions.greaterThan(attribute, v: Integer)
    case v: Long => Expressions.greaterThan(attribute, v: java.lang.Long)
    case v: Float => Expressions.greaterThan(attribute, v: java.lang.Float)
    case v: Double => Expressions.greaterThan(attribute, v: java.lang.Double)
    case v: String => Expressions.greaterThan(attribute, v)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported type for GreaterThan: ${sparkValue.getClass}")
  }

  private def convertLessThanOrEqual(attribute: String, sparkValue: Any): Expression =
    sparkValue match {
    case v: Int => Expressions.lessThanOrEqual(attribute, v: Integer)
    case v: Long => Expressions.lessThanOrEqual(attribute, v: java.lang.Long)
    case v: Float => Expressions.lessThanOrEqual(attribute, v: java.lang.Float)
    case v: Double => Expressions.lessThanOrEqual(attribute, v: java.lang.Double)
    case v: String => Expressions.lessThanOrEqual(attribute, v)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported type for LessThanOrEqual: ${sparkValue.getClass}")
  }

  private def convertGreaterThanOrEqual(attribute: String, sparkValue: Any): Expression =
    sparkValue match {
    case v: Int => Expressions.greaterThanOrEqual(attribute, v: Integer)
    case v: Long => Expressions.greaterThanOrEqual(attribute, v: java.lang.Long)
    case v: Float => Expressions.greaterThanOrEqual(attribute, v: java.lang.Float)
    case v: Double => Expressions.greaterThanOrEqual(attribute, v: java.lang.Double)
    case v: String => Expressions.greaterThanOrEqual(attribute, v)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported type for GreaterThanOrEqual: ${sparkValue.getClass}")
  }
}
