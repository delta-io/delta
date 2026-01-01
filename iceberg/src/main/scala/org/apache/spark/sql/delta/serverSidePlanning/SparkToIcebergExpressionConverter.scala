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
 * | StringStartsWith      | Expressions.startsWith()       |
 * | AlwaysTrue            | Expressions.alwaysTrue()       |
 * | AlwaysFalse           | Expressions.alwaysFalse()      |
 * +-----------------------+--------------------------------+
 * }}}
 *
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
  private[serverSidePlanning] def convert(sparkFilter: Filter): Option[Expression] = try {
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

    // String Operations
    case StringStartsWith(attribute, value) =>
      Some(Expressions.startsWith(attribute, value))

    // Always True/False
    case AlwaysTrue() =>
      Some(Expressions.alwaysTrue())
    case AlwaysFalse() =>
      Some(Expressions.alwaysFalse())

    // Unsupported Filters
    case _ =>
      // Unsupported filter types (e.g., StringEndsWith, StringContains, EqualNullSafe, etc.)
      // Return None to indicate this filter cannot be pushed down to the server.
      None
    }
  } catch {
    case _: IllegalArgumentException =>
      // Cannot convert this filter (NaN, null in NotEqual, etc.)
      None
  }

  // Private helper methods for type-specific conversions

  private def isNaN(value: Any): Boolean = value match {
    case v: Float => v.isNaN
    case v: Double => v.isNaN
    case _ => false
  }

  /**
   * Base helper to build expressions with type coercion.
   * Handles common numeric, string, date/time types.
   *
   * @param handleUnsupported by-name parameter for custom handling of unsupported types
   */
  private def buildExpression(
      attribute: String,
      sparkValue: Any,
      exprBuilder: (String, Any) => Expression,
      handleUnsupported: => Expression): Expression = sparkValue match {
    case v: Int => exprBuilder(attribute, v: Integer)
    case v: Long => exprBuilder(attribute, v: java.lang.Long)
    case v: Float => exprBuilder(attribute, v: java.lang.Float)
    case v: Double => exprBuilder(attribute, v: java.lang.Double)
    case v: BigDecimal => exprBuilder(attribute, v.bigDecimal)
    case v: java.math.BigDecimal => exprBuilder(attribute, v)
    case v: String => exprBuilder(attribute, v)
    case v: java.sql.Date => exprBuilder(attribute, v)
    case v: java.sql.Timestamp => exprBuilder(attribute, v)
    case _ => handleUnsupported
  }

  /**
   * Helper to build comparison expressions (LessThan, GreaterThan, etc.).
   * Throws on unsupported types.
   */
  private def buildComparisonExpression(
      attribute: String,
      sparkValue: Any,
      exprBuilder: (String, Any) => Expression): Expression =
    buildExpression(
      attribute,
      sparkValue,
      exprBuilder,
      throw new IllegalArgumentException(s"Unsupported type: ${sparkValue.getClass}")
    )

  /**
   * Helper to build equality expressions (EqualTo, NotEqualTo).
   * Adds Boolean support and uses toString as fallback for unsupported types.
   */
  private def buildEqualityExpression(
      attribute: String,
      sparkValue: Any,
      exprBuilder: (String, Any) => Expression): Expression = sparkValue match {
    case v: Boolean => exprBuilder(attribute, v: java.lang.Boolean)
    case _ =>
      buildExpression(
        attribute,
        sparkValue,
        exprBuilder,
        exprBuilder(attribute, sparkValue.toString)
      )
  }

  private def convertEqualTo(attribute: String, sparkValue: Any): Expression = {
    // NaN values cannot be represented in Iceberg
    if (isNaN(sparkValue)) {
      throw new IllegalArgumentException("Cannot convert NaN to Iceberg expression")
    }

    sparkValue match {
      case null => Expressions.isNull(attribute)
      case _ => buildEqualityExpression(attribute, sparkValue, (a, v) => Expressions.equal(a, v))
    }
  }

  private def convertNotEqualTo(attribute: String, sparkValue: Any): Expression = {
    // NaN values cannot be represented in Iceberg
    if (isNaN(sparkValue)) {
      throw new IllegalArgumentException("Cannot convert NaN to Iceberg expression")
    }

    sparkValue match {
      case null =>
        throw new IllegalArgumentException(
          "NotEqualTo with null is unsupported. Use IsNotNull instead.")
      case _ => buildEqualityExpression(attribute, sparkValue, (a, v) => Expressions.notEqual(a, v))
    }
  }

  private def convertIn(attribute: String, values: Array[Any]): Expression = {
    // Iceberg expects IN to filter out null values and coerce types
    val nonNullValues = values.filter(_ != null).map {
      case v: Int => v: Integer
      case v: Long => v: java.lang.Long
      case v: Float => v: java.lang.Float
      case v: Double => v: java.lang.Double
      case v: Boolean => v: java.lang.Boolean
      case v => v
    }
    Expressions.in(attribute, nonNullValues: _*)
  }

  private def convertLessThan(attribute: String, sparkValue: Any): Expression =
    buildComparisonExpression(attribute, sparkValue, (a, v) => Expressions.lessThan(a, v))

  private def convertGreaterThan(attribute: String, sparkValue: Any): Expression =
    buildComparisonExpression(attribute, sparkValue, (a, v) => Expressions.greaterThan(a, v))

  private def convertLessThanOrEqual(attribute: String, sparkValue: Any): Expression =
    buildComparisonExpression(attribute, sparkValue, (a, v) => Expressions.lessThanOrEqual(a, v))

  private def convertGreaterThanOrEqual(attribute: String, sparkValue: Any): Expression =
    buildComparisonExpression(attribute, sparkValue, (a, v) => Expressions.greaterThanOrEqual(a, v))
}
