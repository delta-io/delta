/*
 * Copyright (2026) The Delta Lake Project Authors.
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
 * |   EqualTo(col, null)  | Expressions.isNull()           |
 * |   EqualTo(col, NaN)   | Expressions.isNaN()            |
 * | NotEqualTo            | Expressions.notEqual()         |
 * |   NotEqualTo(col, NaN)| Expressions.notNaN()           |
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
private[serverSidePlanning] object SparkToIcebergExpressionConverter {

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

    /*
     * Unsupported Filters:
     * - StringEndsWith, StringContains: Iceberg API doesn't provide these predicates
     * - Not (general): Iceberg doesn't support arbitrary NOT expressions. We convert Not(EqualTo)
     * as a special case to NotEqualTo.
     */
    case _ =>
      None
    }
  } catch {
    case _: IllegalArgumentException =>
      /*
       * The filter is supported but conversion failed as the type or value is unsupported.
       * - NaN in comparison operators (LessThan, GreaterThan, etc.)
       * - Unsupported types (e.g., Array, Map, binary types)
       */
      None
  }

  // Private helper methods for type-specific conversions

  private def isNaN(value: Any): Boolean = value match {
    case v: Float => v.isNaN
    case v: Double => v.isNaN
    case _ => false
  }

  /**
   * Helper to build expressions with type coercion.
   * Handles numeric, string, date/time types, and optionally Boolean.
   * Throws IllegalArgumentException for unsupported types.
   *
   * @param supportBoolean if true, also handles Boolean type.
   *        Note: Comparison operators (LessThan, GreaterThan, etc.) don't support Boolean.
   *        Only equality operators (EqualTo, NotEqualTo) should set this to true.
   */
  private def buildExpression(
      attribute: String,
      sparkValue: Any,
      exprBuilder: (String, Any) => Expression,
      supportBoolean: Boolean = false): Expression = {
    if (isNaN(sparkValue)) {
      throw new IllegalArgumentException("Cannot convert NaN value to Iceberg expression")
    }

    sparkValue match {
      case v: Int => exprBuilder(attribute, v: Integer)
      case v: Long => exprBuilder(attribute, v: java.lang.Long)
      case v: Float => exprBuilder(attribute, v: java.lang.Float)
      case v: Double => exprBuilder(attribute, v: java.lang.Double)
      case v: java.math.BigDecimal => exprBuilder(attribute, v)
      case v: String => exprBuilder(attribute, v)
      case v: java.sql.Date =>
        // Iceberg expects days since epoch (1970-01-01) as Int
        // See: Iceberg Literals.java - Literals.from() doesn't accept java.sql.Date
        // Reference: https://github.com/apache/iceberg/blob/main/api/src/main/java/
        //   org/apache/iceberg/expressions/Literals.java
        val daysFromEpoch = (v.getTime / (1000L * 60 * 60 * 24)).toInt
        exprBuilder(attribute, daysFromEpoch: Integer)
      case v: java.sql.Timestamp =>
        // Iceberg expects microseconds since epoch as Long
        // See: Iceberg Literals.java - Literals.from() doesn't accept java.sql.Timestamp
        // Reference: https://github.com/apache/iceberg/blob/main/api/src/main/java/
        //   org/apache/iceberg/expressions/Literals.java
        val microsFromEpoch = v.getTime * 1000 + (v.getNanos % 1000000) / 1000
        exprBuilder(attribute, microsFromEpoch: java.lang.Long)
      case v: Boolean if supportBoolean => exprBuilder(attribute, v: java.lang.Boolean)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported type: ${sparkValue.getClass}")
    }
  }

  /*
   * Convert EqualTo with special handling for null and NaN.
   * Note: We cannot use Expressions.equal(col, null/NaN) because Iceberg models these
   * with specialized predicates (isNull/isNaN) that have different evaluation semantics:
   * - SQL: col = NULL returns NULL (unknown), but col IS NULL returns TRUE/FALSE
   * - Iceberg distinguishes these cases with separate predicate types
   * Reference: Iceberg SparkV2Filters.handleEqual() and handleNotEqual()
   * https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/
   *   org/apache/iceberg/spark/SparkV2Filters.java#L388-L404
   */
  private def convertEqualTo(attribute: String, sparkValue: Any): Expression = {
    sparkValue match {
      case null => Expressions.isNull(attribute)
      case _ if isNaN(sparkValue) => Expressions.isNaN(attribute)
      case _ => buildExpression(
        attribute,
        sparkValue,
        (a, v) => Expressions.equal(a, v),
        supportBoolean = true
      )
    }
  }

  /*
   * Convert NotEqualTo with special handling for null and NaN.
   * Note: Not(EqualTo(col, null)) from Spark (representing IS NOT NULL) is converted here.
   */
  private def convertNotEqualTo(attribute: String, sparkValue: Any): Expression = {
    sparkValue match {
      case null => Expressions.notNull(attribute)
      case _ if isNaN(sparkValue) => Expressions.notNaN(attribute)
      case _ => buildExpression(
        attribute,
        sparkValue,
        (a, v) => Expressions.notEqual(a, v),
        supportBoolean = true
      )
    }
  }

  private def convertIn(attribute: String, values: Array[Any]): Expression = {
    // Iceberg expects IN to filter out null values
    val nonNullValues = values.filter(_ != null).map {
      case v: Int => v: Integer
      case v: Long => v: java.lang.Long
      case v: Float => v: java.lang.Float
      case v: Double => v: java.lang.Double
      case v: java.math.BigDecimal => v
      case v: Boolean => v: java.lang.Boolean
      case v => v
    }
    Expressions.in(attribute, nonNullValues: _*)
  }

  private def convertLessThan(attribute: String, sparkValue: Any): Expression =
    buildExpression(attribute, sparkValue, (a, v) => Expressions.lessThan(a, v))

  private def convertGreaterThan(attribute: String, sparkValue: Any): Expression =
    buildExpression(attribute, sparkValue, (a, v) => Expressions.greaterThan(a, v))

  private def convertLessThanOrEqual(attribute: String, sparkValue: Any): Expression =
    buildExpression(attribute, sparkValue, (a, v) => Expressions.lessThanOrEqual(a, v))

  private def convertGreaterThanOrEqual(attribute: String, sparkValue: Any): Expression =
    buildExpression(attribute, sparkValue, (a, v) => Expressions.greaterThanOrEqual(a, v))
}
