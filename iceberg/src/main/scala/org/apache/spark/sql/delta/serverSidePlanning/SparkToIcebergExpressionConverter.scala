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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
 * | Not(In)               | Expressions.notIn()            |
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
private[serverSidePlanning] object SparkToIcebergExpressionConverter extends Logging {

  /**
   * Convert a Spark Filter to an Iceberg Expression.
   *
   * @param sparkFilter The Spark filter to convert
   * @return Some(Expression) if the filter is supported, None otherwise
   */
  private[serverSidePlanning] def convert(sparkFilter: Filter): Option[Expression] = {
    logInfo(s"Converting Spark filter to Iceberg expression: $sparkFilter")
    val result = try {
      sparkFilter match {
    // Equality and Comparison Operators
    case EqualTo(attribute, sparkValue) =>
      Some(convertEqualTo(attribute, sparkValue))
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

    // NOT Operator (special case)
    case Not(innerFilter) =>
      convertNot(innerFilter)

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
       */
      case _ =>
        logInfo(s"Unsupported Spark filter (no Iceberg equivalent): " +
          s"${sparkFilter.getClass.getSimpleName} - $sparkFilter")
        None
      }
    } catch {
      case e: IllegalArgumentException =>
        /*
         * The filter is supported but conversion failed as the type or value is unsupported.
         * - NaN in comparison operators (LessThan, GreaterThan, etc.)
         * - Unsupported types (e.g., Array, Map, binary types)
         */
        logWarning(s"Failed to convert Spark filter due to unsupported type or value: " +
          s"$sparkFilter", e)
        None
    }
    logDebug(s"Conversion result for $sparkFilter: " +
      s"${result.map(_.toString).getOrElse("None (unsupported)")}")
    result
  }

  // Private helper methods for type-specific conversions

  private def isNaN(value: Any): Boolean = value match {
    case v: Float => v.isNaN
    case v: Double => v.isNaN
    case _ => false
  }

  /**
   * Convert a Spark value to Iceberg-compatible type with proper coercion.
   * @param supportBoolean if true, also handles Boolean type.
   *        Note: Comparison operators (LessThan, GreaterThan, etc.) don't support Boolean.
   *        Only equality operators (EqualTo, NotEqualTo) should set this to true.
   */
  private[serverSidePlanning] def toIcebergValue(
      value: Any,
      supportBoolean: Boolean = false): Any = value match {
    // Date/Timestamp conversion (semantic change) because
    // Iceberg Literals.from() doesn't accept java.sql.Date/Timestamp, expects Int/Long
    case v: java.sql.Date =>
      // Iceberg expects days since epoch (1970-01-01) as Int
      DateTimeUtils.fromJavaDate(v): Integer
    case v: java.sql.Timestamp =>
      // Iceberg expects microseconds since epoch as Long
      DateTimeUtils.fromJavaTimestamp(v): java.lang.Long
    // Type coercion (Scala to Java boxed types)
    case v: Int => v: Integer
    case v: Long => v: java.lang.Long
    case v: Float => v: java.lang.Float
    case v: Double => v: java.lang.Double
    case v: java.math.BigDecimal => v
    case v: String => v
    case v: Boolean if supportBoolean => v: java.lang.Boolean
    case _ => value
  }

  /*
   * Convert EqualTo with special handling for null and NaN.
   * Note: We cannot use Expressions.equal(col, null/NaN) because Iceberg models these
   * with specialized predicates (isNull/isNaN) that have different evaluation semantics:
   * - SQL: col = NULL returns NULL (unknown), but col IS NULL returns TRUE/FALSE
   * Reference: OSS Iceberg SparkV2Filters.handleEqual()
   */
  private def convertEqualTo(attribute: String, sparkValue: Any): Expression = {
    sparkValue match {
      case null => Expressions.isNull(attribute)
      case _ if isNaN(sparkValue) => Expressions.isNaN(attribute)
      case _ => Expressions.equal(attribute, toIcebergValue(sparkValue, supportBoolean = true))
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
      case _ => Expressions.notEqual(attribute, toIcebergValue(sparkValue, supportBoolean = true))
    }
  }

  /**
   * Convert a Spark NOT filter to an Iceberg Expression.
   *
   * Supported conversions:
   * - Not(EqualTo(col, value)) -> Expressions.notEqual
   * - Not(EqualTo(col, null)) -> Expressions.notNull
   * - Not(EqualTo(col, NaN)) -> Expressions.notNaN
   * - Not(In(col, values)) -> Expressions.notIn
   *
   * All other NOT expressions (Not(LessThan), Not(And), etc.) are unsupported because Iceberg
   * doesn't provide equivalent predicates. This is consistent with OSS Iceberg's SparkV2Filters.
   */
  private def convertNot(sparkInnerFilter: Filter): Option[Expression] = {
    sparkInnerFilter match {
      case EqualTo(attribute, sparkValue) =>
        Some(convertNotEqualTo(attribute, sparkValue))

      case In(attribute, values) =>
        Some(convertNotIn(attribute, values))

      case _ =>
        None  // All other NOT expressions are unsupported
    }
  }

  private def convertIn(attribute: String, values: Array[Any]): Expression = {
    // Iceberg expects IN to filter out null values and convert Date/Timestamp to Int/Long
    val nonNullValues = values.filter(_ != null).map(v =>
      toIcebergValue(v, supportBoolean = true)
    )
    Expressions.in(attribute, nonNullValues: _*)
  }

  /**
   * Convert NOT IN filter to Iceberg notIn expression.
   * Example: NOT IN ("id", [1, 2, 3]) -> Expressions.notIn("id", 1, 2, 3)
   */
  private def convertNotIn(attribute: String, values: Array[Any]): Expression = {
    // Iceberg expects NOT IN to filter out null values and convert Date/Timestamp to Int/Long
    val nonNullValues = values.filter(_ != null).map(v =>
      toIcebergValue(v, supportBoolean = true)
    )
    Expressions.notIn(attribute, nonNullValues: _*)
  }

  private def convertLessThan(attribute: String, sparkValue: Any): Expression =
    Expressions.lessThan(attribute, toIcebergValue(sparkValue, supportBoolean = false))

  private def convertGreaterThan(attribute: String, sparkValue: Any): Expression =
    Expressions.greaterThan(attribute, toIcebergValue(sparkValue, supportBoolean = false))

  private def convertLessThanOrEqual(attribute: String, sparkValue: Any): Expression =
    Expressions.lessThanOrEqual(attribute, toIcebergValue(sparkValue, supportBoolean = false))

  private def convertGreaterThanOrEqual(attribute: String, sparkValue: Any): Expression =
    Expressions.greaterThanOrEqual(attribute, toIcebergValue(sparkValue, supportBoolean = false))
}
