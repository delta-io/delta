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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{
  And, AttributeReference, EqualNullSafe, EqualTo, Expression, GreaterThan,
  GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual,
  Like, Literal, Not, Or
}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

/**
 * Converts Spark V2 [[org.apache.spark.sql.sources.Filter]] objects to
 * resolved Catalyst [[Expression]]s suitable for the V1 data skipping pipeline.
 *
 * The V1 pipeline ([[DataSkippingFilterPlanner]], [[DataFiltersBuilder]])
 * pattern-matches on [[AttributeReference]], so column references must be
 * resolved against the table schema before they can be used.
 */
private[delta] object FilterConversionUtils {

  /**
   * Converts an array of Spark Filter objects to a sequence of resolved
   * Catalyst Expressions. Each filter becomes one expression.
   *
   * @param filters  Spark V2 pushdown filters
   * @param schema   Table schema used to resolve column references
   * @return Resolved expressions ready for the V1 filter pipeline
   */
  def filtersToExpressions(
      filters: Array[sources.Filter],
      schema: StructType): Seq[Expression] = {
    val attrMap: Map[String, (org.apache.spark.sql.types.DataType, Boolean)] =
      schema.fields.map(f => f.name.toLowerCase(Locale.ROOT) -> (f.dataType, f.nullable)).toMap
    filters.map(f => translateFilter(f, attrMap)).toSeq
  }

  /**
   * Translates a single Spark Filter to a resolved Catalyst Expression.
   */
  private def translateFilter(
      filter: sources.Filter,
      attrMap: Map[String, (org.apache.spark.sql.types.DataType, Boolean)]): Expression = {
    filter match {
      case sources.EqualTo(attr, value) =>
        EqualTo(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.EqualNullSafe(attr, value) =>
        EqualNullSafe(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.GreaterThan(attr, value) =>
        GreaterThan(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.GreaterThanOrEqual(attr, value) =>
        GreaterThanOrEqual(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.LessThan(attr, value) =>
        LessThan(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.LessThanOrEqual(attr, value) =>
        LessThanOrEqual(resolveAttr(attr, attrMap), Literal.create(value))
      case sources.In(attr, values) =>
        In(resolveAttr(attr, attrMap), values.map(Literal.create(_)))
      case sources.IsNull(attr) =>
        IsNull(resolveAttr(attr, attrMap))
      case sources.IsNotNull(attr) =>
        IsNotNull(resolveAttr(attr, attrMap))
      case sources.Not(child) =>
        Not(translateFilter(child, attrMap))
      case sources.And(left, right) =>
        And(translateFilter(left, attrMap), translateFilter(right, attrMap))
      case sources.Or(left, right) =>
        Or(translateFilter(left, attrMap), translateFilter(right, attrMap))
      case sources.StringStartsWith(attr, value) =>
        new Like(resolveAttr(attr, attrMap), Literal.create(s"${value}%"))
      case sources.StringEndsWith(attr, value) =>
        new Like(resolveAttr(attr, attrMap), Literal.create(s"%${value}"))
      case sources.StringContains(attr, value) =>
        new Like(resolveAttr(attr, attrMap), Literal.create(s"%${value}%"))
      case sources.AlwaysTrue() => Literal.TrueLiteral
      case sources.AlwaysFalse() => Literal.FalseLiteral
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: $other")
    }
  }

  /**
   * Resolves a column name to an [[AttributeReference]] using the table schema.
   */
  private def resolveAttr(
      name: String,
      attrMap: Map[String, (org.apache.spark.sql.types.DataType, Boolean)]
  ): AttributeReference = {
    attrMap.get(name.toLowerCase(Locale.ROOT)) match {
      case Some((dataType, nullable)) =>
        AttributeReference(name, dataType, nullable)()
      case None =>
        throw new IllegalArgumentException(
          s"Cannot resolve column '$name' in the table schema.")
    }
  }
}
