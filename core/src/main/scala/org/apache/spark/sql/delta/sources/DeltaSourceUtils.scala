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

package org.apache.spark.sql.delta.sources

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter

object DeltaSourceUtils {
  val NAME = "delta"
  val ALT_NAME = "delta"

  // Batch relations don't pass partitioning columns to `CreatableRelationProvider`s, therefore
  // as a hack, we pass in the partitioning columns among the options.
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  // The metadata key recording the generation expression in a generated column's `StructField`.
  val GENERATION_EXPRESSION_METADATA_KEY = "delta.generationExpression"


  def isDeltaDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME || name.toLowerCase(Locale.ROOT) == ALT_NAME
  }

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(provider: Option[String]): Boolean = {
    provider.exists(isDeltaDataSourceName)
  }

  /** Creates Spark literals from a value exposed by the public Spark API. */
  private def createLiteral(value: Any): expressions.Literal = value match {
    case v: String => expressions.Literal.create(v)
    case v: Int => expressions.Literal.create(v)
    case v: Byte => expressions.Literal.create(v)
    case v: Short => expressions.Literal.create(v)
    case v: Long => expressions.Literal.create(v)
    case v: Double => expressions.Literal.create(v)
    case v: Float => expressions.Literal.create(v)
    case v: Boolean => expressions.Literal.create(v)
    case v: java.sql.Date => expressions.Literal.create(v)
    case v: java.sql.Timestamp => expressions.Literal.create(v)
    case v: BigDecimal => expressions.Literal.create(v)
  }

  /** Translates the public Spark Filter APIs into Spark internal expressions. */
  def translateFilters(filters: Array[Filter]): Expression = filters.map {
    case sources.EqualTo(attribute, value) =>
      expressions.EqualTo(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.EqualNullSafe(attribute, value) =>
      expressions.EqualNullSafe(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThan(attribute, value) =>
      expressions.GreaterThan(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThanOrEqual(attribute, value) =>
      expressions.GreaterThanOrEqual(
        UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThan(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThanOrEqual(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.In(attribute, values) =>
      expressions.In(UnresolvedAttribute(attribute), values.map(createLiteral))
    case sources.IsNull(attribute) => expressions.IsNull(UnresolvedAttribute(attribute))
    case sources.IsNotNull(attribute) => expressions.IsNotNull(UnresolvedAttribute(attribute))
    case sources.Not(otherFilter) => expressions.Not(translateFilters(Array(otherFilter)))
    case sources.And(filter1, filter2) =>
      expressions.And(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.Or(filter1, filter2) =>
      expressions.Or(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.StringStartsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"${value}%"))
    case sources.StringEndsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%${value}"))
    case sources.StringContains(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%${value}%"))
    case sources.AlwaysTrue() => expressions.Literal.TrueLiteral
    case sources.AlwaysFalse() => expressions.Literal.FalseLiteral
  }.reduce(expressions.And)
}
