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
  And => CatalystAnd, AttributeReference, Contains, EqualNullSafe, EqualTo,
  EndsWith, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull,
  IsNull, LessThan, LessThanOrEqual, Literal, Not => CatalystNot,
  Or => CatalystOr, StartsWith
}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

/**
 * Java-callable bridge for Catalyst/Filter/Predicate conversions that
 * require `private[sql]` or `protected[sql]` Spark APIs.
 *
 * The V2 Java code (in `io.delta.spark.internal.v2.read`) cannot directly
 * call these Spark internals. This Scala helper, living in the
 * `org.apache.spark.sql.delta` package, acts as the bridge.
 *
 * All scan orchestration (state reconstruction, filter planning,
 * execution) is now handled by the shared V1 components directly:
 *  - [[DefaultStateProvider.fromPaths]] for state from raw log paths
 *  - [[DefaultDeltaScanExecutor.filesForScanAsDF]] for plan + execute
 */
private[delta] object DistributedScanHelper {

  /**
   * Converts a single Catalyst [[Expression]] to a V1 [[Filter]] using Spark's
   * [[DataSourceStrategy.translateFilter]].  Returns `Optional.empty()` when the
   * expression cannot be represented as a V1 filter.
   */
  def catalystToFilter(expr: Expression): java.util.Optional[Filter] = {
    DataSourceStrategy.translateFilter(expr, supportNestedPredicatePushdown = true) match {
      case Some(f) => java.util.Optional.of(f)
      case None => java.util.Optional.empty()
    }
  }

  /**
   * Converts an array of V1 [[Filter]]s to V2 [[V2Predicate]]s.
   * `Filter.toV2` is `private[sql]`, so this helper is needed for Java callers.
   */
  def filtersToV2Predicates(filters: Array[Filter]): Array[V2Predicate] = {
    if (filters == null || filters.isEmpty) return Array.empty
    filters.map(_.toV2)
  }

  /**
   * Resolves an array of V1 [[Filter]]s to a `Seq[Expression]` against a schema.
   * In production, expressions arrive directly via `SupportsPushDownCatalystFilters`.
   * This method exists so that Java test code (which constructs `Filter` objects)
   * can still call `SparkScanBuilder.pushFilters(Seq[Expression])`.
   */
  def resolveFiltersToExprSeq(
      filters: Array[Filter],
      schema: StructType): scala.collection.immutable.Seq[Expression] = {
    if (filters == null || filters.isEmpty) return Nil
    val attrMap: Map[String, (DataType, Boolean)] =
      schema.fields.map(f =>
        f.name.toLowerCase(Locale.ROOT) -> (f.dataType, f.nullable)
      ).toMap
    filters.map(f => resolveFilter(f, attrMap)).toSeq
  }

  /** Resolves a single V1 Filter to a Catalyst Expression using the attribute map. */
  private def resolveFilter(
      filter: Filter,
      attrMap: Map[String, (DataType, Boolean)]): Expression = {
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
        CatalystNot(resolveFilter(child, attrMap))
      case sources.And(left, right) =>
        CatalystAnd(resolveFilter(left, attrMap), resolveFilter(right, attrMap))
      case sources.Or(left, right) =>
        CatalystOr(resolveFilter(left, attrMap), resolveFilter(right, attrMap))
      case sources.StringStartsWith(attr, value) =>
        StartsWith(resolveAttr(attr, attrMap), Literal.create(value, StringType))
      case sources.StringEndsWith(attr, value) =>
        EndsWith(resolveAttr(attr, attrMap), Literal.create(value, StringType))
      case sources.StringContains(attr, value) =>
        Contains(resolveAttr(attr, attrMap), Literal.create(value, StringType))
      case sources.AlwaysTrue() => Literal.TrueLiteral
      case sources.AlwaysFalse() => Literal.FalseLiteral
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: $other")
    }
  }

  private def resolveAttr(
      name: String,
      attrMap: Map[String, (DataType, Boolean)]): AttributeReference = {
    attrMap.get(name.toLowerCase(Locale.ROOT)) match {
      case Some((dataType, nullable)) =>
        AttributeReference(name, dataType, nullable)()
      case None =>
        throw new IllegalArgumentException(
          s"Cannot resolve column '$name' in the table schema.")
    }
  }
}
