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

package io.delta.sharing.spark

import java.util.Locale

import io.delta.sharing.client.util.{ConfUtils, JsonUtils}
import io.delta.sharing.filters.{AndOp, BaseOp, OpConverter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{sources => src}
import org.apache.spark.sql.catalyst.{expressions => expr}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Converts filters into the JSON predicate hint the Delta Sharing server uses for server-side file
 * skipping. Both read paths build the hint here, so they send the server the same thing:
 *   - the V1 path ([[DeltaSharingFileIndex]]) already holds Catalyst expressions and calls
 *     [[convert]] directly;
 *   - the DSv2 path ([[DeltaSharingDSV2Utils]]) receives untyped `sources.Filter`s from the
 *     `SupportsPushDownFilters` API and calls [[fromPushedFilters]], which bridges them to Catalyst
 *     expressions first.
 *
 * The hint is best-effort: the server is not required to apply it, so callers must still evaluate
 * every filter after the scan. Consequently any conversion failure (an unsupported filter, a bad
 * expression) degrades to [[None]] rather than erroring -- a missing hint only forgoes server-side
 * skipping, never correctness.
 */
object DeltaSharingJsonPredicates extends Logging {

  /**
   * Convert partition and data filter expressions to a json predicate:
   *   - the whole conversion is off unless `jsonPredicateHints.enabled` (default on);
   *   - data filters are converted only under `jsonPredicateV2Hints.enabled` (default off), so with
   *     defaults only partition filters reach the server.
   * Partition and data predicates are AND-ed. A conversion error on either side drops just that
   * side (logged, not thrown).
   *
   * @param partitionFilters filters referencing only partition columns.
   * @param dataFilters      filters referencing at least one non-partition column.
   * @param conf             session conf supplying the two feature gates.
   * @return the serialized json predicate, or None when disabled or nothing converted.
   */
  def convert(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      conf: SQLConf): Option[String] = {
    if (!ConfUtils.jsonPredicatesEnabled(conf)) {
      return None
    }

    // Convert the partition filters.
    val partitionOp = try {
      OpConverter.convert(partitionFilters)
    } catch {
      case e: Exception =>
        logError("Error while converting partition filters: " + e)
        None
    }

    // If V2 predicates are enabled, also convert the data filters.
    val dataOp = try {
      if (ConfUtils.jsonPredicatesV2Enabled(conf)) {
        logInfo("Converting data filters")
        OpConverter.convert(dataFilters)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError("Error while converting data filters: " + e)
        None
    }

    // Combine partition and data filters using an AND operation.
    val combinedOp = if (partitionOp.isDefined && dataOp.isDefined) {
      Some(AndOp(Seq(partitionOp.get, dataOp.get)))
    } else if (partitionOp.isDefined) {
      partitionOp
    } else {
      dataOp
    }
    logInfo("Using combined predicate: " + combinedOp)

    if (combinedOp.isDefined) {
      Some(JsonUtils.toJson[BaseOp](combinedOp.get))
    } else {
      None
    }
  }

  /**
   * DSv2 entry point: build the json predicate hint from the `sources.Filter`s the
   * `SupportsPushDownFilters` API hands the scan builder.
   *
   * The V1 path receives Catalyst expressions already split into partition / data filters by
   * `FileSourceStrategy`; the DSv2 API instead hands over untyped `sources.Filter`s, so this
   * bridges each to a Catalyst expression against `schema` (needed for the column type and
   * nullability [[OpConverter]] relies on) and splits them into partition vs data filters by
   * referenced column before delegating to [[convert]]. A filter that does not map to a supported
   * expression is dropped; because the hint is advisory, a dropped filter only forgoes server-side
   * skipping.
   *
   * @param filters          the pushed filters, ANDed together per the DSv2 contract.
   * @param schema           the full table schema, for column type / nullability lookup.
   * @param partitionColumns the table's partition column names (case-insensitive).
   * @param conf             session conf supplying the two feature gates.
   */
  def fromPushedFilters(
      filters: Array[src.Filter],
      schema: StructType,
      partitionColumns: Set[String],
      conf: SQLConf): Option[String] = {
    val partCols = partitionColumns.map(_.toLowerCase(Locale.ROOT))
    val partitionFilters = Seq.newBuilder[Expression]
    val dataFilters = Seq.newBuilder[Expression]
    filters.foreach { filter =>
      toExpression(filter, schema).foreach { e =>
        val refs = filter.references.map(_.toLowerCase(Locale.ROOT))
        // A filter is a partition filter only if every column it references is a partition column
        // (matching V1's FileSourceStrategy split); anything else -- including no-column filters --
        // is treated as a data filter.
        if (refs.nonEmpty && refs.forall(partCols.contains)) {
          partitionFilters += e
        } else {
          dataFilters += e
        }
      }
    }
    convert(partitionFilters.result(), dataFilters.result(), conf)
  }

  /**
   * Bridge a single `sources.Filter` to the Catalyst expression [[OpConverter]] understands, or
   * None if it references an unknown column or is a shape OpConverter cannot convert (e.g. the
   * string-prefix filters). Column sides become an [[expr.AttributeReference]] (not a
   * `BoundReference`, which OpConverter rejects) carrying the schema field's type; literals are
   * coerced to that same type.
   *
   * Nested-column filters (e.g. `s.a = 1`, which Spark pushes with the dotted attribute `"s.a"`)
   * are intentionally dropped: the exact-name lookup below finds no top-level field `"s.a"` and
   * returns None. This is not a gap to "fix" -- the sharing json predicate cannot represent nested
   * columns correctly. OpConverter only accepts a top-level `AttributeReference` (or a `Cast` of
   * one) as a column leaf, and the server evaluates a `ColumnOp` by a flat partition-value lookup
   * on its `name`. Emitting a `ColumnOp("s.a", ...)` would either be rejected by OpConverter or, if
   * a partition column were literally named `"s.a"`, match the wrong column and let the server skip
   * files that actually hold matching rows. Dropping it only forgoes server-side skipping (Spark
   * still applies the filter after the scan), which is always correct.
   */
  private def toExpression(filter: src.Filter, schema: StructType): Option[Expression] = {
    def attr(name: String): Option[expr.AttributeReference] =
      schema.fields.find(_.name == name).map { f =>
        expr.AttributeReference(f.name, f.dataType, f.nullable)()
      }
    def compare(
        name: String,
        value: Any,
        build: (Expression, Expression) => Expression): Option[Expression] =
      schema.fields.find(_.name == name).map { f =>
        build(
          expr.AttributeReference(f.name, f.dataType, f.nullable)(),
          expr.Literal.create(value, f.dataType))
      }

    filter match {
      case src.EqualTo(a, v) => compare(a, v, expr.EqualTo)
      case src.EqualNullSafe(a, v) => compare(a, v, expr.EqualNullSafe)
      case src.GreaterThan(a, v) => compare(a, v, expr.GreaterThan)
      case src.GreaterThanOrEqual(a, v) => compare(a, v, expr.GreaterThanOrEqual)
      case src.LessThan(a, v) => compare(a, v, expr.LessThan)
      case src.LessThanOrEqual(a, v) => compare(a, v, expr.LessThanOrEqual)
      case src.In(a, values) =>
        schema.fields.find(_.name == a).map { f =>
          val ref = expr.AttributeReference(f.name, f.dataType, f.nullable)()
          expr.In(ref, values.map(v => expr.Literal.create(v, f.dataType)).toSeq)
        }
      case src.IsNull(a) => attr(a).map(expr.IsNull)
      case src.IsNotNull(a) => attr(a).map(expr.IsNotNull)
      case src.And(left, right) =>
        for (l <- toExpression(left, schema); r <- toExpression(right, schema))
          yield expr.And(l, r)
      case src.Or(left, right) =>
        for (l <- toExpression(left, schema); r <- toExpression(right, schema))
          yield expr.Or(l, r)
      case src.Not(child) => toExpression(child, schema).map(expr.Not)
      case _ => None
    }
  }
}
