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

package org.apache.spark.sql.delta.optimizer

import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * Replaces a batch Change Data Feed relation ([[DeltaCDFRelation]]) with the plan that reads its
 * change data.
 *
 * [[DeltaCDFRelation]] is a `CatalystScan`: during physical planning, `buildScan` plans the change
 * data as a separate DataFrame and returns it as an RDD of external rows. The file scans in that
 * DataFrame are hidden from the query that reads the relation. Its optimizer cannot prune their
 * columns, the relation has no statistics, every row is converted to an external `Row` and back,
 * and the scans run as an RDD outside the query in the SQL UI.
 *
 * This rule inlines the change-data plan into the query instead. It runs in the operator
 * optimization batch, so the projections and filters above the relation are pushed into the
 * inlined file scans in the same pass.
 *
 * Version resolution is unchanged. A query is optimized on its first action, the same point at
 * which `buildScan` ran before, so a read without an ending version includes every commit made
 * before its first action. Re-running the same DataFrame reuses its plan, and a DataFrame derived
 * from it is planned again and reads up to the latest version, as before.
 *
 * The relation is left as is if a column of the change-data plan has a different type than the
 * relation's column, ignoring field metadata, or is NOT NULL where the relation's column is
 * nullable. Otherwise the optimizer could rely on nullability the relation doesn't promise. The
 * relation relaxes nullability that was tightened within the read range (see
 * [[DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED]]), but such a range fails
 * validation while the change-data plan is built, before this check.
 */
case class InlineDeltaCDFRelation(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    // With DELTA_CDF_SKIP_NONDETERMINISTIC_FILTER_PUSHDOWN disabled, `buildScan` also evaluates
    // nondeterministic predicates inside the change-data query, which that setting exists to
    // restore. Keep the relation so it still does.
    if (!conf.getConf(DeltaSQLConf.DELTA_CDF_BATCH_INLINE_SCAN) ||
        !conf.getConf(DeltaSQLConf.DELTA_CDF_SKIP_NONDETERMINISTIC_FILTER_PUSHDOWN)) {
      return plan
    }
    // The replacement contains no DeltaCDFRelation, so re-running this rule within the
    // fixed-point batch is a no-op.
    plan.transformUp {
      case relation: LogicalRelation if !relation.isStreaming =>
        relation.relation match {
          // An empty CDF range is represented by a relation without a starting version, whose
          // scan returns no rows. There is nothing to inline.
          case cdf: DeltaCDFRelation if cdf.startingVersion.isDefined =>
            inline(relation, cdf).getOrElse(relation)
          case _ => relation
        }
    }
  }

  private def inline(relation: LogicalRelation, cdf: DeltaCDFRelation): Option[LogicalPlan] = {
    // Any error here is the error `buildScan` would have thrown during physical planning, e.g.
    // an unreadable version range. Let it surface rather than falling back.
    val changes = cdf.changesDF().queryExecution.optimizedPlan
    val resolver = session.sessionState.conf.resolver
    val projectList = relation.output.map { attr =>
      changes.output.find(out => resolver(out.name, attr.name)).flatMap(bindTo(attr, _))
    }
    if (projectList.forall(_.isDefined)) {
      Some(Project(projectList.flatten, changes))
    } else {
      None
    }
  }

  /**
   * Exposes `out`, a column of the change-data plan, under the name, expression ID and metadata
   * of `attr`, the relation column the rest of the query references. Returns None if `out` has a
   * different type, or is NOT NULL where `attr` is nullable.
   */
  private def bindTo(attr: Attribute, out: Attribute): Option[NamedExpression] = {
    if ((out.nullable || !attr.nullable) && sameTypeIgnoringMetadata(attr.dataType, out.dataType)) {
      Some(Alias(out, attr.name)(
        exprId = attr.exprId,
        qualifier = attr.qualifier,
        explicitMetadata = Some(attr.metadata)))
    } else {
      None
    }
  }

  /**
   * Whether `left` and `right` are the same type, including the nullability of nested fields,
   * array elements and map values, ignoring field metadata. The relation's schema has Delta's
   * internal field metadata (e.g. column mapping) removed, while the change-data plan keeps it.
   */
  private def sameTypeIgnoringMetadata(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (l: StructType, r: StructType) =>
        l.length == r.length && l.fields.zip(r.fields).forall { case (lf, rf) =>
          lf.name == rf.name && lf.nullable == rf.nullable &&
            sameTypeIgnoringMetadata(lf.dataType, rf.dataType)
        }
      case (l: ArrayType, r: ArrayType) =>
        l.containsNull == r.containsNull && sameTypeIgnoringMetadata(l.elementType, r.elementType)
      case (l: MapType, r: MapType) =>
        l.valueContainsNull == r.valueContainsNull &&
          sameTypeIgnoringMetadata(l.keyType, r.keyType) &&
          sameTypeIgnoringMetadata(l.valueType, r.valueType)
      case _ => left == right
    }
  }
}
