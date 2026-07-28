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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * An analyzer resolution rule that handles implicit casting for all V2WriteCommands targeting
 * Delta tables via DataFrame by-name writes. This rule runs BEFORE [[DeltaAnalysis]].
 */
case class DeltaImplicitCast(session: SparkSession)
    extends Rule[LogicalPlan] with DeltaInsertCastSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!session.conf.get(DeltaSQLConf.DELTA_DF_WRITE_ALLOW_IMPLICIT_CASTS)) return plan
    plan.resolveOperatorsDown {
      case w @ DeltaV2WriteCommand(r, d, writeOptions)
          if shouldApplyImplicitCast(w, r, d, writeOptions) =>
        val projection = resolveQueryColumnsByName(
          query = w.query,
          targetAttrs = r.output,
          deltaTable = d,
          writeOptions = writeOptions,
          allowSchemaEvolution = true,
          isDfByNameInsert = true)
        val newPlan = if (projection != w.query) {
          w match {
            case o: OverwriteByExpression =>
              val aliases = AttributeMap(o.query.output.zip(projection.output).collect {
                case (l: AttributeReference, r: AttributeReference) if !l.sameRef(r) => (l, r)
              })
              val newDeleteExpr = o.deleteExpr.transformUp {
                case a: AttributeReference => aliases.getOrElse(a, a)
              }
              o.copy(deleteExpr = newDeleteExpr, query = projection)
            case _ => w.withNewQuery(projection)
          }
        } else {
          w
        }
        newPlan match {
          case o: OverwritePartitionsDynamic =>
            DeltaDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)
          case other => other
        }
    }
  }

  /**
   * Returns true if this write command is a DataFrame by-name insert that needs implicit casting.
   * SQL inserts with requireImplicitCasting=true are handled in [[DeltaAnalysis]] instead.
   */
  private def shouldApplyImplicitCast(
      w: V2WriteCommand,
      r: DataSourceV2Relation,
      d: DeltaTableV2,
      writeOptions: Map[String, String]): Boolean = {
    w.isByName &&
    w.origin.sqlText.isEmpty &&
    !writeOptions.get(DeltaOptions.OVERWRITE_SCHEMA_OPTION).exists(_.toBoolean) &&
    needsSchemaAdjustmentByName(
      query = w.query,
      targetAttrs = r.output,
      deltaTable = d,
      writeOptions = writeOptions,
      isDfByNameInsert = true)
  }
}

/**
 * Extractor for certain [[V2WriteCommand]] whose resolved target is a Delta table. Yields the
 * [[DataSourceV2Relation]], the [[DeltaTableV2]], and the command's write options.
 */
object DeltaV2WriteCommand {
  def unapply(
      w: V2WriteCommand): Option[(DataSourceV2Relation, DeltaTableV2, Map[String, String])] = {
    if (!w.query.resolved) return None
    w match {
      case _: AppendData | _: OverwriteByExpression | _: OverwritePartitionsDynamic =>
      case _ => return None
    }
    w.table match {
      case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
        Some((r, r.table.asInstanceOf[DeltaTableV2], writeOptionsOf(w)))
      case _ => None
    }
  }

  private def writeOptionsOf(w: V2WriteCommand): Map[String, String] = w match {
    case a: AppendData => a.writeOptions
    case o: OverwriteByExpression => o.writeOptions
    case o: OverwritePartitionsDynamic => o.writeOptions
    case _ => Map.empty
  }
}

