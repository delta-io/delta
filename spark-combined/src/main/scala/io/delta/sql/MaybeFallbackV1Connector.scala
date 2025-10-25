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

package io.delta.sql

import io.delta.kernel.spark.catalog.SparkTable
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoStatement, LogicalPlan, MergeIntoTable, OverwriteByExpression}
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeInto
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MaybeFallbackV1Connector(session: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    def replaceKernelWithFallback(node: LogicalPlan): LogicalPlan = {
      node.resolveOperatorsDown {
        case Batch(fallback) => fallback
        case Streaming(fallback) => fallback
        case ResolvedTableWithSparkTable(fallback) => fallback
      }
    }
    plan.resolveOperatorsDown {
      // Handle ResolvedTable with SparkTable (for commands like DESCRIBE HISTORY)
      case ResolvedTableWithSparkTable(fallback) =>
        fallback
      // Handle MERGE INTO (Spark generic MergeIntoTable)
      case m @ MergeIntoTable(targetTable, sourceTable, mergeCondition,
        matchedActions, notMatchedActions, notMatchedBySourceActions) =>
        val newTarget = replaceKernelWithFallback(targetTable)
        val newSource = replaceKernelWithFallback(sourceTable)
        m.copy(targetTable = newTarget, sourceTable = newSource)

      // Handle MERGE INTO (DeltaMergeInto)
      case m @ DeltaMergeInto(target, source, condition, matched, notMatched, notMatchedBySource,
        withSchemaEvolution, finalSchema) =>
        val newTarget = replaceKernelWithFallback(target)
        val newSource = replaceKernelWithFallback(source)
        m.copy(target = newTarget, source = newSource)

      // Handle V1 INSERT INTO
      case i @ InsertIntoStatement(table, part, cols, query, overwrite, byName, ifNotExists) =>
        // scalastyle:off println
        println("[MaybeFallbackV1] InsertIntoStatement")
        // scalastyle:on println
        val newTable = replaceKernelWithFallback(table)
        i.copy(table = newTable)

      // Handle V2 AppendData (DataFrameWriterV2.append)
      case a @ AppendData(Batch(fallback), _, _, _, _, _) =>
        // scalastyle:off println
        println("[MaybeFallbackV1] AppendData -> falling back")
        // scalastyle:on println
        a.copy(table = fallback)

      // Handle V2 OverwriteByExpression (DataFrameWriterV2.overwrite)
      case o @ OverwriteByExpression(Batch(fallback), _, _, _, _, _, _) =>
        o.copy(table = fallback)

      // Handle batch reads
      case Batch(fallback) if !isReadOnly(plan) =>
        fallback

      // Handle streaming
      case Streaming(fallback) if !isReadOnly(plan) =>
        fallback

      // Print unhandled COMMAND nodes
      case other if other.containsPattern(COMMAND) =>
        other
    }
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement]) &&
      !plan.exists(_.isInstanceOf[DeltaCommand])
  }

  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: SparkTable =>
        val v1CatalogTable = d.getV1CatalogTable()
        if (v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          Some(dsv2.copy(table = DeltaTableV2(
            session,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(catalogTable.identifier.toString))))
        } else {
          Some(dsv2.copy(table = DeltaTableV2(session, new Path(d.name()))))
        }
      case _ => None
    }
  }
  object Streaming {
    def unapply(dsv2: StreamingRelationV2): Option[StreamingRelation] = dsv2.table match {
      case d: SparkTable =>
        // Streaming's fallback is not via DeltaAnalysis, so directly create v1 streaming relation.
        val v1CatalogTable = d.getV1CatalogTable()
        assert(v1CatalogTable.isPresent())
        val catalogTable = v1CatalogTable.get()
        Some(getStreamingRelation(catalogTable, dsv2.extraOptions))
      case _ => None
    }
  }

  object ResolvedTableWithSparkTable {
    def unapply(resolved: ResolvedTable): Option[ResolvedTable] = resolved.table match {
      case s: SparkTable =>
        val v1CatalogTable = s.getV1CatalogTable()
        if (v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          val deltaTableV2 = DeltaTableV2(
            session,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(catalogTable.identifier.toString))
          Some(resolved.copy(table = deltaTableV2))
        } else {
          val deltaTableV2 = DeltaTableV2(session, new Path(s.name()))
          Some(resolved.copy(table = deltaTableV2))
        }
      case _ => None
    }
  }

  private def getStreamingRelation(
                                    table: CatalogTable,
                                    extraOptions: CaseInsensitiveStringMap): StreamingRelation = {
    val dsOptions = DataSourceUtils.generateDatasourceOptions(extraOptions, table)
    val dataSource = DataSource(
      SparkSession.active,
      className = table.provider.get,
      userSpecifiedSchema = if (!DeltaTableUtils.isDeltaTable(table)) {
        Some(table.schema)
      } else None,
      options = dsOptions,
      catalogTable = Some(table))
    StreamingRelation(dataSource)
  }
}
