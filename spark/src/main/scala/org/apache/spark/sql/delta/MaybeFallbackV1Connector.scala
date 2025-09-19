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

package org.apache.spark.sql.delta
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import io.delta.kernel.spark.catalog.SparkTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils}
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MaybeFallbackV1Connector(session: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isReadOnly(plan)) return plan
    def replaceKernelWithFallback(node: LogicalPlan): LogicalPlan = {
      node.resolveOperatorsDown {
        case Batch(fallback) => fallback
        case Streaming(fallback) => fallback
      }
    }

    plan.resolveOperatorsDown {
      case i @ InsertIntoStatement(table, part, cols, query, overwrite, byName, ifNotExists) =>
        val newTable = replaceKernelWithFallback(table)
        i.copy(table = newTable)
      case Batch(fallback) => fallback
      case Streaming(fallback) => fallback
    }
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement])
  }

  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: SparkTable =>
        val v1CatalogTable = d.getV1CatalogTable()
        if(v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          Some(dsv2.copy(table = DeltaTableV2(session, new Path(catalogTable.location),
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

  private def getStreamingRelation(
       table: CatalogTable, extraOptions: CaseInsensitiveStringMap): StreamingRelation = {
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
