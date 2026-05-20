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

package io.delta.internal

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.read.MetadataEvolutionHandler
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext
import org.apache.spark.sql.delta.commands.cdc.CDCReader

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes

/**
 * TODO(#5319): remove this class after Spark supports directly create table reflect cdc/trackingLog
 * Plumbs read options into a V2 [[StreamingRelationV2]]'s [[SparkTable]] when those options
 * change a property the table derives from them.
 */
class ApplyV2ReadOptions extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s @ StreamingRelationV2(_, _, table: SparkTable, extraOptions, _, _, _, _)
        if (CDCReader.isCDCRead(extraOptions) &&
              !s.output.exists(a => CDCSchemaContext.isCDCColumn(a.name))) ||
           MetadataEvolutionHandler.shouldPropagateSchemaTrackingToTable(
             extraOptions, table.getOptions) =>
      val merged = new java.util.HashMap[String, String]()
      merged.putAll(table.getOptions)
      merged.putAll(extraOptions.asCaseSensitiveMap())
      val rebuilt = if (table.getCatalogTable.isPresent) {
        new SparkTable(table.getIdentifier, table.getCatalogTable.get, merged)
      } else {
        new SparkTable(table.getIdentifier, table.getTablePath.toString, merged)
      }
      s.copy(
        source = None,
        table = rebuilt,
        output = toAttributes(rebuilt.schema),
        v1Relation = None)
  }
}
