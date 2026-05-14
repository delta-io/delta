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

import scala.jdk.OptionConverters._

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext
import org.apache.spark.sql.delta.commands.cdc.CDCReader

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier

/**
 * Plumbs read options into a V2 [[StreamingRelationV2]]'s [[SparkTable]] when those options
 * change a property the table derives from them.
 */
class ApplyV2ReadOptions extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case r @ StreamingRelationV2(_, _, sparkTable: SparkTable, extraOptions, _, _, Some(ident), _)
        if CDCReader.isCDCRead(extraOptions)
          && !r.output.exists(a => CDCSchemaContext.isCDCColumn(a.name)) =>
      val newTable = sparkTable.getCatalogTable.toScala match {
        case Some(catalogTable) => new SparkTable(ident, catalogTable, extraOptions)
        case None => new SparkTable(ident, sparkTable.getTablePath.toString, extraOptions)
      }
      StreamingRelationV2(
        source = None,
        sourceName = r.sourceName,
        table = newTable,
        extraOptions = extraOptions,
        output = toAttributes(newTable.schema()),
        catalog = r.catalog,
        identifier = Some(ident),
        v1Relation = None)
  }
}
