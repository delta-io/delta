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

import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoStatement, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rule to maintain backward compatibility for enabling schema evolution in Delta.
 * Spark introduced `withSchemaEvolution` on insert plan nodes, but Delta checks writer option
 * `mergeSchema`. This rules sets that writer option whenever schema evolution is enabled on
 * an insert plan node.
 */
object PropagateSchemaEvolutionWriteOptionShims extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case a: AppendData
        if a.withSchemaEvolution && !a.writeOptions.contains(DeltaOptions.MERGE_SCHEMA_OPTION) =>
      a.copy(writeOptions = a.writeOptions + (DeltaOptions.MERGE_SCHEMA_OPTION -> "true"))
    case o: OverwriteByExpression
        if o.withSchemaEvolution && !o.writeOptions.contains(DeltaOptions.MERGE_SCHEMA_OPTION) =>
      o.copy(writeOptions = o.writeOptions + (DeltaOptions.MERGE_SCHEMA_OPTION  -> "true"))
    case o: OverwritePartitionsDynamic
        if o.withSchemaEvolution && !o.writeOptions.contains(DeltaOptions.MERGE_SCHEMA_OPTION) =>
      o.copy(writeOptions = o.writeOptions + (DeltaOptions.MERGE_SCHEMA_OPTION  -> "true"))
  }
}
