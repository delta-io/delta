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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoStatement, LogicalPlan, MergeIntoTable, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Pre-resolution rule that sets `withSchemaEvolution = true` on plan nodes when schema
 * evolution is enabled in Delta via either:
 * - the dataframe writer option `.option("mergeSchema", "true")`
 * - the session-level spark conf `spark.databricks.delta.schema.autoMerge.enabled`
 *
 * Spark only cares about `withSchemaEvolution`. This allows Spark to handle schema evolution for
 * these write operations when using DSv2.
 */
class SetDSv2SchemaEvolutionShims(session: SparkSession) extends Rule[LogicalPlan] {

  private def canMergeSchema(writeOptions: Map[String, String]): Boolean =
    new DeltaOptions(writeOptions, session.sessionState.conf).canMergeSchema

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (session.sessionState.conf.getConf(DeltaSQLConf.V2_ENABLE_MODE) == "NONE") return plan

    // This rule runs during pre-resolution so it cannot identify the target table type. It applies
    // to all DSv2 tables that declare AUTOMATIC_SCHEMA_EVOLUTION when a Delta-specific config is
    // used to enable schema evolution.
    // Note: the rule can't run later as it must run before [[ResolveSchemaEvolution]].
    plan.resolveOperatorsDown {
      case a: AppendData if !a.withSchemaEvolution && canMergeSchema(a.writeOptions) =>
        a.copy(withSchemaEvolution = true)
      case o: OverwriteByExpression
          if !o.withSchemaEvolution && canMergeSchema(o.writeOptions) =>
        o.copy(withSchemaEvolution = true)
      case o: OverwritePartitionsDynamic
          if !o.withSchemaEvolution && canMergeSchema(o.writeOptions) =>
        o.copy(withSchemaEvolution = true)
      // INSERT INTO (SQL) and MERGE don't support passing writer options, so only check the spark
      // conf for schema evolution.
      case i: InsertIntoStatement if !i.withSchemaEvolution && canMergeSchema(Map.empty) =>
        i.copy(withSchemaEvolution = true)
      case m: MergeIntoTable if !m.withSchemaEvolution && canMergeSchema(Map.empty) =>
        m.copy(withSchemaEvolution = true)
    }
  }
}
