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

import org.apache.spark.sql.SparkSession

/**
 * Spark 4.2 and above: pass-through shim that delegates to the actual [[SetDSv2SchemaEvolution]]
 * rule.
 *
 * This pre-resolution rule sets `withSchemaEvolution = true` on plan nodes when schema
 * evolution is enabled in Delta via either:
 * - the dataframe writer option `.option("mergeSchema", "true")`
 * - the session-level spark conf `spark.databricks.delta.schema.autoMerge.enabled`
 */
class SetDSv2SchemaEvolutionShims(session: SparkSession) extends SetDSv2SchemaEvolution(session)
