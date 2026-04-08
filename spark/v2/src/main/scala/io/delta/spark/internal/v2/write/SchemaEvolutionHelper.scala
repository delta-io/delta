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
package io.delta.spark.internal.v2.write

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.types.StructType

/**
 * Scala helper bridging Spark-internal schema merging logic with the Java-based DSv2 write path.
 */
object SchemaEvolutionHelper {

  /**
   * Merges a data schema into an existing table schema, returning the combined schema.
   * New columns in `dataSchema` that are not in `tableSchema` are appended.
   */
  def mergeSchemas(tableSchema: StructType, dataSchema: StructType): StructType = {
    SchemaMergingUtils.mergeSchemas(tableSchema, dataSchema)
  }
}
