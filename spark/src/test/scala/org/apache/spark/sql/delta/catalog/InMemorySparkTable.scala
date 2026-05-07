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

package org.apache.spark.sql.delta.catalog

import java.util

import org.apache.spark.sql.delta.SparkTableShims

import org.apache.spark.sql.connector.catalog.{InMemoryRowLevelOperationTable, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * In-memory DSv2 table used as a test stand-in for SparkTable (the Kernel-based Delta V2
 * connector).
 *
 * Created by [[InMemoryDeltaCatalog]] when used as the session catalog in tests.
 */
class InMemorySparkTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryRowLevelOperationTable(
    name, schema, partitioning, properties) {

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability](super.capabilities())
    SparkTableShims.schemaEvolutionCapability.foreach(caps.add)
    caps
  }

  // Force DELETE to go through the SupportsRowLevelOperations path instead of
  // the SupportsDeleteV2.deleteWhere path inherited from InMemoryTable, which
  // only supports filtering by partition columns.
  override def canDeleteWhere(filters: Array[Filter]): Boolean = false
}
