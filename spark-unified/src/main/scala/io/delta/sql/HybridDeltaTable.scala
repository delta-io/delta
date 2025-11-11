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

package io.delta.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCapability}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.StructType
import io.delta.kernel.spark.table.SparkTable

/**
 * A hybrid Delta table implementation that declares MICRO_BATCH_READ capability
 * to enable Spark to create StreamingRelationV2 for streaming reads.
 *
 * This table is returned by the catalog for all operations. An analyzer rule then
 * replaces it with the appropriate concrete implementation:
 * - SparkTable (V2/Kernel) for streaming reads
 * - DeltaTableV2 (V1/DeltaLog) for batch reads/writes
 *
 * This class itself is never used for actual read/write operations - it only serves
 * as a placeholder during catalog resolution to signal the MICRO_BATCH_READ capability.
 */
class HybridDeltaTable(
    spark: SparkSession,
    identifier: Identifier,
    tablePath: String,
    options: Map[String, String])
  extends Table {

  // Lazily initialize V1 table
  private lazy val v1Table: DeltaTableV2 = {
    new DeltaTableV2(
      spark,
      new org.apache.hadoop.fs.Path(tablePath),
      catalogTable = None,
      tableIdentifier = Some(identifier.toString),
      options = options
    )
  }

  // Lazily initialize V2 table
  private lazy val v2Table: SparkTable = {
    new SparkTable(identifier, tablePath, options.asJava)
  }

  override def name(): String = identifier.toString

  override def schema(): StructType = v1Table.schema()

  override def capabilities(): java.util.Set[TableCapability] = {
    // Add MICRO_BATCH_READ capability to enable StreamingRelationV2 creation
    // This capability is required for Spark to create StreamingRelationV2 instead of StreamingRelation
    val v1Caps = v1Table.capabilities().asScala.toSet
    val hybridCaps = v1Caps + TableCapability.MICRO_BATCH_READ
    hybridCaps.asJava
  }
  
  /**
   * Returns the underlying DeltaTableV2 for cases where we need to use V1
   * (e.g., for batch operations before DeltaAnalysis).
   */
  def getUnderlyingDeltaTableV2(): DeltaTableV2 = v1Table
  
  /**
   * Returns the SparkTable for cases where we want to use V2
   * (e.g., for streaming reads).
   */
  def getSparkTable(): SparkTable = v2Table
}
