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
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.delta.kernel.spark.table.SparkTable

/**
 * A hybrid Delta table implementation that declares MICRO_BATCH_READ capability
 * and implements SupportsRead to enable Spark to create StreamingRelationV2.
 *
 * This table is returned by the catalog for all operations. An analyzer rule then
 * replaces it with the appropriate concrete implementation:
 * - SparkTable (V2/Kernel) for streaming reads
 * - DeltaTableV2 (V1/DeltaLog) for batch reads/writes
 *
 * IMPORTANT: Spark's ResolveDataSource requires BOTH:
 * 1. SupportsRead interface (checked via pattern match)
 * 2. MICRO_BATCH_READ capability (checked via supportsAny())
 * Without both, Spark will create StreamingRelation (V1) instead of StreamingRelationV2.
 *
 * The newScanBuilder() and newWriteBuilder() methods should never be called in practice
 * because the analyzer rule replaces this table before physical planning, but they must
 * be implemented to satisfy the interface contracts.
 */
class HybridDeltaTable(
    spark: SparkSession,
    identifier: Identifier,
    tablePath: String,
    options: Map[String, String])
  extends Table with SupportsRead with SupportsWrite {

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

  // SupportsRead implementation
  // NOTE: This should never be called in practice because the analyzer rule replaces
  // HybridDeltaTable with SparkTable (for streaming) or DeltaTableV2 (for batch) before
  // physical planning. However, we must implement it to satisfy the SupportsRead interface
  // requirement for StreamingRelationV2 creation.
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Default to V1 implementation if somehow called
    v1Table.asInstanceOf[SupportsRead].newScanBuilder(options)
  }

  // SupportsWrite implementation
  // NOTE: Like newScanBuilder, this should never be called because the analyzer replaces
  // this table with DeltaTableV2 before physical planning for write operations.
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    v1Table.asInstanceOf[SupportsWrite].newWriteBuilder(info)
  }

  override def name(): String = identifier.toString

  override def schema(): StructType = v1Table.schema()

  override def capabilities(): java.util.Set[TableCapability] = {
    // Add MICRO_BATCH_READ capability to enable StreamingRelationV2 creation
    // This capability is required for Spark to create StreamingRelationV2 instead of
    // StreamingRelation
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
