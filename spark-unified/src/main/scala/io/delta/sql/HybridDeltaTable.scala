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

import java.util.{Map => JMap}
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
 * A hybrid Delta table implementation that can delegate to either V1 (DeltaLog-based)
 * or V2 (Kernel-based) implementations based on the query context.
 *
 * This table is returned by the catalog for all operations. An analyzer rule then
 * wraps it with HybridDeltaTableWithContext to indicate whether V2 should be used.
 *
 * By default (without wrapper), this table uses V1 for all operations.
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

  /**
   * Creates a scan builder, delegating to V1 or V2 based on the useV2 flag.
   * This method is called by HybridDeltaTableWithContext wrapper.
   */
  def createScanBuilder(useV2: Boolean, caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    if (useV2) {
      v2Table.newScanBuilder(caseInsensitiveStringMap)
    } else {
      v1Table.asInstanceOf[SupportsRead].newScanBuilder(caseInsensitiveStringMap)
    }
  }

  // Default implementation uses V1
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    createScanBuilder(useV2 = false, options)
  }

  // Write operations always use V1 (V2 doesn't support writes yet)
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    v1Table.asInstanceOf[SupportsWrite].newWriteBuilder(info)
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
   * Returns the underlying DeltaTableV2 for cases where we need to unwrap
   * the hybrid table back to pure V1 (e.g., for batch operations before DeltaAnalysis).
   */
  def getUnderlyingDeltaTableV2(): DeltaTableV2 = v1Table
}

/**
 * Immutable wrapper around HybridDeltaTable that carries the context hint
 * indicating whether to use V2 or V1.
 *
 * This wrapper is created by the analyzer rule for streaming read sources
 * to indicate that V2 (Kernel) should be used.
 */
class HybridDeltaTableWithContext(
    underlying: HybridDeltaTable,
    useV2: Boolean)
  extends Table with SupportsRead with SupportsWrite {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    underlying.createScanBuilder(useV2, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    // Writes always use V1
    underlying.newWriteBuilder(info)
  }

  override def name(): String = underlying.name()

  override def schema(): StructType = underlying.schema()

  override def capabilities(): java.util.Set[TableCapability] = underlying.capabilities()
}

