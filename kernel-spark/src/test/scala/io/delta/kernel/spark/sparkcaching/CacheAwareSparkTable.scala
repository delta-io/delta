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

package io.delta.kernel.spark.sparkcaching

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.delta.kernel.spark.table.SparkTable

/**
 * A wrapper around Delta Kernel's SparkTable that implements proper semantic equality
 * for cache invalidation.
 *
 * This is a TEST-ONLY implementation demonstrating how SparkTable should handle equality
 * to support proper cache invalidation in Spark.
 *
 * Key Features:
 * 1. Semantic equality based on table path and version (for time travel)
 * 2. Delegates all Table operations to the underlying SparkTable
 * 3. Extracts and stores the Delta version for equality comparison
 *
 * Without proper equality, Spark's CacheManager cannot match plans after DML operations,
 * leading to stale cache bugs.
 */
class CacheAwareSparkTable(
    private val underlying: SparkTable,
    private val tablePath: String,
    private val resolvedVersion: Option[Long])
  extends Table with SupportsRead {

  // Delegate all Table methods to underlying SparkTable
  override def name(): String = underlying.name()

  override def schema(): StructType = underlying.schema()

  override def columns(): Array[org.apache.spark.sql.connector.catalog.Column] = 
    underlying.columns()

  override def partitioning(): Array[Transform] = underlying.partitioning()

  override def properties(): java.util.Map[String, String] = underlying.properties()

  override def capabilities(): java.util.Set[TableCapability] = underlying.capabilities()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = 
    underlying.newScanBuilder(options)

  /**
   * Semantic equality for cache invalidation.
   *
   * Two CacheAwareSparkTable instances are equal if:
   * 1. They point to the same table path (normalized)
   * 2. They have the same resolved version (for time travel protection)
   *
   * This is critical for Spark's cache invalidation:
   * - When DML updates table at version N, it calls refreshTable()
   * - refreshTable() creates a NEW CacheAwareSparkTable at version N+1 (no time travel)
   * - Cached entry for version N (no time travel) should match
   * - Cached entry for version K (with time travel) should NOT match (protected)
   */
  override def equals(other: Any): Boolean = other match {
    case that: CacheAwareSparkTable =>
      // Normalize paths for comparison (handle trailing slashes, etc.)
      val thisPathNormalized = normalizePath(this.tablePath)
      val thatPathNormalized = normalizePath(that.tablePath)

      // Compare paths and versions
      thisPathNormalized == thatPathNormalized &&
        this.resolvedVersion == that.resolvedVersion

    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(normalizePath(tablePath), resolvedVersion)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    val versionStr = resolvedVersion.map(v => s"@$v").getOrElse("@latest")
    s"CacheAwareSparkTable($tablePath$versionStr)"
  }

  /**
   * Normalizes a table path for comparison.
   * Removes trailing slashes, resolves relative paths, etc.
   */
  private def normalizePath(path: String): String = {
    // Simple normalization: remove trailing slash and convert to lowercase for comparison
    val normalized = path.stripSuffix("/")
    // In a real implementation, we'd use Hadoop Path to fully normalize
    normalized
  }
}

/**
 * Companion object for CacheAwareSparkTable.
 */
object CacheAwareSparkTable {
  /**
   * Creates a CacheAwareSparkTable by extracting version information from options.
   *
   * @param underlying The underlying SparkTable to wrap
   * @param tablePath The full path to the Delta table
   * @param options The options map, which may contain versionAsOf or timestampAsOf
   * @return A CacheAwareSparkTable instance
   */
  def apply(
      underlying: SparkTable,
      tablePath: String,
      options: CaseInsensitiveStringMap): CacheAwareSparkTable = {
    
    // Extract version if present (for time travel queries)
    val resolvedVersion: Option[Long] = Option(options.get("versionAsOf"))
      .map(_.toLong)
      .orElse(Option(options.get("timestampAsOf")).map { _ =>
        // In a real implementation, we'd resolve the timestamp to a version
        // For this test, we'll mark it as Some(-1) to indicate time travel is present
        -1L
      })

    new CacheAwareSparkTable(underlying, tablePath, resolvedVersion)
  }
}

/**
 * Test-specific catalog that creates CacheAwareSparkTable instances instead of SparkTable.
 *
 * This demonstrates how a catalog should create tables with proper semantic equality.
 */
class CacheAwareTestCatalog extends io.delta.kernel.spark.catalog.TestCatalog {
  
  override def loadTable(ident: Identifier): Table = {
    // Get the underlying SparkTable from parent
    val sparkTable = super.loadTable(ident).asInstanceOf[SparkTable]
    
    // Extract table path using reflection (since it's private in SparkTable)
    val tablePath = try {
      val field = sparkTable.getClass.getDeclaredField("tablePath")
      field.setAccessible(true)
      field.get(sparkTable).asInstanceOf[String]
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Could not extract table path from SparkTable: ${e.getMessage}", e)
    }
    
    // Wrap in CacheAwareSparkTable with empty options (no time travel)
    CacheAwareSparkTable(
      sparkTable,
      tablePath,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
  }
}

