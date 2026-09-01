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

package io.delta.spark.internal.v2.read

import java.util.{Locale, Objects, Optional, OptionalInt}
import java.util.function.Supplier

import io.delta.kernel.engine.Engine
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.stats.DeltaScan
import io.delta.spark.internal.v2.DeltaV2Logging
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExprId}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, Statistics, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A Spark ScanBuilder for the Delta v2 connector. In [[build]] it captures the inputs for a
 * Kernel-backed [[DeltaV2Snapshot]] and hands `DeltaV2Scan` a lazy [[DeltaScan]] supplier. Batch
 * planning invokes the supplier to run V1 data skipping; streaming never needs that batch work.
 *
 * Filter pushdown implements [[SupportsPushDownCatalystFilters]]: the trait's `Seq[Expression]`
 * signature erases to a version-specific `Seq` type across Scala 2.12 / 2.13, which a single Java
 * override cannot satisfy, so this builder is Scala. Pushed filters are split into partition and
 * data filters and kept as Catalyst expressions (fed to `DeltaV2Snapshot.filesForScan`); they are
 * not translated to data source predicates, so `pushedFilters` reports none.
 *
 * @param tableName the name of the table (used only for identification)
 * @param initialSnapshot Kernel snapshot created during connector setup
 * @param kernelEngine the Kernel engine used to read scan files
 * @param catalogTable the catalog table this scan resolved from, if any
 * @param snapshotManager the snapshot manager for this table
 * @param dataSchema the data schema (non-partition columns)
 * @param partitionSchema the partition schema
 * @param tableSchema the full table schema (all columns) for filter type alignment
 * @param catalogStats optional V2 Statistics converted from catalog stats
 * @param options scan options
 */
private[read] class DeltaV2ScanBuilder(
    tableName: String,
    initialSnapshot: Snapshot,
    kernelEngine: Engine,
    catalogTable: Optional[CatalogTable],
    snapshotManager: DeltaV2SnapshotManager,
    dataSchema: StructType,
    partitionSchema: StructType,
    tableSchema: StructType,
    catalogStats: Optional[Statistics],
    options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownCatalystFilters
    with SupportsPushDownLimit
    with DeltaV2Logging {

  // Use Objects.requireNonNull (throws NullPointerException) rather than Scala's require (throws
  // IllegalArgumentException) to preserve the exact null-check behavior of the original Java class.
  Objects.requireNonNull(initialSnapshot, "initialSnapshot is null")
  Objects.requireNonNull(kernelEngine, "kernelEngine is null")
  Objects.requireNonNull(catalogTable, "catalogTable is null")
  Objects.requireNonNull(snapshotManager, "snapshotManager is null")
  Objects.requireNonNull(dataSchema, "dataSchema is null")
  Objects.requireNonNull(partitionSchema, "partitionSchema is null")
  Objects.requireNonNull(tableSchema, "tableSchema is null")
  Objects.requireNonNull(catalogStats, "catalogStats is null")
  Objects.requireNonNull(options, "options is null")

  private val partitionColumnSet: Set[String] =
    partitionSchema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet

  private var requiredDataSchema: StructType = dataSchema

  // Pushed partition + data catalyst filters (their union drives V1 filesForScan skipping).
  private var partitionCatalystFilters: Array[Expression] = Array.empty
  private var dataCatalystFilters: Array[Expression] = Array.empty
  // Tracks whether any filter still needs to be applied after the scan. Data filters are not
  // row-exact (min/max skipping only), so they leave a post-scan residual; partition filters are
  // exact and leave none. Used to decide when a pushed LIMIT is safe (see build()).
  private var hasPostScanResidualFilters: Boolean = false
  private var pushedLimit: OptionalInt = OptionalInt.empty()

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] =
    recordFrameProfile("scanBuilder.pushFilters") {
      val (partitionFilters, dataFilters) =
        DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, filters)
      partitionCatalystFilters = partitionFilters.toArray
      dataCatalystFilters = dataFilters.toArray
      // Data filters need post-scan evaluation (min/max skipping is not row-exact); partition
      // filters are exact and need no re-evaluation. ScanBuilder mutations can be cumulative, so a
      // later pushFilters call must not make an earlier residual safe to ignore.
      hasPostScanResidualFilters |= dataFilters.nonEmpty
      dataFilters
    }

  // Filters are kept as Catalyst expressions and are not translated to data source predicates.
  override def pushedFilters: Array[Predicate] = Array.empty

  override def pruneColumns(requiredSchema: StructType): Unit =
    recordFrameProfile("scanBuilder.pruneColumns") {
      Objects.requireNonNull(requiredSchema, "requiredSchema is null")
      // CDC columns are injected later by CDCReadFunction, so strip them here.
      requiredDataSchema = new StructType(
        requiredSchema.fields.filter { f =>
          val name = f.name.toLowerCase(Locale.ROOT)
          !partitionColumnSet.contains(name) && !CDCSchemaContext.isCDCColumn(name)
        })
    }

  /**
   * Accepts a LIMIT hint from Spark's optimizer.
   *
   * Always returns `true`: the connector treats the limit as a best-effort hint. When the limit is
   * kept (see [[build]]), file selection routes through V1's limit-aware `filesForScan`, which
   * stops adding files once enough logical rows have accumulated. Because pruning happens at file
   * granularity, the planned scan may still return more rows than requested (for example, a single
   * file with 1,000 rows for LIMIT 5), so `isPartiallyPushed()` is left at its default of `true`
   * and Spark keeps its limit operators as a backstop.
   *
   * @param limit the row limit requested by Spark which must be non-negative.
   */
  override def pushLimit(limit: Int): Boolean = {
    if (limit < 0) {
      throw new IllegalArgumentException("Pushed limit must be non-negative, but got: " + limit)
    }
    pushedLimit = OptionalInt.of(limit)
    true
  }

  // isPartiallyPushed() intentionally uses the interface default (true). Because pruning happens at
  // file granularity, the scan may produce more rows than requested, so Spark must reapply LIMIT.

  override def build(): Scan =
    recordFrameProfile("scanBuilder.build") {
      // Capture the planning inputs here, but defer constructing the Kernel-backed V1 snapshot and
      // running filesForScan until DeltaV2Scan actually plans a batch. A MicroBatchStream performs
      // its own snapshot and commit-range reads and never consumes batch-selected files.
      //
      // Ask for per-file record counts exactly when DeltaV2Scan will consume them for scan
      // metadata.
      // This must stay in sync with DeltaV2Scan.arePlanStatsEnabled, which gates the reading side.
      // Note this only affects the no-limit branch below -- V1's limit-aware filesForScan takes no
      // keepNumRecords and always drops per-file stats, so DeltaV2Scan falls back to
      // DeltaScan.scanned.rows there.
      val sqlConf = SQLConf.get
      val keepNumRecords = sqlConf.cboEnabled || sqlConf.planStatsEnabled
      val partitionFiltersForScan = partitionCatalystFilters.toIndexedSeq
      val catalystFilters = partitionFiltersForScan ++ dataCatalystFilters

      // Spark's V2ScanRelationPushDown only pushes a limit when no post-scan residual remains (it
      // matches PhysicalOperation(_, Nil, sHolder)), so an effective limit implies only exact
      // partition filters are present. Retain this residual check for direct callers that may
      // invoke the ScanBuilder methods in a different order.
      val effectiveLimit = if (hasPostScanResidualFilters) OptionalInt.empty() else pushedLimit

      val deltaScanSupplier = new Supplier[DeltaScan] {
        override def get(): DeltaScan = {
          val snapshot = initialSnapshot.asInstanceOf[DeltaV2Snapshot]

          // When a limit is pushed, route selection through V1's limit-aware filesForScan. It
          // requires partition-only filters (guaranteed above) and prunes files by accumulating
          // record counts until the limit is satisfied.
          def selectFiles(): DeltaScan =
            if (effectiveLimit.isPresent) {
              recordFrameProfile("filesForScan.limitAndFilters") {
                snapshot.filesForScan(
                  effectiveLimit.getAsInt.toLong,
                  partitionFiltersForScan)
              }
            } else {
              recordFrameProfile("filesForScan.filters") {
                snapshot.filesForScan(catalystFilters, keepNumRecords)
              }
            }

          // Select files inline on this path.
          selectFiles()
        }
      }

      val kernelSnapshot =
        DeltaV2Snapshot.getKernelSnapshot(initialSnapshot)
      val scan = new DeltaV2Scan(
        snapshotManager,
        kernelSnapshot,
        tableSchema,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        deltaScanSupplier,
        dataCatalystFilters,
        partitionCatalystFilters,
        catalogStats,
        options,
        effectiveLimit)
      scan
    }

  private[read] def getOptions: CaseInsensitiveStringMap = options

  private[read] def getDataSchema: StructType = dataSchema

  private[read] def getPartitionSchema: StructType = partitionSchema

  private[read] def getPushedLimit: OptionalInt = pushedLimit
}

private[read] object DeltaV2ScanBuilder {

  /**
   * Normalizes attribute references so scan equality compares logical filters across analyses.
   * Resets both the ExprId and the qualifier: the same table column can surface with different
   * qualifiers (e.g. a CTE alias vs the fully-qualified table name) yet be logically identical,
   * and leaving the qualifier in place would make two otherwise-equal scans compare unequal and
   * defeat exchange/subquery reuse.
   */
  def normalizeForEquality(expr: Expression): Expression = expr.transformUp {
    case a: AttributeReference => a.withExprId(ExprId(0L)).withQualifier(Nil)
  }

  /** Factory used by the Java DeltaV2ScanUtils. */
  def create(
      tableName: String,
      initialSnapshot: Snapshot,
      kernelEngine: Engine,
      catalogTable: Optional[CatalogTable],
      snapshotManager: DeltaV2SnapshotManager,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableSchema: StructType,
      catalogStats: Optional[Statistics],
      options: CaseInsensitiveStringMap): DeltaV2ScanBuilder =
    new DeltaV2ScanBuilder(
      tableName,
      initialSnapshot,
      kernelEngine,
      catalogTable,
      snapshotManager,
      dataSchema,
      partitionSchema,
      tableSchema,
      catalogStats,
      options)

  /**
   * Adapter for callers that hold a Kernel snapshot but push no filters -- currently Delta Sharing
   * DSv2, whose batch scan builds a full-table read (filter pushdown is a follow-up milestone).
   *
   * Rather than duplicate the DeltaScan-production logic (which must construct the `private[v2]`
   * [[DeltaV2Snapshot]] and run the V1 data-skipping path), this reuses the normal build path with
   * no pushed filters: an empty filter set makes `filesForScan` take its no-filter fast path and
   * return a full-table [[DeltaScan]]. Column pruning is applied so the returned scan reads only
   * `requiredSchema`.
   *
   * @param tableName the table name (used only for identification)
   * @param snapshot the Kernel snapshot to read
   * @param kernelEngine the Kernel engine
   * @param snapshotManager the snapshot manager for this table
   * @param dataSchema the data schema (non-partition columns)
   * @param partitionSchema the partition schema
   * @param tableSchema the full table schema
   * @param requiredSchema the columns actually needed (partition/CDC columns are stripped)
   * @param catalogStats optional V2 Statistics converted from catalog stats
   * @return a `DeltaV2Scan` over the full table (no data skipping)
   */
  def forUnfilteredScan(
      tableName: String,
      snapshot: Snapshot,
      kernelEngine: Engine,
      snapshotManager: DeltaV2SnapshotManager,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableSchema: StructType,
      requiredSchema: StructType,
      catalogStats: Optional[Statistics]): Scan = {
    // No CatalogTable: a full-table, no-filter scan does not use catalogTable (it only feeds the
    val builder = new DeltaV2ScanBuilder(
      tableName,
      snapshot,
      kernelEngine,
      Optional.empty[CatalogTable](),
      snapshotManager,
      dataSchema,
      partitionSchema,
      tableSchema,
      catalogStats,
      CaseInsensitiveStringMap.empty())
    builder.pruneColumns(requiredSchema)
    builder.build()
  }
}
