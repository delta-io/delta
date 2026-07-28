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

import scala.jdk.CollectionConverters._

import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager
import io.delta.spark.internal.v2.utils.ExpressionUtils
import io.delta.kernel.Snapshot
import io.delta.kernel.expressions.{And, Predicate}

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, Statistics, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Package-private scan builder implementation for Delta's Spark DataSource V2 read path.
 *
 * This class must remain package-private so callers outside `v2.read` depend only on Spark's
 * public connector interfaces instead of coupling to Delta's internal V2 implementation.
 *
 * @param tableName the name of the table
 * @param initialSnapshot Snapshot created during connector setup
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
    snapshotManager: DeltaSnapshotManager,
    dataSchema: StructType,
    partitionSchema: StructType,
    tableSchema: StructType,
    catalogStats: Optional[Statistics],
    options: CaseInsensitiveStringMap)
  extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters
  with SupportsPushDownLimit {

  // Use Objects.requireNonNull (throws NullPointerException) rather than Scala's require (throws
  // IllegalArgumentException) to preserve the exact null-check behavior of the original Java class.
  Objects.requireNonNull(initialSnapshot, "initialSnapshot is null")
  Objects.requireNonNull(snapshotManager, "snapshotManager is null")
  Objects.requireNonNull(dataSchema, "dataSchema is null")
  Objects.requireNonNull(partitionSchema, "partitionSchema is null")
  Objects.requireNonNull(tableSchema, "tableSchema is null")
  Objects.requireNonNull(catalogStats, "catalogStats is null")
  Objects.requireNonNull(options, "options is null")

  private var kernelScanBuilder: io.delta.kernel.ScanBuilder =
    initialSnapshot.getScanBuilder

  private val partitionColumnSet: Set[String] =
    partitionSchema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet

  private var requiredDataSchema: StructType = dataSchema

  // pushedKernelPredicates: Predicates that have been pushed down to the Delta Kernel for
  // evaluation.
  // pushedSparkFilters: The same pushed predicates, but represented using Spark's Filter API
  // (needed because Spark operates on Filter objects while the Kernel uses Predicate).
  private var pushedKernelPredicates: Array[Predicate] = Array.empty
  private var pushedSparkFilters: Array[Filter] = Array.empty
  private var dataFilters: Array[Filter] = Array.empty
  // Tracks whether any filter still needs to be applied after the scan.
  private var hasPostScanResidualFilters: Boolean = false
  private var pushedLimit: OptionalInt = OptionalInt.empty()

  override def pruneColumns(requiredSchema: StructType): Unit = {
    Objects.requireNonNull(requiredSchema, "requiredSchema is null")
    // CDC columns are injected later by CDCReadFunction, so strip them here.
    requiredDataSchema = new StructType(
      requiredSchema.fields.filter { f =>
        val name = f.name.toLowerCase(Locale.ROOT)
        !partitionColumnSet.contains(name) && !CDCSchemaContext.isCDCColumn(name)
      })
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val kernelSupportedFilters = new java.util.ArrayList[Filter]()
    val convertedKernelPredicates = new java.util.ArrayList[Predicate]()
    val dataFilterList = new java.util.ArrayList[Filter]()
    val postScanFilters = new java.util.ArrayList[Filter]()

    filters.foreach { filter =>
      val classification =
        ExpressionUtils.classifyFilter(filter, partitionColumnSet.asJava, tableSchema)
      // Collect kernel predicates if supported.
      if (classification.isKernelSupported) {
        convertedKernelPredicates.add(classification.kernelPredicate.get())
        if (!classification.isPartialConversion) {
          // Add filter to kernelSupportedFilters if it is fully converted.
          // TODO: add partially converted Spark filter as well; right now we only have the
          // partially converted kernel predicate.
          kernelSupportedFilters.add(filter)
        }
      }

      // Collect data filters.
      if (classification.isDataFilter) {
        dataFilterList.add(filter)
      }

      // Collect post-scan filters. Filters with the following characteristics need to be evaluated
      // after the delta kernel scan:
      // 1. filters that are not supported by delta kernel, thus kernel cannot apply them during
      //    scan;
      // 2. filters that are not fully converted to kernel predicates, thus the unconverted part
      //    needs to be evaluated after scan;
      // 3. filters that are data filters, as kernel only evaluates data filters based on min/max
      //    stats, thus need to be evaluated with actual data after scan.
      //
      // Fully converted partition filters are used to prune partitions during scan. Only the
      // partitions that satisfy the filters will be scanned, so no need for post-scan evaluation.
      if (!classification.isKernelSupported
          || classification.isPartialConversion
          || classification.isDataFilter) {
        postScanFilters.add(filter)
      }
    }

    pushedSparkFilters = kernelSupportedFilters.toArray(new Array[Filter](0))
    pushedKernelPredicates = convertedKernelPredicates.toArray(new Array[Predicate](0))
    if (pushedKernelPredicates.nonEmpty) {
      val kernelAnd = pushedKernelPredicates.reduce((left, right) => new And(left, right))
      kernelScanBuilder = kernelScanBuilder.withFilter(kernelAnd)
    }
    dataFilters = dataFilterList.toArray(new Array[Filter](0))
    val postScan = postScanFilters.toArray(new Array[Filter](0))
    // ScanBuilder mutations can be cumulative, so a later pushFilters call should not make an
    // earlier residual safe to ignore.
    hasPostScanResidualFilters |= postScan.length > 0
    postScan
  }

  override def pushedFilters(): Array[Filter] = pushedSparkFilters

  /**
   * Accepts a LIMIT hint from Spark's optimizer.
   *
   * Always returns `true`: the connector treats the limit as a best-effort hint, using per-file
   * `numRecords` statistics to stop adding files to the scan plan once enough rows have been
   * accumulated (see [[DeltaV2Scan]]). Because pruning happens at file granularity, the planned
   * scan may still return more rows than requested (for example, a single file with 1,000 rows for
   * LIMIT 5), so `isPartiallyPushed()` is left at its default of `true` and Spark keeps its limit
   * operators as a backstop.
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

  override def build(): Scan = {
    // Spark's V2ScanRelationPushDown only pushes a limit when no post-scan residual remains. It
    // matches PhysicalOperation(_, Nil, sHolder). Retain this defensive check for direct callers
    // that may invoke the ScanBuilder methods in a different order.
    val effectiveLimit = if (hasPostScanResidualFilters) OptionalInt.empty() else pushedLimit

    new DeltaV2Scan(
      snapshotManager,
      initialSnapshot,
      tableSchema,
      dataSchema,
      partitionSchema,
      requiredDataSchema,
      pushedKernelPredicates,
      dataFilters,
      kernelScanBuilder.build(),
      catalogStats,
      options,
      effectiveLimit)
  }

  private[read] def getOptions: CaseInsensitiveStringMap = options

  private[read] def getDataSchema: StructType = dataSchema

  private[read] def getPartitionSchema: StructType = partitionSchema

  private[read] def getPushedLimit: OptionalInt = pushedLimit
}

private[read] object DeltaV2ScanBuilder {

  /**
   * Factory used by the Java `DeltaV2ScanUtils`. A companion-object static forwarder (rather than a
   * direct `new` from Java) so the OSS sbt/zinc mixed Java+Scala compilation in the spark/v2 module
   * can resolve the reference: Java code calling a Scala `object` method compiles cleanly, whereas
   * Java `new`-ing this Scala class -- which itself references same-module Java -- does not stage.
   */
  def create(
      tableName: String,
      initialSnapshot: Snapshot,
      snapshotManager: DeltaSnapshotManager,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableSchema: StructType,
      catalogStats: Optional[Statistics],
      options: CaseInsensitiveStringMap): DeltaV2ScanBuilder =
    new DeltaV2ScanBuilder(
      tableName,
      initialSnapshot,
      snapshotManager,
      dataSchema,
      partitionSchema,
      tableSchema,
      catalogStats,
      options)
}
