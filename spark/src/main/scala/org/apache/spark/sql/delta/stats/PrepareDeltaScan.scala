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

package org.apache.spark.sql.delta.stats

import java.util.Objects

import scala.collection.mutable

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, Protocol}
import org.apache.spark.sql.delta.files.{TahoeFileIndexWithSnapshotDescriptor, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.perf.OptimizeMetadataOnlyDeltaQuery
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PROJECT
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

/**
 * Before query planning, we prepare any scans over delta tables by pushing
 * any projections or filters in allowing us to gather more accurate statistics
 * for CBO and metering.
 *
 * Note the following
 * - This rule also ensures that all reads from the same delta log use the same snapshot of log
 *   thus providing snapshot isolation.
 * - If this rule is invoked within an active [[OptimisticTransaction]], then the scans are
 *   generated using the transaction.
 */
trait PrepareDeltaScanBase extends Rule[LogicalPlan]
  with PredicateHelper
  with DeltaLogging
  with OptimizeMetadataOnlyDeltaQuery
  with PreprocessTableWithDVs { self: PrepareDeltaScan =>

  /**
   * Tracks the first-access snapshots of other logs planned by this rule. The snapshots are
   * the keyed by the log's unique id. Note that the lifetime of this rule is a single
   * query, therefore, the map tracks the snapshots only within a query.
   */
  private val scannedSnapshots =
    new java.util.concurrent.ConcurrentHashMap[(String, Path), Snapshot]

  /**
   * Gets the [[DeltaScanGenerator]] for the given log, which will be used to generate
   * [[DeltaScan]]s. Every time this method is called on a log within the lifetime of this
   * rule (i.e., the lifetime of the query for which this rule was instantiated), the returned
   * generator will read a snapshot that is pinned on the first access for that log.
   *
   * Internally, it will use the snapshot of the file index, the snapshot of the active transaction
   * (if any), or the latest snapshot of the given log.
   */
  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator = {
    // The first case means that we've fixed the table snapshot for time travel
    if (index.isTimeTravelQuery) return index.getSnapshot
    val scanGenerator = OptimisticTransaction.getActive()
      .map(_.getDeltaScanGenerator(index))
      .getOrElse {
        // Will be called only when the log is accessed the first time
        scannedSnapshots.computeIfAbsent(index.deltaLog.compositeId, _ => index.getSnapshot)
      }
    import PrepareDeltaScanBase._
    if (onGetDeltaScanGeneratorCallback != null) onGetDeltaScanGeneratorCallback(scanGenerator)
    scanGenerator
  }

  /**
   * Helper method to generate a [[PreparedDeltaFileIndex]]
   */
  protected def getPreparedIndex(
      preparedScan: DeltaScan,
      fileIndex: TahoeLogFileIndex): PreparedDeltaFileIndex = {
    assert(fileIndex.partitionFilters.isEmpty,
      "Partition filters should have been extracted by DeltaAnalysis.")
    PreparedDeltaFileIndex(
      spark,
      fileIndex.deltaLog,
      fileIndex.path,
      preparedScan,
      fileIndex.versionToUse)
  }

  /**
   * Scan files using the given `filters` and return `DeltaScan`.
   *
   * Note: when `limitOpt` is non empty, `filters` must contain only partition filters. Otherwise,
   * it can contain arbitrary filters. See `DeltaTableScan` for more details.
   */
  protected def filesForScan(
      scanGenerator: DeltaScanGenerator,
      limitOpt: Option[Int],
      filters: Seq[Expression],
      delta: LogicalRelation): DeltaScan = {
    withStatusCode("DELTA", "Filtering files for query") {
      if (limitOpt.nonEmpty) {
        // If we trigger limit push down, the filters must be partition filters. Since
        // there are no data filters, we don't need to apply Generated Columns
        // optimization. See `DeltaTableScan` for more details.
        return scanGenerator.filesForScan(limitOpt.get, filters)
      }
      val filtersForScan =
        if (!GeneratedColumn.partitionFilterOptimizationEnabled(spark)) {
          filters
        } else {
          val generatedPartitionFilters = GeneratedColumn.generatePartitionFilters(
            spark, scanGenerator.snapshotToScan, filters, delta)
          filters ++ generatedPartitionFilters
        }
      scanGenerator.filesForScan(filtersForScan)
    }
  }

  /**
   * Prepares delta scans sequentially.
   */
  protected def prepareDeltaScan(plan: LogicalPlan): LogicalPlan = {
    // A map from the canonicalized form of a DeltaTableScan operator to its corresponding delta
    // scan. This map is used to avoid fetching duplicate delta indexes for structurally-equal
    // delta scans.
    val deltaScans = new mutable.HashMap[LogicalPlan, DeltaScan]()

    transformWithSubqueries(plan) {
        case scan @ DeltaTableScan(planWithRemovedProjections, filters, fileIndex,
          limit, delta) =>
          val scanGenerator = getDeltaScanGenerator(fileIndex)
          val preparedScan = deltaScans.getOrElseUpdate(planWithRemovedProjections.canonicalized,
              filesForScan(scanGenerator, limit, filters, delta))
          val preparedIndex = getPreparedIndex(preparedScan, fileIndex)
          optimizeGeneratedColumns(scan, preparedIndex, filters, limit, delta)
      }
  }

  protected def optimizeGeneratedColumns(
      scan: LogicalPlan,
      preparedIndex: PreparedDeltaFileIndex,
      filters: Seq[Expression],
      limit: Option[Int],
      delta: LogicalRelation): LogicalPlan = {
    if (limit.nonEmpty) {
      // If we trigger limit push down, the filters must be partition filters. Since
      // there are no data filters, we don't need to apply Generated Columns
      // optimization. See `DeltaTableScan` for more details.
      return DeltaTableUtils.replaceFileIndex(scan, preparedIndex)
    }
    if (!GeneratedColumn.partitionFilterOptimizationEnabled(spark)) {
      DeltaTableUtils.replaceFileIndex(scan, preparedIndex)
    } else {
      val generatedPartitionFilters =
        GeneratedColumn.generatePartitionFilters(spark, preparedIndex, filters, delta)
      val scanWithFilters =
        if (generatedPartitionFilters.nonEmpty) {
          scan transformUp {
            case delta @ DeltaTable(_: TahoeLogFileIndex) =>
              Filter(generatedPartitionFilters.reduceLeft(And), delta)
          }
        } else {
          scan
        }
      DeltaTableUtils.replaceFileIndex(scanWithFilters, preparedIndex)
    }
  }

  override def apply(_plan: LogicalPlan): LogicalPlan = {
    var plan = _plan

    val shouldPrepareDeltaScan = (
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)
    )
    val updatedPlan = if (shouldPrepareDeltaScan) {
      // Should not be applied to subqueries to avoid duplicate delta jobs.
      val isSubquery = isSubqueryRoot(plan)
      // Should not be applied to DataSourceV2 write plans, because they'll be planned later
      // through a V1 fallback and only that later planning takes place within the transaction.
      val isDataSourceV2 = plan.isInstanceOf[V2WriteCommand]
      if (isSubquery || isDataSourceV2) {
        return plan
      }

      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED)) {
        plan = optimizeQueryWithMetadata(plan)
      }
      prepareDeltaScan(plan)
    } else {
      // If this query is running inside an active transaction and is touching the same table
      // as the transaction, then mark that the entire table as tainted to be safe.
      OptimisticTransaction.getActive.foreach { txn =>
        val logsInPlan = plan.collect { case DeltaTable(fileIndex) => fileIndex.deltaLog }
        if (logsInPlan.exists(_.isSameLogAs(txn.deltaLog))) {
          txn.readWholeTable()
        }
      }

      // Just return the plan if statistics based skipping is off.
      // It will fall back to just partition pruning at planning time.
      plan
    }
    preprocessTablesWithDVs(updatedPlan)
  }

  /**
   * This is an extractor object. See https://docs.scala-lang.org/tour/extractor-objects.html.
   */
  object DeltaTableScan {

    /**
     * The components of DeltaTableScanType are:
     * - the plan with removed projections. We remove projections as a plan differentiator
     * because it does not affect file listing results.
     * - filter expressions collected by `PhysicalOperation`
     * - the `TahoeLogFileIndex` of the matched DeltaTable`
     * - integer value of limit expression, if any
     * - matched `DeltaTable`
     */
    private type DeltaTableScanType =
      (LogicalPlan, Seq[Expression], TahoeLogFileIndex, Option[Int], LogicalRelation)

    /**
     * This is an extractor method (basically, the opposite of a constructor) which takes in an
     * object `plan` and tries to give back the arguments as a [[DeltaTableScanType]].
     */
    def unapply(plan: LogicalPlan): Option[DeltaTableScanType] = {
      val limitPushdownEnabled = spark.conf.get(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED)

      // Remove projections as a plan differentiator because it does not affect file listing
      // results. Plans with the same filters but different projections therefore will not have
      // duplicate delta indexes.
      def canonicalizePlanForDeltaFileListing(plan: LogicalPlan): LogicalPlan = {
        val planWithRemovedProjections = plan.transformWithPruning(_.containsPattern(PROJECT)) {
          case p: Project if p.projectList.forall(_.isInstanceOf[AttributeReference]) => p.child
        }
        planWithRemovedProjections
      }

      plan match {
        case LocalLimit(IntegerLiteral(limit),
          PhysicalOperation(_, filters, delta @ DeltaTable(fileIndex: TahoeLogFileIndex)))
            if limitPushdownEnabled && containsPartitionFiltersOnly(filters, fileIndex) =>
          Some((canonicalizePlanForDeltaFileListing(plan), filters, fileIndex, Some(limit), delta))
        case PhysicalOperation(
            _,
            filters,
            delta @ DeltaTable(fileIndex: TahoeLogFileIndex)) =>
          val allFilters = fileIndex.partitionFilters ++ filters
          Some((canonicalizePlanForDeltaFileListing(plan), allFilters, fileIndex, None, delta))

        case _ => None
      }
    }

    private def containsPartitionFiltersOnly(
        filters: Seq[Expression],
        fileIndex: TahoeLogFileIndex): Boolean = {
      val partitionColumns = fileIndex.snapshotAtAnalysis.metadata.partitionColumns
      import DeltaTableUtils._
      filters.forall(expr => !containsSubquery(expr) &&
        isPredicatePartitionColumnsOnly(expr, partitionColumns, spark))
    }
  }
}

class PrepareDeltaScan(protected val spark: SparkSession)
  extends PrepareDeltaScanBase

object PrepareDeltaScanBase {

  /**
   * Optional callback function that is called after `getDeltaScanGenerator` is called
   * by the PrepareDeltaScan rule. This is primarily used for testing purposes.
   */
  @volatile private var onGetDeltaScanGeneratorCallback: DeltaScanGenerator => Unit = _

  /**
   * Run a thunk of code with the given callback function injected into the PrepareDeltaScan rule.
   * The callback function is called after `getDeltaScanGenerator` is called
   * by the PrepareDeltaScan rule. This is primarily used for testing purposes.
   */
  private[delta] def withCallbackOnGetDeltaScanGenerator[T](
      callback: DeltaScanGenerator => Unit)(thunk: => T): T = {
    try {
      onGetDeltaScanGeneratorCallback = callback
      thunk
    } finally {
      onGetDeltaScanGeneratorCallback = null
    }
  }
}

/**
 * A [[TahoeFileIndex]] that uses a prepared scan to return the list of relevant files.
 * This is injected into a query right before query planning by [[PrepareDeltaScan]] so that
 * CBO and metering can accurately understand how much data will be read.
 *
 * @param versionScanned The version of the table that is being scanned, if a specific version
 *                       has specifically been requested, e.g. by time travel.
 */
case class PreparedDeltaFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    preparedScan: DeltaScan,
    versionScanned: Option[Long])
  extends TahoeFileIndexWithSnapshotDescriptor(spark, deltaLog, path, preparedScan.scannedSnapshot)
  with DeltaLogging {

  /**
   * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`
   */
  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    val currentFilters = ExpressionSet(partitionFilters ++ dataFilters)
    val (addFiles, eventData) = if (currentFilters == preparedScan.allFilters ||
        currentFilters == preparedScan.filtersUsedForSkipping) {
      // [[DeltaScan]] was created using `allFilters` out of which only `filtersUsedForSkipping`
      // filters were used for skipping while creating the DeltaScan.
      // If currentFilters is same as allFilters, then no need to recalculate files and we can use
      // previous results.
      // If currentFilters is same as filtersUsedForSkipping, then also we don't need to recalculate
      // files as [[DeltaScan.files]] were calculates using filtersUsedForSkipping only. So if we
      // recalculate, we will get same result. So we should use previous result in this case also.
      val eventData = Map(
        "reused" -> true,
        "currentFiltersSameAsPreparedAllFilters" -> (currentFilters == preparedScan.allFilters),
        "currentFiltersSameAsPreparedFiltersUsedForSkipping" ->
          (currentFilters == preparedScan.filtersUsedForSkipping)
      )
      (preparedScan.files.distinct, eventData)
    } else {
      logInfo(
        s"""
           |Prepared scan does not match actual filters. Reselecting files to query.
           |Prepared: ${preparedScan.allFilters}
           |Actual: ${currentFilters}
         """.stripMargin)
      val eventData = Map(
        "reused" -> false,
        "preparedAllFilters" -> preparedScan.allFilters.mkString(","),
        "preparedFiltersUsedForSkipping" -> preparedScan.filtersUsedForSkipping.mkString(","),
        "currentFilters" -> currentFilters.mkString(",")
      )
      val files = preparedScan.scannedSnapshot.filesForScan(partitionFilters ++ dataFilters).files
      (files, eventData)
    }
    recordDeltaEvent(deltaLog,
      opType = "delta.preparedDeltaFileIndex.reuseSkippingResult",
      data = eventData)
    addFiles
  }

  /**
   * Returns the list of files that will be read when scanning this relation. This call may be
   * very expensive for large tables.
   */
  override def inputFiles: Array[String] =
    preparedScan.files.map(f => absolutePath(f.path).toString).toArray

  /** Refresh any cached file listings */
  override def refresh(): Unit = { }

  /** Sum of table file sizes, in bytes */
  override def sizeInBytes: Long =
    preparedScan.scanned.bytesCompressed
      .getOrElse(spark.sessionState.conf.defaultSizeInBytes)

  override def equals(other: Any): Boolean = other match {
    case p: PreparedDeltaFileIndex =>
      p.deltaLog == deltaLog && p.path == path && p.preparedScan == preparedScan &&
        p.partitionSchema == partitionSchema && p.versionScanned == versionScanned
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(deltaLog, path, preparedScan, partitionSchema, versionScanned)
  }

}
