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

package io.delta.sharing.spark

import org.apache.spark.sql.delta.{DeltaTableUtils => SqlDeltaTableUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{PreparedDeltaFileIndex, PrepareDeltaScan}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.DeltaSharingFileIndex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Before query planning, we prepare any scans over delta sharing tables by pushing
 * any filters or limits to delta sharing server through RPC, allowing us to return only needed
 * files and gather more accurate statistics for CBO and metering.
 */
class PrepareDeltaSharingScan(override val spark: SparkSession) extends PrepareDeltaScan(spark) {

  /**
   * Only skip DataSourceV2 writes that target a Delta table.
   *
   * [[PrepareDeltaScan]] skips every V2 write because a Delta target is re-planned through a V1
   * fallback, which runs this rule again inside the transaction. That reasoning is about the
   * *sink*, not the source: when a Delta Sharing scan feeds a non-Delta V2 sink (e.g. Iceberg)
   * nothing ever re-plans the query, so skipping here means the scan is never prepared at all.
   * The [[DeltaSharingFileIndex]] then survives into planning instead of being replaced by a
   * [[PreparedDeltaFileIndex]], and because the rule that injects the deletion vector filter only
   * matches a `TahoeFileIndex`, rows marked deleted are silently returned. See delta-io/delta#6719.
   *
   * Delta targets are still skipped so we do not issue a second, redundant RPC to the sharing
   * server for a scan the V1 fallback is about to prepare again.
   */
  override protected def shouldSkipV2WritePlan(plan: LogicalPlan): Boolean = plan match {
    case w: V2WriteCommand =>
      w.table match {
        case r: DataSourceV2Relation => r.table.isInstanceOf[DeltaTableV2]
        case _ => false
      }
    case _ => false
  }

  /**
   * Prepares delta sharing scans sequentially.
   */
  override protected def prepareDeltaScan(plan: LogicalPlan): LogicalPlan = {
    transformWithSubqueries(plan) {
      case scan @ DeltaSharingTableScan(_, filters, dsFileIndex, limit, _) =>
        val partitionCols = dsFileIndex.partitionColumns
        val (partitionFilters, dataFilters) = filters.partition { e =>
          SqlDeltaTableUtils.isPredicatePartitionColumnsOnly(e, partitionCols, spark)
        }
        logInfo(s"Classified filters: partition: $partitionFilters, data: $dataFilters, " +
          s"limit: $limit.")
        val deltaLog = dsFileIndex.fetchFilesAndConstructDeltaLog(
          partitionFilters,
          dataFilters,
          limit.map(_.toLong)
        )
        val snapshot = deltaLog.snapshot
        val deltaScan = limit match {
          case Some(limit) => snapshot.filesForScan(limit, filters)
          case _ => snapshot.filesForScan(filters)
        }
        val preparedIndex = PreparedDeltaFileIndex(
          spark,
          deltaLog,
          deltaLog.dataPath,
          catalogTableOpt = None,
          preparedScan = deltaScan,
          versionScanned = Some(snapshot.version)
        )
        SqlDeltaTableUtils.replaceFileIndex(scan, preparedIndex)
    }
  }

  // Just return the plan if statistics based skipping is off.
  // It will fall back to just partition pruning at planning time.
  // When data skipping is disabled, just convert Delta sharing scans to normal tahoe scans.
  // NOTE: File skipping is only disabled on the client, so we still pass filters to the server.
  override protected def prepareDeltaScanWithoutFileSkipping(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case scan@DeltaSharingTableScan(_, filters, sharingIndex, _, _) =>
        val partitionCols = sharingIndex.partitionColumns
        val (partitionFilters, dataFilters) = filters.partition { e =>
          SqlDeltaTableUtils.isPredicatePartitionColumnsOnly(e, partitionCols, spark)
        }
        logInfo(s"Classified filters: partition: $partitionFilters, data: $dataFilters")
        val fileIndex = sharingIndex.asTahoeFileIndex(partitionFilters, dataFilters)
        SqlDeltaTableUtils.replaceFileIndex(scan, fileIndex)
    }
  }

  // TODO: Support metadata-only query optimization!
  override def optimizeQueryWithMetadata(plan: LogicalPlan): LogicalPlan = plan

  /**
   * This is an extractor object. See https://docs.scala-lang.org/tour/extractor-objects.html.
   */
  object DeltaSharingTableScan extends DeltaTableScan[DeltaSharingFileIndex] {
    // Since delta library is used to read the data on constructed delta log, this should also
    // consider the spark config for delta limit pushdown.
    override def limitPushdownEnabled(plan: LogicalPlan): Boolean =
      ConfUtils.limitPushdownEnabled(plan.conf) &&
        (spark.conf.get(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key) == "true")

    override def getPartitionColumns(fileIndex: DeltaSharingFileIndex): Seq[String] =
      fileIndex.partitionColumns

    override def getPartitionFilters(fileIndex: DeltaSharingFileIndex): Seq[Expression] =
      Seq.empty[Expression]

  }
}
