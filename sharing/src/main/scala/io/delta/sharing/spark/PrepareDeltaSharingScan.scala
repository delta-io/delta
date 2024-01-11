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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{PreparedDeltaFileIndex, PrepareDeltaScan}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.DeltaSharingFileIndex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Before query planning, we prepare any scans over delta sharing tables by pushing
 * any filters or limits to delta sharing server through RPC, allowing us to return only needed
 * files and gather more accurate statistics for CBO and metering.
 */
class PrepareDeltaSharingScan(override val spark: SparkSession) extends PrepareDeltaScan(spark) {

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
        logInfo(s"Classified filters: partition: $partitionFilters, data: $dataFilters")
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
