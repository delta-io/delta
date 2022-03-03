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

package org.apache.spark.sql.delta.test

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.metering.ScanReport
import org.apache.spark.sql.delta.stats.{DataSize, PreparedDeltaFileIndex}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

trait ScanReportHelper extends SharedSparkSession with AdaptiveSparkPlanHelper {

  import ScanReportHelper._

  /**
   * Collect the scan leaves in the given SparkPlan.
   */
  private def collectScans(plan: SparkPlan): Seq[FileSourceScanExec] = {
    collectWithSubqueries(plan)({
      case fs: FileSourceScanExec => Seq(fs)
      case cached: InMemoryTableScanExec => collectScans(cached.relation.cacheBuilder.cachedPlan)
    }).flatten
  }

  private def getListener(): QueryExecutionListener = {
    new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        try qe.assertAnalyzed() catch {
          case NonFatal(e) =>
            logDebug("Not running Delta Metering because the query failed during analysis.", e)
            return
        }

        val fileScans = collectScans(qe.executedPlan)

        for (scanExec <- fileScans) {
          scanExec.relation.location match {
            case deltaTable: PreparedDeltaFileIndex =>
              val preparedScan = deltaTable.preparedScan
              // The names of the partition columns that were used as filters in this scan.
              // Convert this to a set first to avoid double-counting partition columns that might
              // appear multiple times.
              val usedPartitionColumns =
              preparedScan.partitionFilters.map(_.references.map(_.name)).flatten.toSet.toSeq
              val report = ScanReport(
                tableId = deltaTable.deltaLog.snapshot.metadata.id,
                path = deltaTable.path.toString,
                scanType = "delta-query",
                deltaDataSkippingType = preparedScan.dataSkippingType.toString,
                partitionFilters = preparedScan.partitionFilters.map(_.sql).toSeq,
                dataFilters = preparedScan.dataFilters.map(_.sql).toSeq,
                unusedFilters = preparedScan.unusedFilters.map(_.sql).toSeq,
                size = Map(
                  "total" -> preparedScan.total,
                  "partition" -> preparedScan.partition,
                  "scanned" -> preparedScan.scanned),
                metrics = scanExec.metrics.mapValues(_.value).toMap +
                  ("scanDurationMs" -> preparedScan.scanDurationMs),
                annotations = Map.empty, // TODO(scott): annotations
                versionScanned = deltaTable.versionScanned,
                usedPartitionColumns = usedPartitionColumns,
                numUsedPartitionColumns = usedPartitionColumns.size,
                allPartitionColumns = deltaTable.deltaLog.snapshot.metadata.partitionColumns,
                numAllPartitionColumns = deltaTable.deltaLog.snapshot.metadata.partitionColumns.size
              )

              scans += report

            case deltaTable: TahoeFileIndex =>
              val report = ScanReport(
                tableId = deltaTable.deltaLog.snapshot.metadata.id,
                path = deltaTable.path.toString,
                scanType = "delta-unknown",
                partitionFilters = Nil,
                dataFilters = Nil,
                unusedFilters = Nil,
                size = Map(
                  "total" -> DataSize(
                    bytesCompressed = Some(deltaTable.deltaLog.snapshot.sizeInBytes))
                ),
                metrics = scanExec.metrics.mapValues(_.value).toMap,
                versionScanned = None,
                annotations = Map.empty // TODO(scott): annotations
              )

              scans += report

            case _ if scanExec.relation.location.rootPaths.nonEmpty =>
              val partitionBytes = scanExec.selectedPartitions.map(_.files.map(_.getLen).sum).sum
              val report = ScanReport(
                tableId = null,
                path = scanExec.relation.location.rootPaths.head.toString,
                scanType = scanExec.relation.fileFormat.toString,
                partitionFilters = scanExec.partitionFilters.map(_.sql),
                dataFilters = scanExec.dataFilters.map(_.sql),
                unusedFilters = Nil,
                size = Map(
                  "partition" -> DataSize(bytesCompressed = Some(partitionBytes))
                ),
                metrics = scanExec.metrics.mapValues(_.value).toMap,
                versionScanned = None,
                annotations = Map.empty // TODO(scott): annotations
              )

              scans += report

            case _ => // ignore
          }
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = { }
    }
  }

  def getScanReport(f: => Unit): Seq[ScanReport] = {
    synchronized {
      assert(scans == null, "getScanReport does not support nested invocation.")
      scans = scala.collection.mutable.ArrayBuffer.empty[ScanReport]
    }

    val listener = getListener()
    spark.listenerManager.register(listener)

    var result: scala.collection.mutable.ArrayBuffer[ScanReport] = null
    try {
      f
    } finally {
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      spark.listenerManager.unregister(listener)

      result = scans
      synchronized {
        scans = null
      }
    }

    result.toSeq
  }
}

object ScanReportHelper {
  @volatile var scans: scala.collection.mutable.ArrayBuffer[ScanReport] = null
}
