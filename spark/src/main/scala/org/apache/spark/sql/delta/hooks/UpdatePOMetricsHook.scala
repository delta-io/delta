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

package org.apache.spark.sql.delta.hooks

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Post-commit hook that sends commit metrics to the PO (Predictive Optimization) endpoint
 * for Unity Catalog managed Delta tables.
 *
 * Endpoint: POST /api/2.1/unity-catalog/delta/preview/metrics
 *
 * The server validates:
 *  - table_id must match a known UC Delta table
 *  - commit_version (in file_size_histogram) must be within validCommitVersionWindow of
 *    the latest table version tracked by UC
 *  - All numeric fields must be non-negative
 *
 * This hook is best-effort: failures are logged as warnings but never fail the commit.
 *
 * @param catalogTable The catalog table metadata (required to identify UC-managed tables)
 */
case class UpdatePOMetricsHook(catalogTable: Option[CatalogTable])
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update PO Metrics"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!isUCManagedTable(txn.deltaLog, catalogTable)) return

    try {
      val tableId = txn.deltaLog.tableId
      if (tableId.isEmpty) {
        throw new IllegalStateException("UC-managed table must have a table ID")
      }

      val request = buildRequest(tableId, txn.committedActions, txn.committedVersion)
      val catalogName = catalogTable.flatMap(_.identifier.catalog)
      POMetricsClient.sendMetrics(spark, request, catalogName = catalogName)

      logInfo(
        log"Successfully sent PO metrics for table " +
        log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
        log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}")

    } catch {
      case e: Exception =>
        logWarning(
          log"Failed to send PO metrics for table " +
          log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
          log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}: " +
          log"${MDC(DeltaLogKeys.ERROR, e.getMessage)}", e)
    }
  }

  // ---------------------------------------------------------------------------
  // Request builder - package-private for direct testing without Delta infrastructure
  // ---------------------------------------------------------------------------

  private[hooks] def buildRequest(
      tableId: String,
      _committedActions: Seq[Action],
      _committedVersion: Long): ReportDeltaMetricsRequest = {
    ReportDeltaMetricsRequest(
      tableId = tableId,
      report = CommitReportEnvelope(CommitReport())
    )
  }

  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    logWarning(
      log"PO metrics hook failed for version ${MDC(DeltaLogKeys.VERSION, version)}: " +
      log"${MDC(DeltaLogKeys.ERROR, error.getMessage)}", error)
  }

  private def isUCManagedTable(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable]): Boolean = {
    if (deltaLog.tableId.isEmpty) return false

    catalogTable match {
      case Some(ct) =>
        ct.identifier.catalog.isDefined ||
        (ct.properties.get("provider").exists(
          _.toLowerCase(java.util.Locale.ROOT) == "delta") &&
          deltaLog.tableId.nonEmpty)
      case None => false
    }
  }
}
