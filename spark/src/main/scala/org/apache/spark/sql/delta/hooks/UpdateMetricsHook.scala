/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.CatalogTableUtils

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Post-commit hook that sends commit metrics to Unity Catalog for UC-managed Delta tables.
 *
 * This hook is best-effort: failures are logged as warnings but never fail the commit.
 *
 * @param catalogTable The catalog table metadata (required to identify UC-managed tables)
 */
case class UpdateMetricsHook(catalogTable: Option[CatalogTable])
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update UC Metrics"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    val ct = catalogTable.orNull
    if (ct == null || !CatalogTableUtils.isUnityCatalogManagedTable(ct)) return

    val tableId = ct.storage.properties
      .get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
      .filter(_.nonEmpty)
    if (tableId.isEmpty) {
      logWarning(
        log"Skipping UC metrics: table ID not found in storage properties" +
        log" for ${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)}")
      return
    }

    try {
      val request = ReportDeltaMetrics.buildRequest(
        tableId.get, txn.committedActions, txn.committedVersion)
      val catalogName = ct.identifier.catalog
      UCMetricsClient.sendMetrics(
        spark, request, catalogName = catalogName)

      logInfo(
        log"Successfully sent UC metrics for table " +
        log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
        log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}")

    } catch {
      case e: Exception =>
        logWarning(
          log"Failed to send UC metrics for table " +
          log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
          log"version ${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}: " +
          log"${MDC(DeltaLogKeys.ERROR, e.getMessage)}", e)
    }
  }

  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    logWarning(
      log"UC metrics hook failed for version ${MDC(DeltaLogKeys.VERSION, version)}: " +
      log"${MDC(DeltaLogKeys.ERROR, error.getMessage)}", error)
  }
}
