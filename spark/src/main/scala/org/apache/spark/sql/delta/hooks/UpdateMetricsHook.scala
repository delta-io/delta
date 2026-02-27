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
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.{
  UC_TABLE_ID_KEY, UC_TABLE_ID_KEY_OLD}

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}

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

  // Key injected by the open-source UCSingleCatalog into CatalogTable.storage.properties.
  // Delta's DeltaTableV2.properties() surfaces this as "option.fs.unitycatalog.table.id".
  private val UC_TABLE_ID_STORAGE_KEY = "fs.unitycatalog.table.id"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!isUCManagedTable(txn.deltaLog, catalogTable)) return

    try {
      val tableId = resolveTableId(catalogTable, txn.deltaLog)
      if (tableId.isEmpty) {
        throw new IllegalStateException("UC-managed table must have a table ID")
      }

      val request = ReportDeltaMetrics.buildRequest(
        tableId, txn.committedActions, txn.committedVersion)
      val catalogName = catalogTable.flatMap(_.identifier.catalog)
      UCMetricsClient.sendMetrics(spark, request, catalogName = catalogName)

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

  // ---------------------------------------------------------------------------
  // UC table detection
  // ---------------------------------------------------------------------------

  /**
   * Resolves the table ID to send in the UC metrics payload.
   *
   * Resolution order (first non-empty wins):
   *  1. CatalogTable.properties["io.unitycatalog.tableId"]
   *     set by the Databricks-internal UC connector
   *  2. CatalogTable.properties["ucTableId"]
   *     legacy Databricks-internal key
   *  3. CatalogTable.storage.properties["fs.unitycatalog.table.id"]
   *     set by the open-source UCSingleCatalog connector
   *  4. DeltaLog.tableId (Delta Metadata.id)
   *     fallback; matches UC ID on DBR-created tables
   *
   * The Delta Metadata.id diverges from the UC-registered table ID when the first Delta commit
   * was written by a non-DBR client (which generates a random UUID), causing the UC endpoint
   * to return 404. The catalog properties always carry the authoritative UC-registered table ID.
   */
  private[hooks] def resolveTableId(
      catalogTable: Option[CatalogTable],
      deltaLog: DeltaLog): String = {
    catalogTable
      .flatMap { ct =>
        ct.properties.get(UC_TABLE_ID_KEY)
          .orElse(ct.properties.get(UC_TABLE_ID_KEY_OLD))
          .orElse(ct.storage.properties.get(UC_TABLE_ID_STORAGE_KEY))
          .filter(_.nonEmpty)
      }
      .getOrElse(deltaLog.tableId)
  }

  private def isUCManagedTable(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable]): Boolean = {
    if (deltaLog.tableId.isEmpty) return false

    catalogTable match {
      case Some(ct) =>
        ct.tableType == CatalogTableType.MANAGED &&
        (ct.identifier.catalog.isDefined ||
        ct.properties.get("provider").exists(
          _.toLowerCase(java.util.Locale.ROOT) == "delta"))
      case None => false
    }
  }
}
