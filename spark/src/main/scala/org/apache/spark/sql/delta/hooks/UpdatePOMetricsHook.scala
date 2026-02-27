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
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.{
  UC_TABLE_ID_KEY, UC_TABLE_ID_KEY_OLD}

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.stats.FileSizeHistogram

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

  // Key injected by the open-source UCSingleCatalog into CatalogTable.storage.properties.
  // Delta's DeltaTableV2.properties() surfaces this as "option.fs.unitycatalog.table.id".
  private val UC_TABLE_ID_STORAGE_KEY = "fs.unitycatalog.table.id"

  // Default bin boundaries for the file size histogram (in bytes).
  // Must start at 0. Boundaries cover the typical Parquet file size range.
  private val FILE_SIZE_BIN_BOUNDARIES: IndexedSeq[Long] = IndexedSeq(
    0L,
    8L * 1024, // 8 KB
    64L * 1024, // 64 KB
    512L * 1024, // 512 KB
    1L << 20, // 1 MB
    4L << 20, // 4 MB
    8L << 20, // 8 MB
    16L << 20, // 16 MB
    32L << 20, // 32 MB
    64L << 20, // 64 MB
    128L << 20, // 128 MB
    256L << 20, // 256 MB
    512L << 20, // 512 MB
    1L << 30 // 1 GB
  )

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!isUCManagedTable(txn.deltaLog, catalogTable)) return

    try {
      val tableId = resolveTableId(catalogTable, txn.deltaLog)
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
      committedActions: Seq[Action],
      committedVersion: Long): ReportDeltaMetricsRequest = {
    val commitInfo = committedActions.collectFirst { case ci: CommitInfo => ci }
    val opMetrics = commitInfo.flatMap(_.operationMetrics).getOrElse(Map.empty)

    val addFiles = committedActions.collect { case a: AddFile => a }
    val removeFiles = committedActions.collect { case r: RemoveFile => r }

    val commitReport = CommitReport(
      numFilesAdded = Some(addFiles.size.toLong),
      numFilesRemoved = Some(removeFiles.size.toLong),
      numBytesAdded = Some(addFiles.map(_.size).sum),
      numBytesRemoved = Some(removeFiles.flatMap(_.size).sum),
      numClusteredBytesAdded = Some(
        addFiles.filter(_.clusteringProvider.isDefined).map(_.size).sum),
      // num_clustered_bytes_removed: not available - RemoveFile has no clusteringProvider
      numRowsInserted = extractRowsInserted(opMetrics, addFiles),
      numRowsRemoved = extractRowsRemoved(opMetrics, removeFiles),
      numRowsUpdated = extractRowsUpdated(opMetrics),
      fileSizeHistogram = Some(buildFileSizeHistogram(addFiles, committedVersion))
    )

    ReportDeltaMetricsRequest(
      tableId = tableId,
      report = CommitReportEnvelope(commitReport)
    )
  }

  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    logWarning(
      log"PO metrics hook failed for version ${MDC(DeltaLogKeys.VERSION, version)}: " +
      log"${MDC(DeltaLogKeys.ERROR, error.getMessage)}", error)
  }

  // ---------------------------------------------------------------------------
  // Row metric extraction - prefer operationMetrics for accuracy; fall back to
  // file-level numLogicalRecords when operationMetrics are absent (e.g. external
  // writers that don't emit CommitInfo, or simple WRITE operations).
  // ---------------------------------------------------------------------------

  /**
   * numRowsInserted:
   *  - MERGE  -&gt; numTargetRowsInserted
   *  - WRITE / STREAMING_UPDATE -&gt; numOutputRows
   *  - fallback -&gt; sum of AddFile.numLogicalRecords
   */
  private def extractRowsInserted(
      opMetrics: Map[String, String],
      addFiles: Seq[AddFile]): Option[Long] = {
    opMetrics.get("numTargetRowsInserted")
      .orElse(opMetrics.get("numOutputRows"))
      .flatMap(toLong)
      .orElse {
        val fromStats = addFiles.flatMap(_.numLogicalRecords)
        if (fromStats.nonEmpty) Some(fromStats.sum) else None
      }
  }

  /**
   * numRowsRemoved:
   *  - MERGE  -&gt; numTargetRowsDeleted
   *  - DELETE -&gt; numDeletedRows
   *  - fallback -&gt; sum of RemoveFile.numLogicalRecords
   */
  private def extractRowsRemoved(
      opMetrics: Map[String, String],
      removeFiles: Seq[RemoveFile]): Option[Long] = {
    opMetrics.get("numTargetRowsDeleted")
      .orElse(opMetrics.get("numDeletedRows"))
      .flatMap(toLong)
      .orElse {
        val fromStats = removeFiles.flatMap(_.numLogicalRecords)
        if (fromStats.nonEmpty) Some(fromStats.sum) else None
      }
  }

  /**
   * numRowsUpdated:
   *  - MERGE  -&gt; numTargetRowsUpdated
   *  - UPDATE -&gt; numUpdatedRows
   *  - No file-level fallback (updated rows are indistinguishable from inserts in file stats)
   */
  private def extractRowsUpdated(opMetrics: Map[String, String]): Option[Long] = {
    opMetrics.get("numTargetRowsUpdated")
      .orElse(opMetrics.get("numUpdatedRows"))
      .flatMap(toLong)
  }

  private def toLong(s: String): Option[Long] =
    try Some(s.toLong)
    catch { case _: NumberFormatException => None }

  // ---------------------------------------------------------------------------
  // File size histogram
  // ---------------------------------------------------------------------------

  /**
   * Builds a file-size histogram from the AddFiles in this commit.
   * commit_version is required by the server to validate the payload is not stale.
   */
  private def buildFileSizeHistogram(
      addFiles: Seq[AddFile],
      committedVersion: Long): FileSizeHistogramPayload = {
    val histogram = FileSizeHistogram(FILE_SIZE_BIN_BOUNDARIES)
    addFiles.foreach(f => histogram.insert(f.size))
    FileSizeHistogramPayload(
      sortedBinBoundaries = histogram.sortedBinBoundaries,
      fileCounts = histogram.fileCounts.toSeq,
      totalBytes = histogram.totalBytes.toSeq,
      commitVersion = Some(committedVersion)
    )
  }

  // ---------------------------------------------------------------------------
  // UC table detection
  // ---------------------------------------------------------------------------

  /**
   * Resolves the table ID to send in the PO metrics payload.
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
   * was written by a non-DBR client (which generates a random UUID), causing the PO endpoint
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
        ct.identifier.catalog.isDefined ||
        (ct.properties.get("provider").exists(
          _.toLowerCase(java.util.Locale.ROOT) == "delta") &&
          deltaLog.tableId.nonEmpty)
      case None => false
    }
  }
}
