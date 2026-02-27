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

import com.fasterxml.jackson.annotation.JsonProperty

import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.stats.FileSizeHistogram

/**
 * Top-level request body for POST /api/2.1/unity-catalog/delta/preview/metrics.
 *
 * @param tableId UUID of the UC-managed Delta table
 * @param report  The commit metrics, wrapped in a typed report envelope
 */
case class ReportDeltaMetricsRequest(
    @JsonProperty("table_id") tableId: String,
    @JsonProperty("report") report: CommitReportEnvelope)

/**
 * Envelope that matches the server's @JsonSubTypes WRAPPER_OBJECT format.
 * Serializes as: { "commit_report": { ... } }
 */
case class CommitReportEnvelope(
    @JsonProperty("commit_report") commitReport: CommitReport)

/**
 * Commit-level file and row metrics.
 *
 * All fields are optional - the server validates they are non-negative if present.
 * num_clustered_bytes_removed is intentionally omitted: RemoveFile carries no
 * clusteringProvider, so we cannot compute it from the commit log alone.
 *
 * commit_version is conveyed via file_size_histogram.commit_version and is
 * required by the server to validate the payload is not stale.
 */
case class CommitReport(
    @JsonProperty("num_files_added") numFilesAdded: Option[Long] = None,
    @JsonProperty("num_files_removed") numFilesRemoved: Option[Long] = None,
    @JsonProperty("num_bytes_added") numBytesAdded: Option[Long] = None,
    @JsonProperty("num_bytes_removed") numBytesRemoved: Option[Long] = None,
    @JsonProperty("num_clustered_bytes_added") numClusteredBytesAdded: Option[Long] = None,
    // num_clustered_bytes_removed omitted: not derivable from RemoveFile (no clusteringProvider)
    @JsonProperty("num_rows_inserted") numRowsInserted: Option[Long] = None,
    @JsonProperty("num_rows_removed") numRowsRemoved: Option[Long] = None,
    @JsonProperty("num_rows_updated") numRowsUpdated: Option[Long] = None,
    @JsonProperty("file_size_histogram") fileSizeHistogram: Option[FileSizeHistogramPayload] = None)

/**
 * File size distribution for the added files in this commit.
 * commit_version is also used by the server to validate payload freshness.
 */
case class FileSizeHistogramPayload(
    @JsonProperty("sorted_bin_boundaries") sortedBinBoundaries: Seq[Long],
    @JsonProperty("file_counts") fileCounts: Seq[Long],
    @JsonProperty("total_bytes") totalBytes: Seq[Long],
    @JsonProperty("commit_version") commitVersion: Option[Long] = None)

/**
 * Builds the UC metrics request payload from committed transaction data.
 *
 * Package-private for direct testing without Delta infrastructure.
 */
object ReportDeltaMetrics {

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

  private def extractRowsUpdated(opMetrics: Map[String, String]): Option[Long] = {
    opMetrics.get("numTargetRowsUpdated")
      .orElse(opMetrics.get("numUpdatedRows"))
      .flatMap(toLong)
  }

  private def toLong(s: String): Option[Long] =
    try Some(s.toLong)
    catch { case _: NumberFormatException => None }

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
}
