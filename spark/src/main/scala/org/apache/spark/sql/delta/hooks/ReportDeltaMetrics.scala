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
 * commit_version is conveyed via file_size_histogram.commit_version and is
 * required by the server to validate the payload is not stale.
 */
case class CommitReport(
    @JsonProperty("num_files_added") numFilesAdded: Option[Long] = None,
    @JsonProperty("num_files_removed") numFilesRemoved: Option[Long] = None,
    @JsonProperty("num_bytes_added") numBytesAdded: Option[Long] = None,
    @JsonProperty("num_bytes_removed") numBytesRemoved: Option[Long] = None,
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

  private val KB = 1024L
  private val MB = 1024L * KB
  private val GB = 1024L * MB

  private val FILE_SIZE_BIN_BOUNDARIES: IndexedSeq[Long] = IndexedSeq(
    0L,
    8*KB, 16*KB, 32*KB, 64*KB, 128*KB, 256*KB,
    512*KB, 1*MB, 2*MB, 4*MB,
    8*MB, 12*MB, 16*MB, 20*MB, 24*MB, 28*MB,
    32*MB, 36*MB, 40*MB,
    48*MB, 56*MB, 64*MB, 72*MB, 80*MB,
    88*MB, 96*MB, 104*MB, 112*MB, 120*MB,
    124*MB, 128*MB, 132*MB, 136*MB, 140*MB, 144*MB,
    160*MB, 176*MB, 192*MB, 208*MB, 224*MB, 240*MB,
    256*MB, 272*MB, 288*MB, 304*MB, 320*MB, 336*MB,
    352*MB, 368*MB, 384*MB, 400*MB, 416*MB, 432*MB,
    448*MB, 464*MB, 480*MB, 496*MB, 512*MB, 528*MB,
    544*MB, 560*MB, 576*MB,
    640*MB, 704*MB, 768*MB, 832*MB, 896*MB, 960*MB,
    1024*MB, 1088*MB, 1152*MB, 1216*MB, 1280*MB,
    1344*MB, 1408*MB,
    1536*MB, 1664*MB, 1792*MB, 1920*MB, 2048*MB,
    2304*MB, 2560*MB, 2816*MB, 3072*MB,
    3328*MB, 3584*MB, 3840*MB, 4*GB,
    8*GB, 16*GB, 32*GB, 64*GB, 128*GB, 256*GB
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
