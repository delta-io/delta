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

package org.apache.spark.sql.delta.hooks.metrics

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{CatalogTableUtils, JsonUtils}

import io.unitycatalog.client.auth.TokenProvider
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

// Payload case classes (JSON-serialized via Jackson @JsonProperty)

case class ReportDeltaMetricsRequest(
    @JsonProperty("table_id") tableId: String,
    @JsonProperty("report") report: CommitReportEnvelope)

case class CommitReportEnvelope(
    @JsonProperty("commit_report") commitReport: CommitReport)

/**
 * Commit-level file and row metrics. All fields are optional.
 */
case class CommitReport(
    @JsonProperty("num_files_added")
      numFilesAdded: Option[Long] = None,
    @JsonProperty("num_files_removed")
      numFilesRemoved: Option[Long] = None,
    @JsonProperty("num_bytes_added")
      numBytesAdded: Option[Long] = None,
    @JsonProperty("num_bytes_removed")
      numBytesRemoved: Option[Long] = None,
    @JsonProperty("num_rows_inserted")
      numRowsInserted: Option[Long] = None,
    @JsonProperty("num_rows_removed")
      numRowsRemoved: Option[Long] = None,
    @JsonProperty("num_rows_updated")
      numRowsUpdated: Option[Long] = None,
    @JsonProperty("file_size_histogram")
      fileSizeHistogram: Option[FileSizeHistogramPayload] = None)

case class FileSizeHistogramPayload(
    @JsonProperty("sorted_bin_boundaries")
      sortedBinBoundaries: Seq[Long],
    @JsonProperty("file_counts") fileCounts: Seq[Long],
    @JsonProperty("total_bytes") totalBytes: Seq[Long],
    @JsonProperty("commit_version")
      commitVersion: Option[Long] = None)

/**
 * Post-commit hook that sends commit metrics to Unity Catalog
 * for UC-managed Delta tables.
 *
 * Best-effort: failures are logged as warnings and never fail
 * the commit.
 *
 * @param catalogTable catalog metadata for UC-managed detection
 */
case class UpdateMetricsHook(catalogTable: Option[CatalogTable])
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update catalog metrics"

  override def run(
      spark: SparkSession,
      txn: CommittedTransaction): Unit = {
    val ct = catalogTable.orNull
    if (ct == null ||
        !CatalogTableUtils.isUnityCatalogManagedTable(ct)) {
      return
    }
    val tableId = ct.storage.properties(
      UCCommitCoordinatorClient.UC_TABLE_ID_KEY)

    try {
      val request = UpdateMetricsHook.buildRequest(
        tableId, txn.committedActions, txn.committedVersion)
      val catalogName = ct.identifier.catalog
      UpdateMetricsHook.sendMetrics(
        spark, request, catalogName = catalogName)

      logDebug(
        log"Successfully sent UC metrics for table " +
        log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
        log"version " +
        log"${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}")

    } catch {
      case e: Exception =>
        logWarning(
          log"Failed to send UC metrics for table " +
          log"${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
          log"version " +
          log"${MDC(DeltaLogKeys.VERSION, txn.committedVersion)}" +
          log": ${MDC(DeltaLogKeys.ERROR, e.getMessage)}", e)
    }
  }
}

object UpdateMetricsHook {

  // -- Payload builder --

  private[metrics] def buildRequest(
      tableId: String,
      committedActions: Seq[Action],
      committedVersion: Long): ReportDeltaMetricsRequest = {
    val commitInfo =
      committedActions.collectFirst { case ci: CommitInfo => ci }
    val opMetrics =
      commitInfo.flatMap(_.operationMetrics).getOrElse(Map.empty)

    val addFiles =
      committedActions.collect { case a: AddFile => a }
    val removeFiles =
      committedActions.collect { case r: RemoveFile => r }

    val commitReport = CommitReport(
      numFilesAdded = Some(addFiles.size.toLong),
      numFilesRemoved = Some(removeFiles.size.toLong),
      numBytesAdded = Some(addFiles.map(_.size).sum),
      numBytesRemoved = Some(removeFiles.flatMap(_.size).sum),
      numRowsInserted =
        extractRowsInserted(opMetrics, addFiles),
      numRowsRemoved =
        extractRowsRemoved(opMetrics, removeFiles),
      numRowsUpdated = extractRowsUpdated(opMetrics),
      fileSizeHistogram = Some(
        buildFileSizeHistogram(addFiles, committedVersion))
    )

    ReportDeltaMetricsRequest(
      tableId = tableId,
      report = CommitReportEnvelope(commitReport)
    )
  }

  // operationMetrics keys vary by operation: MERGE writes
  // numTargetRowsInserted, WRITE writes numOutputRows.
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

  // MERGE writes numTargetRowsDeleted, DELETE writes
  // numDeletedRows.
  private def extractRowsRemoved(
      opMetrics: Map[String, String],
      removeFiles: Seq[RemoveFile]): Option[Long] = {
    opMetrics.get("numTargetRowsDeleted")
      .orElse(opMetrics.get("numDeletedRows"))
      .flatMap(toLong)
      .orElse {
        val fromStats =
          removeFiles.flatMap(_.numLogicalRecords)
        if (fromStats.nonEmpty) Some(fromStats.sum) else None
      }
  }

  // MERGE writes numTargetRowsUpdated, UPDATE writes
  // numUpdatedRows.
  private def extractRowsUpdated(
      opMetrics: Map[String, String]): Option[Long] = {
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
    val histogram = FileSizeHistogram.emptyHistogram
    addFiles.foreach(f => histogram.insert(f.size))
    FileSizeHistogramPayload(
      sortedBinBoundaries = histogram.sortedBinBoundaries,
      fileCounts = histogram.fileCounts.toSeq,
      totalBytes = histogram.totalBytes.toSeq,
      commitVersion = Some(committedVersion)
    )
  }

  // -- HTTP client --

  private val CATALOG_CONF_PREFIX = "spark.sql.catalog"
  private val CATALOG_URI_CONF_SUFFIX = "uri"
  private val METRICS_ENDPOINT_PATH =
    "/api/2.1/unity-catalog/delta/preview/metrics"
  private val HTTP_TIMEOUT_MS = 5000L

  private[metrics] def sendMetrics(
      spark: SparkSession,
      request: ReportDeltaMetricsRequest,
      catalogName: Option[String] = None): Unit = {
    val catalog = catalogName.getOrElse(
      throw new IllegalArgumentException(
        "Catalog name required for UC metrics; " +
        "endpoint URI is read from " +
        "spark.sql.catalog.<name>.uri"))
    val endpointUrl = getEndpointUrl(spark, catalog)
    val authToken = getAuthToken(spark, catalog)

    val requestConfig = RequestConfig.custom()
      .setConnectTimeout(HTTP_TIMEOUT_MS.toInt)
      .setSocketTimeout(HTTP_TIMEOUT_MS.toInt)
      .setConnectionRequestTimeout(HTTP_TIMEOUT_MS.toInt)
      .build()

    val httpClient: CloseableHttpClient =
      HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .build()

    try {
      val httpPost = new HttpPost(endpointUrl)
      httpPost.setHeader(
        HttpHeaders.AUTHORIZATION, s"Bearer $authToken")

      val jsonPayload = JsonUtils.toJson(request)
      httpPost.setEntity(
        new StringEntity(
          jsonPayload, ContentType.APPLICATION_JSON))

      val response = httpClient.execute(httpPost)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode < 200 || statusCode >= 300) {
          val responseBody = Option(response.getEntity)
            .map(EntityUtils.toString)
            .getOrElse("<no response body>")
          throw new RuntimeException(
            "UC metrics endpoint returned error " +
            s"status $statusCode: $responseBody")
        }
      } finally {
        response.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def getEndpointUrl(
      spark: SparkSession,
      catalogName: String): String = {
    val uriKey = s"$CATALOG_CONF_PREFIX.$catalogName" +
      s".$CATALOG_URI_CONF_SUFFIX"
    spark.conf.getOption(uriKey) match {
      case Some(uri) if uri.nonEmpty =>
        s"${uri.stripSuffix("/")}$METRICS_ENDPOINT_PATH"
      case _ =>
        throw new IllegalArgumentException(
          s"UC catalog base URI not configured. " +
          s"Set $uriKey")
    }
  }

  private def getAuthToken(
      spark: SparkSession,
      catalogName: String): String = {
    val configMap =
      UCCommitCoordinatorBuilder.getCatalogConfigMap(spark)
    val config = configMap.get(catalogName).getOrElse(
      throw new IllegalArgumentException(
        "Unity Catalog configuration not found for " +
        s"catalog '$catalogName'. Configure " +
        "spark.sql.catalog.<catalog>.uri and auth " +
        "(auth.type/auth.token or legacy .token)."))
    val tokenProvider =
      TokenProvider.create(config.authConfig.asJava)
    tokenProvider.accessToken()
  }
}
