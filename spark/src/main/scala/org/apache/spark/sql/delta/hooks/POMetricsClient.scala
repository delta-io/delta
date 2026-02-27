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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession

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
 * HTTP client for sending commit metrics to the UC PO endpoint.
 *
 * Endpoint: POST /api/2.1/unity-catalog/delta/preview/metrics
 *
 * The server (ReportDeltaMetricsHandler) will:
 *  1. Look up the table via getTableById to fetch PO-enable status and latest version
 *  2. Validate commit_version is within validCommitVersionWindow of the latest version
 *  3. Validate all numeric fields are non-negative
 *  4. Forward the metrics to PO via PredictiveOptimizationClient.pushExternalDeltaCommitMetrics
 */
object POMetricsClient {

  /**
   * Sends commit metrics to the PO endpoint synchronously.
   *
   * @param spark   The SparkSession (used to read configuration)
   * @param request The fully-constructed request payload
   * @throws Exception if the HTTP request fails (caller should catch and log)
   */
  def sendMetrics(spark: SparkSession, request: ReportDeltaMetricsRequest): Unit = {
    val endpointUrl = getEndpointUrl(spark)
    val authToken = getAuthToken(spark)
    val timeoutMs = getTimeoutMs(spark)

    val requestConfig = RequestConfig.custom()
      .setConnectTimeout(timeoutMs.toInt)
      .setSocketTimeout(timeoutMs.toInt)
      .setConnectionRequestTimeout(timeoutMs.toInt)
      .build()

    val httpClient: CloseableHttpClient = HttpClientBuilder.create()
      .setDefaultRequestConfig(requestConfig)
      .build()

    try {
      val httpPost = new HttpPost(endpointUrl)
      httpPost.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType)
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $authToken")

      val jsonPayload = JsonUtils.toJson(request)
      httpPost.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))

      val response = httpClient.execute(httpPost)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode < 200 || statusCode >= 300) {
          val responseBody = if (response.getEntity != null) {
            EntityUtils.toString(response.getEntity)
          } else {
            "<no response body>"
          }
          throw new RuntimeException(
            s"PO metrics endpoint returned error status $statusCode: $responseBody")
        }
      } finally {
        response.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def getEndpointUrl(spark: SparkSession): String = {
    spark.conf.getOption(DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key) match {
      case Some(url) if url.nonEmpty => url
      case _ =>
        val key = DeltaSQLConf.DELTA_PO_METRICS_ENDPOINT.key
        throw new IllegalArgumentException(
          s"PO metrics endpoint URL not configured. Set $key")
    }
  }

  private def getAuthToken(spark: SparkSession): String = {
    spark.conf.getOption(DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key)
      .orElse(Option(System.getenv("DATABRICKS_TOKEN"))) match {
      case Some(token) if token.nonEmpty => token
      case _ =>
        val key = DeltaSQLConf.DELTA_PO_METRICS_AUTH_TOKEN.key
        throw new IllegalArgumentException(
          s"PO metrics auth token not configured. Set $key or " +
          "DATABRICKS_TOKEN environment variable")
    }
  }

  private def getTimeoutMs(spark: SparkSession): Long = {
    spark.conf.get(DeltaSQLConf.DELTA_PO_METRICS_TIMEOUT_MS)
  }
}
