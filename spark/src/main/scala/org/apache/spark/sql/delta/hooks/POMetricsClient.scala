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
 * Skeleton payload for transport wiring validation. The full metrics payload
 * is added in a follow-up change.
 */
case class CommitReport()

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
  private val UC_CATALOG_URI_CONF_KEY = "spark.sql.catalog.migration_bugbash.uri"
  private val CATALOG_TOKEN_CONF_PREFIX = "spark.sql.catalog."
  private val CATALOG_TOKEN_CONF_SUFFIX = ".token"
  private val PO_METRICS_ENDPOINT_SUFFIX = "/api/2.1/unity-catalog/delta/preview/metrics"
  private val HTTP_TIMEOUT_MS = 5000L

  /**
   * Sends commit metrics to the PO endpoint synchronously.
   *
   * @param spark   The SparkSession (used to read configuration)
   * @param request The fully-constructed request payload
   * @throws Exception if the HTTP request fails (caller should catch and log)
   */
  def sendMetrics(
      spark: SparkSession,
      request: ReportDeltaMetricsRequest,
      catalogName: Option[String] = None): Unit = {
    val endpointUrl = getEndpointUrl(spark)
    val authToken = getAuthToken(spark, catalogName)

    val requestConfig = RequestConfig.custom()
      .setConnectTimeout(HTTP_TIMEOUT_MS.toInt)
      .setSocketTimeout(HTTP_TIMEOUT_MS.toInt)
      .setConnectionRequestTimeout(HTTP_TIMEOUT_MS.toInt)
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
    spark.conf.getOption(UC_CATALOG_URI_CONF_KEY) match {
      case Some(uri) if uri.nonEmpty =>
        s"${uri.stripSuffix("/")}$PO_METRICS_ENDPOINT_SUFFIX"
      case _ =>
        val key = UC_CATALOG_URI_CONF_KEY
        throw new IllegalArgumentException(
          s"UC catalog base URI not configured. Set $key")
    }
  }

  private def getAuthToken(spark: SparkSession, catalogName: Option[String]): String = {
    val token = catalogName
      .map(name => s"$CATALOG_TOKEN_CONF_PREFIX$name$CATALOG_TOKEN_CONF_SUFFIX")
      .flatMap(spark.conf.getOption)
      .filter(_.nonEmpty)

    token match {
      case Some(token) if token.nonEmpty => token
      case _ =>
        val keyHint = catalogName
          .map(name => s"$CATALOG_TOKEN_CONF_PREFIX$name$CATALOG_TOKEN_CONF_SUFFIX")
          .getOrElse(
          s"$CATALOG_TOKEN_CONF_PREFIX<catalog>$CATALOG_TOKEN_CONF_SUFFIX")
        throw new IllegalArgumentException(
          s"PO metrics auth token not configured. Set $keyHint")
    }
  }

}
