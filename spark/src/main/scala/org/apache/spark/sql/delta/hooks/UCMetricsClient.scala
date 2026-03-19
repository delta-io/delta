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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.util.JsonUtils

import io.unitycatalog.client.auth.TokenProvider
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession

/**
 * HTTP client for sending commit metrics to the UC metrics endpoint.
 */
object UCMetricsClient {
  private val CATALOG_CONF_PREFIX = "spark.sql.catalog"
  private val CATALOG_URI_CONF_SUFFIX = "uri"
  private val METRICS_ENDPOINT_PATH = "/api/2.1/unity-catalog/delta/preview/metrics"
  private val HTTP_TIMEOUT_MS = 10000L

  /**
   * Sends commit metrics to the UC metrics endpoint synchronously.
   *
   * @param spark   The SparkSession (used to read configuration)
   * @param request The fully-constructed request payload
   * @param catalogName The catalog name (required to resolve endpoint URI and auth token)
   * @throws Exception if the HTTP request fails (caller should catch and log)
   */
  def sendMetrics(
      spark: SparkSession,
      request: ReportDeltaMetricsRequest,
      catalogName: Option[String] = None): Unit = {
    val catalog = catalogName.getOrElse(
      throw new IllegalArgumentException(
        "Catalog name required for UC metrics; " +
        "endpoint URI is read from spark.sql.catalog.<name>.uri"))
    val endpointUrl = getEndpointUrl(spark, catalog)
    val authToken = getAuthToken(spark, catalog)

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
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $authToken")

      val jsonPayload = JsonUtils.toJson(request)
      httpPost.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))

      val response = httpClient.execute(httpPost)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode < 200 || statusCode >= 300) {
          val responseBody = Option(response.getEntity)
            .map(EntityUtils.toString)
            .getOrElse("<no response body>")
          throw new RuntimeException(
            s"UC metrics endpoint returned error status $statusCode: $responseBody")
        }
      } finally {
        response.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def getEndpointUrl(spark: SparkSession, catalogName: String): String = {
    val uriKey = s"$CATALOG_CONF_PREFIX.$catalogName.$CATALOG_URI_CONF_SUFFIX"
    spark.conf.getOption(uriKey) match {
      case Some(uri) if uri.nonEmpty =>
        s"${uri.stripSuffix("/")}$METRICS_ENDPOINT_PATH"
      case _ =>
        throw new IllegalArgumentException(
          s"UC catalog base URI not configured. Set $uriKey")
    }
  }

  private def getAuthToken(spark: SparkSession, catalogName: String): String = {
    val configMap = UCCommitCoordinatorBuilder.getCatalogConfigMap(spark)
    val config = configMap.get(catalogName).getOrElse(
      throw new IllegalArgumentException(
        s"Unity Catalog configuration not found for catalog '$catalogName'. " +
        "Configure spark.sql.catalog.<catalog>.uri and auth " +
        "(auth.type/auth.token or legacy .token)."))
    val tokenProvider = TokenProvider.create(config.authConfig.asJava)
    tokenProvider.accessToken()
  }

}
