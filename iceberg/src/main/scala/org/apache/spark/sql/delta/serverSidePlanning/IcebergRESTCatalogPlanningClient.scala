/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.serverSidePlanning

import java.io.IOException
import java.lang.reflect.Method
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.rest.requests.{PlanTableScanRequest, PlanTableScanRequestParser}
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse

/**
 * Iceberg REST implementation of ServerSidePlanningClient that calls Iceberg REST catalog server.
 *
 * This implementation calls the Iceberg REST catalog's `/plan` endpoint to perform server-side
 * scan planning. The server returns the list of data files to read, which eliminates the need
 * for client-side listing operations.
 *
 * Thread safety: This class creates a shared HTTP client that is thread-safe for concurrent
 * requests. The HTTP client should be explicitly closed by calling close() when done.
 *
 * @param icebergRestCatalogUriRoot Base URI of the Iceberg REST catalog server, e.g.,
 *                                   "http://localhost:8181". Should not include trailing slash
 *                                   or "/v1" prefix.
 * @param token Authentication token for the catalog server.
 */
class IcebergRESTCatalogPlanningClient(
    icebergRestCatalogUriRoot: String,
    token: String) extends ServerSidePlanningClient with AutoCloseable {

  // Sentinel value indicating "use current snapshot" in Iceberg REST API
  private val CURRENT_SNAPSHOT_ID = 0L

  // Partition spec ID for unpartitioned tables
  private val UNPARTITIONED_SPEC_ID = 0

  private val httpHeaders = Map(
    // TODO: Authentication not yet implemented. Uncomment when ready to add Bearer token auth.
    // HttpHeaders.AUTHORIZATION -> s"Bearer $token",
    HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
    HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
  ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

  private lazy val httpClient = HttpClientBuilder.create()
    .setDefaultHeaders(httpHeaders)
    .setConnectionTimeToLive(30, java.util.concurrent.TimeUnit.SECONDS)
    .build()

  override def planScan(database: String, table: String): ScanPlan = {
    // TODO: Follow Iceberg REST catalog spec for proper path construction. Per the spec, clients
    // should first call GET /v1/config to retrieve catalog configuration including the optional
    // "prefix" parameter in the overrides section (e.g., overrides.prefix). This prefix should
    // then be used to construct all subsequent API paths as /v1/{prefix}/namespaces/... instead
    // of hardcoding /v1/namespaces/... This allows catalogs to support multi-tenant hierarchies
    // (e.g., AWS Glue uses /catalogs/{catalog}, S3 Tables uses table bucket ARNs).
    // See: https://iceberg.apache.org/rest-catalog-spec/
    val planTableScanUri =
      s"$icebergRestCatalogUriRoot/v1/namespaces/$database/tables/$table/plan"

    // Request planning for current snapshot. snapshotId = 0 means "use current snapshot"
    // in the Iceberg REST API spec. Time-travel queries are not yet supported.
    val request = new PlanTableScanRequest.Builder().withSnapshotId(CURRENT_SNAPSHOT_ID).build()

    val requestJson = PlanTableScanRequestParser.toJson(request)
    val httpPost = new HttpPost(planTableScanUri)
    httpPost.setEntity(new StringEntity(requestJson, ContentType.APPLICATION_JSON))
    // TODO: Add retry logic for transient HTTP failures (e.g., connection timeouts, 5xx errors)
    val httpResponse = httpClient.execute(httpPost)

    // Only unpartitioned tables are supported. This map is used when parsing the response
    // to resolve partition specs. The validation that the table is actually unpartitioned
    // happens later in convertToScanPlan when we check file.partition().size().
    val unpartitionedSpecMap = Map(UNPARTITIONED_SPEC_ID -> PartitionSpec.unpartitioned())

    try {
      val statusCode = httpResponse.getStatusLine.getStatusCode
      val responseBody = EntityUtils.toString(httpResponse.getEntity)
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        // Parse response. caseSensitive = true because Iceberg is case-sensitive by default.
        val icebergResponse = parsePlanTableScanResponse(
          responseBody, unpartitionedSpecMap, caseSensitive = true)

        // Verify plan status is "completed". The Iceberg REST spec allows async planning
        // where the server returns "submitted" status and the client must poll for results.
        // We don't support async planning yet, so we require "completed" status.
        val planStatus = icebergResponse.planStatus()
        if (planStatus != null && planStatus.toString.toLowerCase(Locale.ROOT) != "completed") {
          throw new UnsupportedOperationException(
            s"Async planning not supported. Plan status was '$planStatus' but " +
            s"expected 'completed'. Table: $database.$table")
        }

        convertToScanPlan(icebergResponse, responseBody)
      } else {
        // TODO: Parse structured ErrorResponse JSON from Iceberg REST spec instead of raw body
        throw new IOException(
          s"Failed to plan table scan for $database.$table. " +
          s"HTTP status: $statusCode, Response: $responseBody")
      }
    } finally {
      httpResponse.close()
    }
  }

  // TODO: PlanTableScanResponse in Iceberg REST spec does not include storage-credentials yet.
  // Server sends PlanTableScanResponse and injects credentials at the servlet level.
  /**
   * Convert Iceberg PlanTableScanResponse to simple ScanPlan data class.
   *
   * Validates response structure and ensures the table is unpartitioned.
   *
   * @param response Parsed Iceberg response containing file scan tasks
   * @param responseBody Raw JSON response body for extracting credentials
   */
  private def convertToScanPlan(
      response: PlanTableScanResponse,
      responseBody: String): ScanPlan = {
    require(response != null, "PlanTableScanResponse cannot be null")
    require(response.fileScanTasks() != null, "File scan tasks cannot be null")

    val files = response.fileScanTasks().asScala.map { task =>
      require(task != null, "FileScanTask cannot be null")
      require(task.file() != null, "DataFile cannot be null")
      val file = task.file()

      // Validate that table is unpartitioned. Partitioned tables are not supported yet.
      if (file.partition().size() > 0) {
        throw new UnsupportedOperationException(
          s"Table has partition data: ${file.partition()}. " +
          s"Only unpartitioned tables (spec ID $UNPARTITIONED_SPEC_ID) are currently supported.")
      }

      ScanFile(
        filePath = file.path().toString,
        fileSizeInBytes = file.fileSizeInBytes(),
        fileFormat = file.format().toString.toLowerCase(Locale.ROOT)
      )
    }.toSeq

    // Extract credentials from raw JSON response
    // The credentials are in storage-credentials[0].config as per UC Iceberg REST spec
    val credentials = extractCredentials(responseBody)

    ScanPlan(files = files, credentials = credentials)
  }

  /**
   * Extract storage credentials from the raw JSON response.
   * Returns None if credentials are not present in the response.
   *
   * The credentials are located at: storage-credentials[0].config with keys:
   * - s3.access-key-id
   * - s3.secret-access-key
   * - s3.session-token
   */
  private def extractCredentials(responseBody: String): Option[StorageCredentials] = {
    Try {
      val mapper = new ObjectMapper()
      val jsonNode = mapper.readTree(responseBody)

      // Get storage-credentials array
      val storageCredsNode = Option(jsonNode.get("storage-credentials"))
        .filter(node => node.isArray && node.size() > 0)
        .getOrElse(return None)

      // Get config node from first credential
      val configNode = Option(storageCredsNode.get(0).get("config"))
        .getOrElse(return None)

      // Extract all three required credential fields
      val accessKey = Option(configNode.get("s3.access-key-id")).map(_.asText())
      val secretKey = Option(configNode.get("s3.secret-access-key")).map(_.asText())
      val sessionToken = Option(configNode.get("s3.session-token")).map(_.asText())

      // Return credentials only if all three fields are present
      for {
        access <- accessKey
        secret <- secretKey
        session <- sessionToken
      } yield StorageCredentials(access, secret, session)

    }.toOption.flatten
  }

  /**
   * Close the HTTP client and release resources.
   *
   * This should be called when the client is no longer needed to prevent resource leaks.
   * After calling close(), this client instance should not be used for further requests.
   */
  override def close(): Unit = {
    if (httpClient != null) {
      httpClient.close()
    }
  }

  private def parsePlanTableScanResponse(
    json: String,
    specsById: Map[Int, PartitionSpec],
    caseSensitive: Boolean): PlanTableScanResponse = {

    // Use reflection to access the private fromJson method in the Iceberg parser class.
    // The method is not part of the public API, so we need reflection and setAccessible.
    // scalastyle:off classforname
    val parserClass = Class.forName(
      "shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponseParser")
    // scalastyle:on classforname

    val fromJsonMethod: Method = parserClass.getDeclaredMethod(
      "fromJson",
      classOf[String],
      classOf[java.util.Map[_, _]],
      classOf[Boolean])

    fromJsonMethod.setAccessible(true)

    fromJsonMethod.invoke(
      null,  // static method
      json,
      specsById.map { case (k, v) => Int.box(k) -> v }.asJava,
      Boolean.box(caseSensitive)
    ).asInstanceOf[PlanTableScanResponse]
  }
}
