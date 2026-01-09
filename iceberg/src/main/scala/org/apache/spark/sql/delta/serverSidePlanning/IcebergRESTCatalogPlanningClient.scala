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

package org.apache.spark.sql.delta.serverSidePlanning

import java.io.IOException
import java.lang.reflect.Method
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
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

  private val httpHeaders = {
    val baseHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.USER_AGENT -> buildUserAgent()
    )
    // Add Bearer token authentication if token is provided
    val headersWithAuth = if (token != null && token.nonEmpty) {
      baseHeaders + (HttpHeaders.AUTHORIZATION -> s"Bearer $token")
    } else {
      baseHeaders
    }
    headersWithAuth.map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava
  }

  /**
   * Build User-Agent header with Delta and Spark version information.
   * Format: "Delta-Lake/<version> Apache-Spark/<version>"
   */
  private def buildUserAgent(): String = {
    val deltaVersion = getDeltaVersion().getOrElse("unknown")
    val sparkVersion = getSparkVersion().getOrElse("unknown")
    s"Delta-Lake/$deltaVersion Apache-Spark/$sparkVersion"
  }

  /**
   * Get Spark version. Returns None if Spark version cannot be determined.
   */
  private def getSparkVersion(): Option[String] = {
    try {
      val packageClass = Utils.classForName("org.apache.spark.package$")
      val moduleField = packageClass.getField("MODULE$")
      val moduleObj = moduleField.get(null)
      val versionObj = packageClass.getMethod("SPARK_VERSION").invoke(moduleObj)
      if (versionObj != null) {
        Some(versionObj.toString)
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Get Delta version. Returns None if Delta is not available or version cannot be determined.
   */
  private def getDeltaVersion(): Option[String] = {
    // Try io.delta.Version.getVersion() first (preferred method)
    try {
      val versionClass = Utils.classForName("io.delta.Version")
      val versionObj = versionClass.getMethod("getVersion").invoke(null)
      if (versionObj != null) {
        return Some(versionObj.toString)
      }
    } catch {
      case _: Exception => // Fall through to fallback
    }

    // Fall back to io.delta.VERSION constant
    try {
      val packageClass = Utils.classForName("io.delta.package$")
      val moduleField = packageClass.getField("MODULE$")
      val moduleObj = moduleField.get(null)
      val versionObj = packageClass.getMethod("VERSION").invoke(moduleObj)
      if (versionObj != null) {
        return Some(versionObj.toString)
      }
    } catch {
      case _: Exception => // Delta not available or version not accessible
    }

    None
  }

  private lazy val httpClient = HttpClientBuilder.create()
    .setDefaultHeaders(httpHeaders)
    .setConnectionTimeToLive(30, java.util.concurrent.TimeUnit.SECONDS)
    .build()

  override def planScan(
      database: String,
      table: String,
      sparkFilterOption: Option[Filter] = None,
      sparkProjectionOption: Option[Seq[String]] = None): ScanPlan = {
    // Construct the /plan endpoint URI. For Unity Catalog tables, the
    // icebergRestCatalogUriRoot is constructed by UnityCatalogMetadata which calls
    // /v1/config to get the optional prefix and builds the proper endpoint
    // (e.g., {ucUri}/api/2.1/unity-catalog/iceberg-rest/v1/{prefix}).
    // For other catalogs, the endpoint is passed directly via metadata.
    // See: https://iceberg.apache.org/rest-catalog-spec/
    val planTableScanUri =
      s"$icebergRestCatalogUriRoot/v1/namespaces/$database/tables/$table/plan"

    // Request planning for current snapshot. snapshotId = 0 means "use current snapshot"
    // in the Iceberg REST API spec. Time-travel queries are not yet supported.
    val builder = new PlanTableScanRequest.Builder().withSnapshotId(CURRENT_SNAPSHOT_ID)

    // Convert Spark Filter to Iceberg Expression and add to request if filter is present.
    sparkFilterOption.foreach { sparkFilter =>
      SparkToIcebergExpressionConverter.convert(sparkFilter).foreach { icebergExpr =>
        builder.withFilter(icebergExpr)
      }
    }

    // Add projection to request if present.
    sparkProjectionOption.foreach { columnNames =>
      builder.withSelect(columnNames.asJava)
    }

    val request = builder.build()

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

        convertToScanPlan(icebergResponse)
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

  /**
   * Convert Iceberg PlanTableScanResponse to simple ScanPlan data class.
   *
   * Validates response structure and ensures the table is unpartitioned.
   */
  private def convertToScanPlan(response: PlanTableScanResponse): ScanPlan = {
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

    ScanPlan(files = files)
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
    val parserClass = Utils.classForName(
      "shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponseParser")

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
