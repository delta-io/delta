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
import scala.util.Try

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.rest.requests.{PlanTableScanRequest, PlanTableScanRequestParser}
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse

/**
 * Case class for parsing Iceberg REST catalog /v1/config response.
 * Per the Iceberg REST spec, the config endpoint returns defaults and overrides.
 * The optional "prefix" in overrides is used for multi-tenant catalog paths.
 */
private case class CatalogConfigResponse(
    defaults: Map[String, String],
    overrides: Map[String, String])

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
 * @param baseUriRaw Base URI of the Iceberg REST catalog up to /v1, e.g.,
 *                   "http://<catalog-URL>/iceberg/v1". Trailing slashes are handled automatically.
 * @param catalogName Name of the catalog for config endpoint query parameter.
 * @param token Authentication token for the catalog server.
 */
class IcebergRESTCatalogPlanningClient(
    baseUriRaw: String,
    catalogName: String,
    token: String) extends ServerSidePlanningClient with AutoCloseable {

  // Normalize baseUri to handle trailing slashes
  private val baseUri = baseUriRaw.stripSuffix("/")

  // Sentinel value indicating "use current snapshot" in Iceberg REST API
  private val CURRENT_SNAPSHOT_ID = 0L

  // Partition spec ID for unpartitioned tables
  private val UNPARTITIONED_SPEC_ID = 0

  /**
   * Lazily fetch the catalog configuration and construct the endpoint URI root.
   * Calls /v1/config?warehouse=<catalogName> per Iceberg REST catalog spec to get the prefix.
   * If no prefix is returned, uses baseUri directly without any prefix per Iceberg spec.
   */
  private lazy val icebergRestCatalogUriRoot: String = {
    fetchCatalogPrefix() match {
      case Some(prefix) => s"$baseUri/$prefix"
      case None => baseUri
    }
  }

  /**
   * Fetch catalog prefix from /v1/config endpoint per Iceberg REST catalog spec.
   * Returns None on any error or if no prefix is defined in the config.
   */
  private def fetchCatalogPrefix(): Option[String] = {
    try {
      val configUri = s"$baseUri/config?warehouse=$catalogName"
      val httpGet = new HttpGet(configUri)
      val response = httpClient.execute(httpGet)
      try {
        if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
          val body = EntityUtils.toString(response.getEntity)
          val config = JsonUtils.fromJson[CatalogConfigResponse](body)
          // Apply overrides on top of defaults per Iceberg REST spec
          config.overrides.get("prefix").orElse(config.defaults.get("prefix"))
        } else {
          None
        }
      } finally {
        response.close()
      }
    } catch {
      case _: Exception => None
    }
  }

  private val httpHeaders = {
    val baseHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.USER_AGENT -> buildUserAgent()
    )
    // Add Bearer token authentication if token is provided
    val headersWithAuth = if (token.nonEmpty) {
      baseHeaders + (HttpHeaders.AUTHORIZATION -> s"Bearer $token")
    } else {
      baseHeaders
    }
    headersWithAuth.map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava
  }

  /**
   * Build User-Agent header with Delta, Spark, Java and Scala version information.
   * Format: "Delta/<version> Spark/<version> Java/<version> Scala/<version>"
   * Example: "Delta/4.0.0 Spark/3.5.0 Java/17.0.10 Scala/2.12.18"
   */
  private def buildUserAgent(): String = {
    val deltaVersion = getDeltaVersion().getOrElse("unknown")
    val sparkVersion = getSparkVersion().getOrElse("unknown")
    val javaVersion = getJavaVersion()
    val scalaVersion = getScalaVersion()
    s"Delta/$deltaVersion Spark/$sparkVersion Java/$javaVersion Scala/$scalaVersion"
  }

  /**
   * Get the User-Agent header value used by this client.
   * Format: "Delta/<version> Spark/<version> Java/<version> Scala/<version>"
   *
   * @return The User-Agent string used in HTTP requests
   */
  def getUserAgent(): String = {
    buildUserAgent()
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

  /**
   * Get Java version from system properties.
   */
  private def getJavaVersion(): String = {
    System.getProperty("java.version", "unknown")
  }

  /**
   * Get Scala version from the scala.util.Properties.versionNumberString property.
   */
  private def getScalaVersion(): String = {
    scala.util.Properties.versionNumberString
  }

  private lazy val httpClient = HttpClientBuilder.create()
    .setDefaultHeaders(httpHeaders)
    .setConnectionTimeToLive(30, java.util.concurrent.TimeUnit.SECONDS)
    .build()

  override def canConvertFilters(filters: Array[Filter]): Boolean = {
    // Check if all filters can be converted to Iceberg expressions
    // Returns true only if ALL filters successfully convert
    filters.forall { filter =>
      SparkToIcebergExpressionConverter.convert(filter).isDefined
    }
  }

  override def planScan(
      database: String,
      table: String,
      sparkFilterOption: Option[Filter] = None,
      sparkProjectionOption: Option[Seq[String]] = None,
      sparkLimitOption: Option[Int] = None): ScanPlan = {
    // Construct the /plan endpoint URI. For Unity Catalog tables, the
    // Call /v1/config to get the catalog prefix, then construct the full endpoint.
    // icebergRestCatalogUriRoot is lazily constructed as: {baseUri}/{prefix}
    // where prefix comes from /v1/config?warehouse=<catalogName> per Iceberg REST spec.
    // See: https://iceberg.apache.org/rest-catalog-spec/
    val planTableScanUri = s"$icebergRestCatalogUriRoot/namespaces/$database/tables/$table/plan"

    // Request planning for current snapshot. snapshotId = 0 means "use current snapshot"
    // in the Iceberg REST API spec. Time-travel queries are not yet supported.
    val builder = new PlanTableScanRequest.Builder()
      .withSnapshotId(CURRENT_SNAPSHOT_ID)
      // Set caseSensitive=false (defaults to true in spec) to match Spark's case-insensitive
      // column handling. Server should validate and block requests with caseSensitive=true.
      .withCaseSensitive(false)

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

    // Iceberg 1.11 adds withMinRowsRequested() support. For now, manually inject the field.
    val requestJson = sparkLimitOption match {
      case Some(limit) =>
        implicit val formats: Formats = DefaultFormats
        val jsonAst = parse(PlanTableScanRequestParser.toJson(request))
        val modifiedJson = jsonAst merge JObject("min-rows-requested" -> JLong(limit.toLong))
        compact(render(modifiedJson))
      case None =>
        PlanTableScanRequestParser.toJson(request)
    }
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
        // Parse response with caseSensitive=false to match request and Spark's case-insensitive
        // column handling
        val icebergResponse = parsePlanTableScanResponse(
          responseBody, unpartitionedSpecMap, caseSensitive = false)

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

  /**
   * Convert Iceberg PlanTableScanResponse to simple ScanPlan data class.
   *
   * Validates response structure and ensures the table is unpartitioned.
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

    val credentials = extractCredentials(responseBody)
    ScanPlan(files = files, credentials = credentials)
  }

  /**
   * Extract storage credentials from IRC server response.
   * Uses sealed trait pattern - tries each credential type in priority order.
   *
   * JSON structure:
   * {
   *   "storage-credentials": [{
   *     "config": {
   *       "s3.access-key-id": "...",
   *       "azure.account-name": "...",
   *       "gcs.oauth2.token": "...",
   *       ...
   *     }
   *   }]
   * }
   */
  /**
   * Extract storage credentials from response using sealed trait factory.
   * Returns None if no credentials section exists.
   * Throws IllegalStateException if credentials are incomplete or malformed.
   */
  private def extractCredentials(responseBody: String): Option[ScanPlanStorageCredentials] = {
    implicit val formats: Formats = DefaultFormats
    val json = parse(responseBody)

    // Extract config map from storage-credentials[0].config
    val config: Option[Map[String, String]] = try {
      (json \ "storage-credentials")(0) \ "config" match {
        case JNothing | JNull => None
        case c => Some(c.extract[Map[String, String]])
      }
    } catch {
      case _: Exception => None // No credentials section in response
    }

    // If config exists and is non-empty, use factory (throws on incomplete credentials)
    config.filter(_.nonEmpty).map(ScanPlanStorageCredentials.fromConfig)
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
