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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpRequest, HttpRequestInterceptor, HttpResponse, HttpStatus}
import org.apache.http.client.ServiceUnavailableRetryStrategy
import org.apache.http.impl.client.{DefaultHttpRequestRetryHandler, HttpClientBuilder}
import org.apache.http.protocol.{HttpContext, HttpCoreContext}
import org.apache.http.message.BasicHeader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.expressions.Expressions
import shadedForDelta.org.apache.iceberg.rest.{PlanStatus, RESTUtil}
import shadedForDelta.org.apache.iceberg.rest.requests.{PlanTableScanRequest, PlanTableScanRequestParser}
import shadedForDelta.org.apache.iceberg.rest.responses.{
  BaseScanTaskResponse,
  ErrorResponse,
  FetchPlanningResultResponse,
  PlanTableScanResponse}
import shadedForDelta.org.apache.iceberg.util.Tasks

/**
 * Case class for parsing Iceberg REST catalog /v1/config response.
 * Per the Iceberg REST spec, the config endpoint returns defaults and overrides.
 * The optional "prefix" in overrides is used for multi-tenant catalog paths.
 */
private case class CatalogConfigResponse(
    defaults: Map[String, String] = Map.empty,
    overrides: Map[String, String] = Map.empty,
    endpoints: Option[Seq[String]] = None)

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
 * @param tokenSupplier Supplier of auth tokens, called per-request to support OAuth.
 *                      Returns empty string if no auth is needed.
 * @param clientProperties Client-provided Iceberg REST catalog properties.
 */
class IcebergRESTCatalogPlanningClient(
    baseUriRaw: String,
    catalogName: String,
    tokenSupplier: () => String,
    clientProperties: Map[String, String] = Map.empty
) extends ServerSidePlanningClient with Logging {

  // Normalize baseUri to handle trailing slashes
  private val baseUri = baseUriRaw.stripSuffix("/")

  // Sentinel value indicating "use current snapshot" in Iceberg REST API
  private val CURRENT_SNAPSHOT_ID = 0L

  // Partition spec ID for unpartitioned tables
  private val UNPARTITIONED_SPEC_ID = 0

  // IRC config key mappings for each credential type
  private val S3_KEYS = Seq("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
  private val AZURE_SAS_TOKEN_KEY_PREFIX = "adls.sas-token."
  private val GCS_TOKEN_KEY = "gcs.oauth2.token"
  private val GCS_EXPIRY_KEY = "gcs.oauth2.token-expires-at"

  // Iceberg REST scan-planning poll properties and defaults.
  // NOTE: Of these, only `rest-scan-planning.poll-timeout-ms` is a documented config key in
  // released Iceberg 1.11.0 (RESTCatalogProperties). The poll-num-retries / poll-min-wait-ms /
  // poll-max-wait-ms / poll-scale-factor keys track the still-open Iceberg proposal #17846;
  // released 1.11.0 hard-codes retries and backoff inside RESTTableScan. We read them from
  // /v1/config so a server that adopts the proposal can tune them, and fall back to the
  // hard-coded defaults below otherwise.
  private val POLL_TIMEOUT_MS = "rest-scan-planning.poll-timeout-ms"
  private val POLL_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5)
  private val POLL_NUM_RETRIES = "rest-scan-planning.poll-num-retries"
  private val POLL_NUM_RETRIES_DEFAULT = 10
  private val POLL_MIN_WAIT_MS = "rest-scan-planning.poll-min-wait-ms"
  private val POLL_MIN_WAIT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(1)
  private val POLL_MAX_WAIT_MS = "rest-scan-planning.poll-max-wait-ms"
  private val POLL_MAX_WAIT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1)
  private val POLL_SCALE_FACTOR = "rest-scan-planning.poll-scale-factor"
  private val POLL_SCALE_FACTOR_DEFAULT = 2.0
  private val FETCH_PLANNING_RESULT_ENDPOINT =
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
  private val CANCEL_PLANNING_ENDPOINT =
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"

  // Credential vending header sent on POST /plan and GET /plan/{plan-id}, matching Iceberg REST
  // clients. Servers that only vend storage credentials when the client opts in rely on this
  // header being present; without it a completed plan can come back without storage-credentials.
  private val ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation"
  private val ACCESS_DELEGATION_VENDED_CREDENTIALS = "vended-credentials"

  private case class PollSettings(
      timeoutMs: Long,
      numRetries: Int,
      minWaitMs: Long,
      maxWaitMs: Long,
      scaleFactor: Double)

  private case class CompletedPlanningResult(
      response: FetchPlanningResultResponse,
      responseBody: String)

  private class NotCompleteException extends RuntimeException

  private case class S3Credentials(
      accessKeyId: String,
      secretAccessKey: String,
      sessionToken: String) extends ScanPlanStorageCredentials {
    override def configure(conf: Configuration): Unit = {
      conf.set("fs.s3a.path.style.access", "true")
      conf.set("fs.s3.impl.disable.cache", "true")
      conf.set("fs.s3a.impl.disable.cache", "true")
      conf.set("fs.s3a.access.key", accessKeyId)
      conf.set("fs.s3a.secret.key", secretAccessKey)
      conf.set("fs.s3a.session.token", sessionToken)
    }
  }

  private case class AzureCredentials(
      accountName: String,
      sasToken: String) extends ScanPlanStorageCredentials {
    override def configure(conf: Configuration): Unit = {
      val accountSuffix = s"$accountName.dfs.core.windows.net"
      conf.set("fs.abfs.impl.disable.cache", "true")
      conf.set("fs.abfss.impl.disable.cache", "true")
      conf.set(s"fs.azure.account.auth.type.$accountSuffix", "SAS")
      conf.set(s"fs.azure.sas.fixed.token.$accountSuffix", sasToken)
    }
  }

  private case class GcsCredentials(
      oauth2Token: String,
      expirationEpochMs: Option[Long] = None) extends ScanPlanStorageCredentials {
    override def configure(conf: Configuration): Unit = {
      conf.set("fs.gs.impl.disable.cache", "true")
      conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
      conf.set("fs.gs.auth.access.token.provider",
        classOf[FixedGcsAccessTokenProvider].getName)
      conf.set("fs.gs.auth.access.token", oauth2Token)
      expirationEpochMs.foreach { ms =>
        conf.set("fs.gs.auth.access.token.expiration.ms", ms.toString)
      }
    }
  }

  private def hasAzureKeys(config: Map[String, String]): Boolean =
    config.keys.exists(_.startsWith(AZURE_SAS_TOKEN_KEY_PREFIX))

  private def buildAzureCredentials(config: Map[String, String]): AzureCredentials = {
    val sasTokenKey = config.keys
      .find(_.startsWith(AZURE_SAS_TOKEN_KEY_PREFIX))
      .getOrElse(throw new IllegalStateException(
        s"Missing Azure SAS token key starting with: $AZURE_SAS_TOKEN_KEY_PREFIX"))

    val accountName = sasTokenKey
      .stripPrefix(AZURE_SAS_TOKEN_KEY_PREFIX)
      .stripSuffix(".dfs.core.windows.net")

    val sasToken = config(sasTokenKey)

    AzureCredentials(accountName = accountName, sasToken = sasToken)
  }

  private def fromConfig(config: Map[String, String]): ScanPlanStorageCredentials = {
    def get(key: String): String =
      config.getOrElse(key, throw new IllegalStateException(s"Missing required credential: $key"))

    def hasAny(keys: Seq[String]): Boolean = keys.exists(config.contains)

    if (hasAny(S3_KEYS)) {
      S3Credentials(
        get("s3.access-key-id"),
        get("s3.secret-access-key"),
        get("s3.session-token"))
    } else if (hasAzureKeys(config)) {
      buildAzureCredentials(config)
    } else if (config.contains(GCS_TOKEN_KEY)) {
      val token = get(GCS_TOKEN_KEY)
      val expirationEpochMs = config.get(GCS_EXPIRY_KEY)
        .flatMap(s => scala.util.Try(s.toLong).toOption)
      GcsCredentials(token, expirationEpochMs)
    } else {
      throw new IllegalStateException(
        "Unrecognized credential keys. " +
          "Expected S3 (s3.*), Azure (adls.*), or GCS (gcs.*) properties.")
    }
  }

  /** Fetch the server's catalog configuration once. */
  private lazy val catalogConfig: CatalogConfigResponse = {
    val configUri = s"$baseUri/config?warehouse=$catalogName"
    try {
      val httpGet = new HttpGet(configUri)
      val response = httpClient.execute(httpGet)
      try {
        if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
          val body = EntityUtils.toString(response.getEntity)
          JsonUtils.fromJson[CatalogConfigResponse](body)
        } else {
          CatalogConfigResponse(Map.empty, Map.empty)
        }
      } finally {
        response.close()
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to fetch catalog config from $configUri. " +
          s"Falling back to defaults. Error: ${e.getMessage}")
        CatalogConfigResponse(Map.empty, Map.empty)
    }
  }

  /**
   * Merge catalog properties using Iceberg REST precedence:
   * server overrides > client properties > server defaults > client defaults.
   *
   * Client defaults are applied by the propertyAs* helpers when a key is absent from this map.
   */
  private lazy val catalogProperties: Map[String, String] =
    Option(catalogConfig.defaults).getOrElse(Map.empty) ++
      Option(clientProperties).getOrElse(Map.empty) ++
      Option(catalogConfig.overrides).getOrElse(Map.empty)

  /**
   * Lazily construct the endpoint URI root from the Iceberg REST catalog config prefix.
   * If no prefix is returned, use baseUri directly.
   */
  private lazy val icebergRestCatalogUriRoot: String =
    catalogProperties.get("prefix").map(prefix => s"$baseUri/$prefix").getOrElse(baseUri)

  private lazy val pollSettings: PollSettings = {
    val settings = PollSettings(
      timeoutMs = propertyAsLong(POLL_TIMEOUT_MS, POLL_TIMEOUT_MS_DEFAULT),
      numRetries = propertyAsInt(POLL_NUM_RETRIES, POLL_NUM_RETRIES_DEFAULT),
      minWaitMs = propertyAsLong(POLL_MIN_WAIT_MS, POLL_MIN_WAIT_MS_DEFAULT),
      maxWaitMs = propertyAsLong(POLL_MAX_WAIT_MS, POLL_MAX_WAIT_MS_DEFAULT),
      scaleFactor = propertyAsDouble(POLL_SCALE_FACTOR, POLL_SCALE_FACTOR_DEFAULT))

    require(settings.timeoutMs > 0, s"$POLL_TIMEOUT_MS must be positive")
    require(settings.numRetries >= 0, s"$POLL_NUM_RETRIES must be non-negative")
    require(settings.minWaitMs > 0, s"$POLL_MIN_WAIT_MS must be positive")
    require(settings.maxWaitMs >= settings.minWaitMs,
      s"$POLL_MAX_WAIT_MS must be greater than or equal to $POLL_MIN_WAIT_MS")
    require(settings.scaleFactor >= 1.0, s"$POLL_SCALE_FACTOR must be at least 1.0")
    settings
  }

  private def propertyAsLong(key: String, default: Long): Long =
    catalogProperties.get(key).map(value => parseProperty(key, value)(_.toLong)).getOrElse(default)

  private def propertyAsInt(key: String, default: Int): Int =
    catalogProperties.get(key).map(value => parseProperty(key, value)(_.toInt)).getOrElse(default)

  private def propertyAsDouble(key: String, default: Double): Double =
    catalogProperties.get(key)
      .map(value => parseProperty(key, value)(_.toDouble))
      .getOrElse(default)

  private def parseProperty[T](key: String, value: String)(parse: String => T): T = {
    Try(parse(value)).getOrElse(
      throw new IllegalArgumentException(s"Invalid value for $key: '$value'"))
  }

  // A missing endpoints list is treated as the Iceberg default endpoint set, which does not
  // include fetch planning, so an absent list is reported as unsupported (same as Iceberg's
  // Endpoint.check refusing a submitted plan it cannot follow up on).
  private def supportsFetchPlanningResult: Boolean =
    catalogConfig.endpoints.exists(_.contains(FETCH_PLANNING_RESULT_ENDPOINT))

  private def supportsCancelPlanning: Boolean =
    catalogConfig.endpoints.exists(_.contains(CANCEL_PLANNING_ENDPOINT))

  // Default headers without auth -- auth is injected per-request via HttpRequestInterceptor
  private val httpHeaders = {
    Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.USER_AGENT -> buildUserAgent()
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava
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

  // Maximum number of retries for transient HTTP failures (IOException, 5xx server errors)
  private val HTTP_MAX_RETRIES = 3

  private lazy val httpClient = {
    val builder = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .setConnectionTimeToLive(30, java.util.concurrent.TimeUnit.SECONDS)
      // Do not retry requests that may have reached the server. Retrying POST /plan after an
      // ambiguous transport failure could allocate a duplicate server-side plan.
      .setRetryHandler(new DefaultHttpRequestRetryHandler(HTTP_MAX_RETRIES, false))
      .setServiceUnavailableRetryStrategy(new ServerErrorRetryStrategy(HTTP_MAX_RETRIES))

    // Per-request interceptor: calls tokenSupplier() to get the current token.
    // The token provider implementation handles caching as needed.
    builder.addInterceptorFirst(new HttpRequestInterceptor {
      override def process(request: HttpRequest, context: HttpContext): Unit = {
        val token = tokenSupplier()
        if (token != null && token.nonEmpty) {
          request.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $token")
        }
      }
    })

    builder.build()
  }

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
    // Validate poll settings before POST /plan. Otherwise a malformed rest-scan-planning.poll-*
    // value from /v1/config would only throw after the server had already allocated a plan-id.
    val _ = pollSettings

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
    httpPost.setHeader(ACCESS_DELEGATION_HEADER, ACCESS_DELEGATION_VENDED_CREDENTIALS)
    httpPost.setEntity(new StringEntity(requestJson, ContentType.APPLICATION_JSON))
    val httpResponse = httpClient.execute(httpPost)
    val (statusCode, responseBody) = try {
      httpResponse.getStatusLine.getStatusCode -> EntityUtils.toString(httpResponse.getEntity)
    } finally {
      httpResponse.close()
    }

    // Only unpartitioned tables are supported. This map is used when parsing the response
    // to resolve partition specs. The validation that the table is actually unpartitioned
    // happens later in convertToScanPlan when we check file.partition().size().
    val unpartitionedSpecMap = Map(UNPARTITIONED_SPEC_ID -> PartitionSpec.unpartitioned())

    if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
      // Parse response with caseSensitive=false to match request and Spark's case-insensitive
      // column handling
      val icebergResponse = parsePlanTableScanResponse(
        responseBody, unpartitionedSpecMap, caseSensitive = false)

      icebergResponse.planStatus() match {
        case PlanStatus.COMPLETED =>
          convertToScanPlan(icebergResponse, responseBody)
        case PlanStatus.SUBMITTED =>
          val planId = Option(icebergResponse.planId())
            .filter(_.nonEmpty)
            .getOrElse(throw new IllegalStateException(
              s"Submitted scan plan response did not contain a plan ID. Table: $database.$table"))
          val encodedPlanId = RESTUtil.encodeString(planId)
          val planUri =
            s"$icebergRestCatalogUriRoot/namespaces/$database/tables/$table/plan/$encodedPlanId"
          if (!supportsFetchPlanningResult) {
            // The POST already allocated a server-side plan; best-effort cancel it (if the server
            // advertises cancellation) before failing, so we do not leak a submitted plan.
            cancelPlanning(planUri, planId)
            throw new UnsupportedOperationException(
              s"Server does not support endpoint: $FETCH_PLANNING_RESULT_ENDPOINT")
          }
          fetchPlanningResult(database, table, planId, planUri, unpartitionedSpecMap)
        case PlanStatus.FAILED =>
          throw planningFailure(database, table, Option(icebergResponse.planId()),
            icebergResponse.errorResponse())
        case status =>
          throw new IllegalStateException(
            s"Unexpected scan plan status '$status'. Table: $database.$table")
      }
    } else {
      // TODO: Parse structured ErrorResponse JSON from Iceberg REST spec instead of raw body
      throw new IOException(
        s"Failed to plan table scan for $database.$table. " +
        s"HTTP status: $statusCode, Response: $responseBody")
    }
  }

  private def fetchPlanningResult(
      database: String,
      table: String,
      planId: String,
      fetchUri: String,
      specsById: Map[Int, PartitionSpec]): ScanPlan = {
    val result = new AtomicReference[CompletedPlanningResult]()
    // Set when the server reports a terminal status (FAILED / CANCELLED). Those plan-ids are
    // already done server-side, so we must not send a cancel for them.
    val serverTerminal = new AtomicBoolean(false)

    try {
      val settings = pollSettings
      // Poll GET /plan/{plan-id} only. Plan-task pagination (GET /tasks) is not supported.
      val pollPlan: Tasks.Task[String, Exception] = (_: String) => {
        val httpGet = new HttpGet(fetchUri)
        httpGet.setHeader(ACCESS_DELEGATION_HEADER, ACCESS_DELEGATION_VENDED_CREDENTIALS)
        val httpResponse = httpClient.execute(httpGet)
        val (statusCode, responseBody) = try {
          httpResponse.getStatusLine.getStatusCode -> EntityUtils.toString(httpResponse.getEntity)
        } finally {
          httpResponse.close()
        }

        if (statusCode != HttpStatus.SC_OK) {
          throw new IOException(
            s"Failed to fetch scan plan $planId for $database.$table. " +
              s"HTTP status: $statusCode, Response: $responseBody")
        }

        val response =
          parseFetchPlanningResultResponse(responseBody, specsById, caseSensitive = false)
        response.planStatus() match {
          case PlanStatus.COMPLETED =>
            result.set(CompletedPlanningResult(response, responseBody))
          case PlanStatus.SUBMITTED =>
            throw new NotCompleteException
          case PlanStatus.FAILED =>
            serverTerminal.set(true)
            throw planningFailure(database, table, Some(planId), response.errorResponse())
          case PlanStatus.CANCELLED =>
            serverTerminal.set(true)
            throw new IllegalStateException(
              s"Scan plan $planId for $database.$table was cancelled")
          case status =>
            throw new IllegalStateException(
              s"Unexpected scan plan status '$status' for plan $planId. Table: $database.$table")
        }
      }

      try {
        Tasks.foreach(planId)
          .exponentialBackoff(
            settings.minWaitMs,
            settings.maxWaitMs,
            settings.timeoutMs,
            settings.scaleFactor)
          .retry(settings.numRetries)
          .onlyRetryOn(classOf[NotCompleteException])
          .throwFailureWhenFinished()
          .run(pollPlan, classOf[Exception])
      } catch {
        case e: NotCompleteException =>
          throw new IOException(
            s"Scan plan $planId for $database.$table did not complete within configured " +
              s"poll limits (timeout=${settings.timeoutMs} ms, " +
              s"numRetries=${settings.numRetries})",
            e)
      }
    } finally {
      // Best-effort cancel whenever we leave the poll loop without a completed result and the
      // server has not already reported a terminal status. This covers timeout / retry
      // exhaustion, malformed poll config, and mid-poll transport or unexpected-status
      // failures, matching Iceberg's RESTTableScan which cancels from onFailure.
      // Cancellation is a no-op unless the server advertises the cancel endpoint.
      if (result.get() == null && !serverTerminal.get()) {
        cancelPlanning(fetchUri, planId)
      }
    }

    val completed = result.get()
    convertToScanPlan(completed.response, completed.responseBody)
  }

  private def cancelPlanning(planUri: String, planId: String): Unit = {
    if (supportsCancelPlanning) {
      try {
        val response = httpClient.execute(new HttpDelete(planUri))
        try {
          val statusCode = response.getStatusLine.getStatusCode
          if (statusCode != HttpStatus.SC_NO_CONTENT && statusCode != HttpStatus.SC_NOT_FOUND) {
            logWarning(
              s"Failed to cancel scan plan $planId. HTTP status: $statusCode")
          }
        } finally {
          response.close()
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to cancel scan plan $planId", e)
      }
    }
  }

  private def planningFailure(
      database: String,
      table: String,
      planId: Option[String],
      error: ErrorResponse): IllegalStateException = {
    val planDescription = planId.map(id => s" $id").getOrElse("")
    val errorDescription = Option(error)
      .map(value => s"${value.`type`()} (code=${value.code()}): ${value.message()}")
      .getOrElse("unknown error")
    new IllegalStateException(
      s"Scan plan$planDescription for $database.$table failed: $errorDescription")
  }

  /**
   * Convert an Iceberg scan task response to a simple ScanPlan data class.
   *
   * Validates response structure and ensures the table is unpartitioned.
   */
  private def convertToScanPlan(
      response: BaseScanTaskResponse,
      responseBody: String): ScanPlan = {
    require(response != null, "Scan task response cannot be null")
    if (response.planTasks() != null && !response.planTasks().isEmpty) {
      throw new UnsupportedOperationException(
        "Plan tasks are not supported; completed scan plans must contain inline file scan tasks")
    }
    require(response.fileScanTasks() != null, "File scan tasks cannot be null")

    val files = response.fileScanTasks().asScala.map { task =>
      require(task != null, "FileScanTask cannot be null")
      require(task.file() != null, "DataFile cannot be null")
      if (task.deletes() != null && !task.deletes().isEmpty) {
        throw new UnsupportedOperationException(
          "Delete files are not supported; file scan tasks must not reference delete files")
      }

      // Validate that the server does not expect the application of a residual. The application of
      // a residual filter is currently not supported, and its ignorance leads to wrong results.
      val residual = task.residual()
      if (residual != null && !residual.isEquivalentTo(Expressions.alwaysTrue)) {
        throw new UnsupportedOperationException(
          s"Found FileScanTask with residual: ${residual}. " +
            s"Only FileScanTasks with no or alwaysTrue residual are currently supported.")
      }

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

    // Iceberg's spec returns storage-credentials as a list keyed by path prefix and picks the
    // credential whose prefix is the longest match for a given file. ScanPlan currently carries a
    // single credential set applied to every file (unpartitioned, single-location tables), so it
    // cannot represent per-prefix selection. Rather than silently pick the first entry and risk
    // reading files with the wrong credentials, fail loud when the server returns more than one.
    val credentialEntries = json \ "storage-credentials" match {
      case JArray(entries) => entries
      case _ => Nil
    }
    if (credentialEntries.size > 1) {
      throw new UnsupportedOperationException(
        s"Multiple storage-credentials entries (${credentialEntries.size}) are not supported; " +
          "only a single credential set applied to all files is currently handled")
    }

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
    config.filter(_.nonEmpty).map(fromConfig)
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

  /**
   * Retry strategy for server errors (5xx status codes) with exponential backoff.
   * Retries up to maxRetries times with doubling intervals (1s, 2s, 4s, ...).
   * Does NOT retry on client errors (4xx) since those indicate request-level issues.
   *
   * POST /plan is intentionally excluded: a 5xx after the server has already allocated a plan-id
   * is ambiguous, and retrying would leak a second server-side plan (same hazard as
   * requestSentRetryEnabled). We only retry idempotent methods (GET poll, DELETE cancel).
   *
   * The ServiceUnavailableRetryStrategy interface calls retryRequest() first, then
   * getRetryInterval(), so we capture the execution count in retryRequest() and
   * use it to compute the backoff in getRetryInterval().
   */
  private class ServerErrorRetryStrategy(maxRetries: Int)
      extends ServiceUnavailableRetryStrategy {

    // ThreadLocal so concurrent planScan calls each track their own retry attempt.
    // The HTTP client is shared and thread-safe (see class doc), so multiple threads
    // can be retrying independently through the same strategy instance.
    private val lastExecutionCount = new ThreadLocal[Int] {
      override def initialValue(): Int = 1
    }

    override def retryRequest(
        response: HttpResponse,
        executionCount: Int,
        context: HttpContext): Boolean = {
      lastExecutionCount.set(executionCount)
      val method = Option(context.getAttribute(HttpCoreContext.HTTP_REQUEST))
        .collect { case r: HttpRequest => r.getRequestLine.getMethod }
        .getOrElse("")
      val statusCode = response.getStatusLine.getStatusCode
      // Never retry POST: a retried submit can allocate a duplicate server-side plan.
      !method.equalsIgnoreCase("POST") && statusCode >= 500 && executionCount <= maxRetries
    }

    // Exponential backoff: 1s, 2s, 4s, ...
    override def getRetryInterval: Long =
      java.util.concurrent.TimeUnit.SECONDS.toMillis(1L << (lastExecutionCount.get() - 1))
  }

  private def parsePlanTableScanResponse(
    json: String,
    specsById: Map[Int, PartitionSpec],
    caseSensitive: Boolean): PlanTableScanResponse = {
    parseScanResponse(
      "shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponseParser",
      json,
      specsById,
      caseSensitive).asInstanceOf[PlanTableScanResponse]
  }

  private def parseFetchPlanningResultResponse(
      json: String,
      specsById: Map[Int, PartitionSpec],
      caseSensitive: Boolean): FetchPlanningResultResponse = {
    parseScanResponse(
      "shadedForDelta.org.apache.iceberg.rest.responses.FetchPlanningResultResponseParser",
      json,
      specsById,
      caseSensitive).asInstanceOf[FetchPlanningResultResponse]
  }

  /**
   * Use reflection to access the package-private String-based fromJson methods in Iceberg's
   * response parsers. These methods are not part of Iceberg's public API.
   */
  private def parseScanResponse(
      parserClassName: String,
      json: String,
      specsById: Map[Int, PartitionSpec],
      caseSensitive: Boolean): AnyRef = {
    val parserClass = Utils.classForName(parserClassName)
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
    )
  }
}
