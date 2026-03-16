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

package org.apache.spark.sql.delta.serverSidePlanning

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import io.unitycatalog.client.auth.TokenProvider

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.catalog._

/**
 * Tests that the IcebergRESTCatalogPlanningClient calls the tokenSupplier per-request,
 * enabling OAuth token refresh between requests.
 *
 * Includes e2e tests that exercise the real OAuthTokenProvider against a mock OAuth
 * token endpoint using JDK's HttpServer (zero external dependencies).
 */
class TokenRefreshSuite extends QueryTest with SharedSparkSession {

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = TestSchemas.testSchema
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = IcebergRESTServerTestUtils.startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(
      s"spark.sql.catalog.rest_catalog",
      "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.type", "rest")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.uri", serverUri)

    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces]
        .createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException(
        "Catalog does not support namespaces")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.clearCaptured()
        server.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  // ---------------------------------------------------------------------------
  // Supplier-level tests (mock supplier, no real TokenProvider)
  // ---------------------------------------------------------------------------

  test("tokenSupplier is called per-request, not once at construction") {
    withTempTable("tokenRefreshTest") { table =>
      val callCount = new AtomicInteger(0)
      val supplier: () => String = () => {
        s"token-${callCount.incrementAndGet()}"
      }

      val client = new IcebergRESTCatalogPlanningClient(
        serverUri, "test_catalog", supplier)
      try {
        // First planScan: /v1/config GET + /plan POST
        client.planScan(
          defaultNamespace.toString, "tokenRefreshTest")
        val countAfterFirst = callCount.get()
        assert(countAfterFirst >= 2,
          s"tokenSupplier should be called at least twice " +
          s"(config + plan), but was called $countAfterFirst")

        // Second planScan: at least 1 more call for /plan
        client.planScan(
          defaultNamespace.toString, "tokenRefreshTest")
        val countAfterSecond = callCount.get()
        assert(countAfterSecond > countAfterFirst,
          s"tokenSupplier should be called again. " +
          s"After first: $countAfterFirst, " +
          s"after second: $countAfterSecond")
      } finally {
        client.close()
      }
    }
  }

  test("tokenSupplier uses latest value per-request") {
    withTempTable("dynamicTokenTest") { table =>
      val tokens =
        new java.util.concurrent.CopyOnWriteArrayList[String]()
      var currentToken = "initial-token"
      val supplier: () => String = () => {
        tokens.add(currentToken)
        currentToken
      }

      val client = new IcebergRESTCatalogPlanningClient(
        serverUri, "test_catalog", supplier)
      try {
        client.planScan(
          defaultNamespace.toString, "dynamicTokenTest")
        assert(tokens.contains("initial-token"),
          s"Initial token should be used. Seen: $tokens")

        // Simulate OAuth token refresh
        currentToken = "refreshed-token"
        tokens.clear()

        client.planScan(
          defaultNamespace.toString, "dynamicTokenTest")
        assert(tokens.contains("refreshed-token"),
          s"Refreshed token should be used. Seen: $tokens")
        assert(!tokens.contains("initial-token"),
          s"Old token should not appear. Seen: $tokens")
      } finally {
        client.close()
      }
    }
  }

  test("empty and null tokens do not cause errors") {
    Seq(
      ("empty string", () => ""),
      ("null", () => null)
    ).foreach { case (description, supplier) =>
      val tableName = description.replace(" ", "")
      withTempTable(s"${tableName}Test") { table =>
        val client = new IcebergRESTCatalogPlanningClient(
          serverUri, "test_catalog", supplier)
        try {
          val scanPlan = client.planScan(
            defaultNamespace.toString, s"${tableName}Test")
          assert(scanPlan != null,
            s"[$description] Scan plan should not be null")
          assert(scanPlan.files != null,
            s"[$description] Files should not be null")
        } finally {
          client.close()
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // E2E test: real OAuthTokenProvider + mock OAuth token endpoint
  // ---------------------------------------------------------------------------

  test("e2e: OAuthTokenProvider with mock OAuth server") {
    // Start a mock OAuth token endpoint using JDK HttpServer.
    // OAuthTokenProvider sends: POST /token with
    //   Authorization: Basic base64(clientId:clientSecret)
    //   Body: grant_type=client_credentials&scope=all-apis
    val tokenRequestCount = new AtomicInteger(0)
    val mockAccessToken = "mock-oauth-access-token"
    val oauthServer = startMockOAuthServer(
      tokenRequestCount, mockAccessToken, expiresInSeconds = 3600)

    try {
      val oauthUri =
        s"http://localhost:${oauthServer.getAddress.getPort}/token"

      // Create a real TokenProvider via the same path production
      // code uses: TokenProvider.create() with OAuth config
      val authConfig = Map(
        "type" -> "oauth",
        "oauth.uri" -> oauthUri,
        "oauth.clientId" -> "test-client-id",
        "oauth.clientSecret" -> "test-client-secret"
      )
      val tokenProvider =
        TokenProvider.create(authConfig.asJava)
      val supplier: () => String =
        () => tokenProvider.accessToken()

      withTempTable("oauthE2eTest") { table =>
        val client = new IcebergRESTCatalogPlanningClient(
          serverUri, "test_catalog", supplier)
        try {
          // First planScan triggers token fetch from mock server
          val scanPlan = client.planScan(
            defaultNamespace.toString, "oauthE2eTest")
          assert(scanPlan != null)

          // OAuthTokenProvider should have fetched exactly 1 token
          // (cached for subsequent calls within the same request)
          assert(tokenRequestCount.get() == 1,
            s"Expected 1 token request, " +
            s"got ${tokenRequestCount.get()}")

          // Second planScan reuses cached token (not expired)
          client.planScan(
            defaultNamespace.toString, "oauthE2eTest")
          assert(tokenRequestCount.get() == 1,
            s"Token should be cached across planScan calls. " +
            s"Expected 1 request, got ${tokenRequestCount.get()}")
        } finally {
          client.close()
        }
      }
    } finally {
      oauthServer.stop(0)
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Start a mock OAuth token server on a dynamic port.
   * Serves POST /token returning a valid OAuth client credentials
   * response. Validates the request format matches what
   * OAuthTokenProvider sends.
   *
   * Pattern follows UCTokenBasedRestClientSuite.scala:50-68.
   */
  private def startMockOAuthServer(
      requestCount: AtomicInteger,
      accessToken: String,
      expiresInSeconds: Long): HttpServer = {
    val oauthServer = HttpServer.create(
      new InetSocketAddress("localhost", 0), 0)
    oauthServer.createContext("/token", (exchange: HttpExchange) => {
      try {
        requestCount.incrementAndGet()

        // Validate request method
        assert(exchange.getRequestMethod == "POST",
          s"Expected POST, got ${exchange.getRequestMethod}")

        // Validate Authorization: Basic header is present
        val authHeader = exchange.getRequestHeaders
          .getFirst("Authorization")
        assert(authHeader != null &&
          authHeader.startsWith("Basic "),
          s"Expected Basic auth header, got: $authHeader")

        // Validate request body
        val body = new String(
          exchange.getRequestBody.readAllBytes(),
          StandardCharsets.UTF_8)
        assert(body.contains("grant_type=client_credentials"),
          s"Expected client_credentials grant, got: $body")

        // Return OAuth token response
        val responseJson =
          s"""{"access_token":"$accessToken",""" +
          s""""token_type":"Bearer",""" +
          s""""expires_in":$expiresInSeconds}"""
        val bytes = responseJson.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders
          .add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, bytes.length)
        exchange.getResponseBody.write(bytes)
      } finally {
        exchange.close()
      }
    })
    oauthServer.start()
    oauthServer
  }

  private def withTempTable[T](tableName: String)(
      func: shadedForDelta.org.apache.iceberg.Table => T): T = {
    IcebergRESTServerTestUtils.withTempTable(
      catalog, defaultNamespace, tableName,
      defaultSchema, defaultSpec, Some(server)
    )(func)
  }
}
