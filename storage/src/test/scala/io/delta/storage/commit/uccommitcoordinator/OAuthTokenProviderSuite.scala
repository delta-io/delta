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

package io.delta.storage.commit.uccommitcoordinator

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class OAuthTokenProviderSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private val TEST_CLIENT_ID = "test-client-id"
  private val TEST_CLIENT_SECRET = "test-client-secret"

  private var server: HttpServer = _
  private var serverUri: String = _
  private var requestHandler: HttpExchange => Unit = _

  override def beforeAll(): Unit = {
    // Start a simple HTTP server on random available port
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/token", (exchange: HttpExchange) => {
      if (requestHandler != null) {
        requestHandler(exchange)
      }
      exchange.close()
    })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}/token"
  }

  override def afterAll(): Unit = {
    if (server != null) {
      server.stop(0)
    }
  }

  override def beforeEach(): Unit = {
    requestHandler = null
  }

  /** Helper to send a JSON response */
  private def sendJsonResponse(exchange: HttpExchange, statusCode: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(statusCode, bytes.length)
    val os = exchange.getResponseBody
    os.write(bytes)
    os.close()
  }

  /** Creates a successful OAuth response JSON */
  private def successResponse(token: String = "test-token", expiresIn: Long = 3600): String =
    s"""{"access_token":"$token","token_type":"Bearer","expires_in":"$expiresIn"}"""

  /** Sets up server to respond with success */
  private def respondWith(statusCode: Int, body: String): Unit = {
    requestHandler = exchange => sendJsonResponse(exchange, statusCode, body)
  }

  /** Sets up server to respond with multiple responses in sequence */
  private def respondWithSequence(responses: Seq[(Int, String)]): Unit = {
    val iterator = responses.iterator
    requestHandler = exchange => {
      val (code, body) = iterator.next()
      sendJsonResponse(exchange, code, body)
    }
  }

  /** Helper to create provider with configs */
  private def createProvider(
      uri: String = serverUri,
      clientId: String = TEST_CLIENT_ID,
      clientSecret: String = TEST_CLIENT_SECRET): OAuthTokenProvider = {
    val configs = Map(
      AuthConfigs.TYPE -> AuthConfigs.OAUTH_TYPE_VALUE,
      AuthConfigs.OAUTH_URI -> uri,
      AuthConfigs.OAUTH_CLIENT_ID -> clientId,
      AuthConfigs.OAUTH_CLIENT_SECRET -> clientSecret
    ).asJava
    val provider = new OAuthTokenProvider()
    provider.initialize(configs)
    provider
  }

  test("constructor validates all parameters") {
    val invalidValues = Seq(null, "")

    // URI validation
    for (invalidUri <- invalidValues) {
      intercept[IllegalArgumentException] {
        createProvider(uri = invalidUri)
      }
    }

    // Client ID validation
    for (invalidClientId <- invalidValues) {
      intercept[IllegalArgumentException] {
        createProvider(clientId = invalidClientId)
      }
    }

    // Client secret validation
    for (invalidClientSecret <- invalidValues) {
      intercept[IllegalArgumentException] {
        createProvider(clientSecret = invalidClientSecret)
      }
    }
  }

  test("accessToken fetches and caches token correctly") {
    val requestCount = new AtomicInteger(0)
    requestHandler = exchange => {
      requestCount.incrementAndGet()
      sendJsonResponse(exchange, HttpStatus.SC_OK, successResponse())
    }

    val provider = createProvider()
    val token1 = provider.accessToken()
    assert(token1 === "test-token")
    assert(requestCount.get() === 1)

    // Second call should use cached token
    val token2 = provider.accessToken()
    assert(token2 === "test-token")
    assert(requestCount.get() === 1) // Still only one request
  }

  test("accessToken renews expiring tokens") {
    respondWithSequence(Seq(
      (HttpStatus.SC_OK, successResponse("token-1", expiresIn = 20)), // Expires soon
      (HttpStatus.SC_OK, successResponse("token-2", expiresIn = 3600))
    ))

    val provider = createProvider()
    assert(provider.accessToken() === "token-1")
    assert(provider.accessToken() === "token-2") // Should renew
  }

  test("accessToken handles multiple sequential renewals") {
    val count = new AtomicInteger(0)
    requestHandler = exchange => {
      val num = count.incrementAndGet()
      sendJsonResponse(exchange, HttpStatus.SC_OK, successResponse(s"token-$num", expiresIn = 20))
    }

    val provider = createProvider()
    assert(provider.accessToken() === "token-1")
    assert(provider.accessToken() === "token-2")
    assert(provider.accessToken() === "token-3")
    assert(count.get() === 3)
  }

  test("accessToken handles various error responses") {
    // HTTP error
    respondWith(HttpStatus.SC_UNAUTHORIZED, """{"error":"invalid_client"}""")
    val provider1 = createProvider()
    val ex1 = intercept[Exception] {
      provider1.accessToken()
    }
    assert(ex1.getMessage.contains("Failed to obtain access token") ||
      ex1.getCause.getMessage.contains("status code: 401"))

    // Missing access_token
    respondWith(HttpStatus.SC_OK, """{"token_type":"Bearer","expires_in":"3600"}""")
    val provider2 = createProvider()
    val ex2 = intercept[Exception] {
      provider2.accessToken()
    }
    assert(ex2.getMessage.contains("missing 'access_token' field") ||
      ex2.getCause.getMessage.contains("missing 'access_token' field"))

    // Empty access_token
    respondWith(HttpStatus.SC_OK, """{"access_token":"","expires_in":"3600"}""")
    val provider3 = createProvider()
    val ex3 = intercept[Exception] {
      provider3.accessToken()
    }
    assert(ex3.getMessage.contains("missing 'access_token' field") ||
      ex3.getCause.getMessage.contains("missing 'access_token' field"))

    // Missing expires_in
    respondWith(HttpStatus.SC_OK, """{"access_token":"token"}""")
    val provider4 = createProvider()
    val ex4 = intercept[Exception] {
      provider4.accessToken()
    }
    assert(ex4.getMessage.contains("missing 'expires_in' field") ||
      ex4.getCause.getMessage.contains("missing 'expires_in' field"))

    // Invalid expires_in
    respondWith(HttpStatus.SC_OK, """{"access_token":"token","expires_in":"not-a-number"}""")
    val provider5 = createProvider()
    val ex5 = intercept[Exception] {
      provider5.accessToken()
    }
    assert(ex5.getMessage.contains("Invalid 'expires_in' value") ||
      ex5.getCause.getMessage.contains("Invalid 'expires_in' value"))

    // Malformed JSON
    respondWith(HttpStatus.SC_OK, "not valid json")
    val provider6 = createProvider()
    intercept[Exception] {
      provider6.accessToken()
    }
  }

  test("accessToken handles edge case expiration values") {
    // Zero expiration - token should be ready to renew immediately
    respondWith(HttpStatus.SC_OK, successResponse(expiresIn = 0))
    val provider1 = createProvider()
    val token1 = provider1.accessToken()
    assert(token1 === "test-token")

    // Negative expiration - token should be ready to renew immediately
    respondWith(HttpStatus.SC_OK, successResponse(expiresIn = -1))
    val provider2 = createProvider()
    val token2 = provider2.accessToken()
    assert(token2 === "test-token")
  }
}

