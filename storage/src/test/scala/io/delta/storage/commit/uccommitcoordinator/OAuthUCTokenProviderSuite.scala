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

package io.delta.storage.commit.uccommitcoordinator

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class OAuthUCTokenProviderSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

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

  test("constructor validates all parameters") {
    val invalidValues = Seq(null, "", "   ")

    // URI validation
    for (invalidUri <- invalidValues) {
      intercept[IllegalArgumentException] {
        new OAuthUCTokenProvider(invalidUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
      }
    }

    // Client ID validation
    for (invalidClientId <- invalidValues) {
      intercept[IllegalArgumentException] {
        new OAuthUCTokenProvider(serverUri, invalidClientId, TEST_CLIENT_SECRET)
      }
    }

    // Client secret validation
    for (invalidClientSecret <- invalidValues) {
      intercept[IllegalArgumentException] {
        new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, invalidClientSecret)
      }
    }
  }

  test("accessToken fetches and caches token correctly") {
    val requestCount = new AtomicInteger(0)
    requestHandler = exchange => {
      requestCount.incrementAndGet()
      sendJsonResponse(exchange, HttpStatus.SC_OK, successResponse())
    }

    val provider = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    try {
      val token1 = provider.accessToken()
      assert(token1 === "test-token")
      assert(requestCount.get() === 1)

      // Second call should use cached token
      val token2 = provider.accessToken()
      assert(token2 === "test-token")
      assert(requestCount.get() === 1) // Still only one request
    } finally {
      provider.close()
    }
  }

  test("accessToken renews expiring tokens") {
    respondWithSequence(Seq(
      (HttpStatus.SC_OK, successResponse("token-1", expiresIn = 20)), // Expires soon
      (HttpStatus.SC_OK, successResponse("token-2", expiresIn = 3600))
    ))

    val provider = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    try {
      assert(provider.accessToken() === "token-1")
      assert(provider.accessToken() === "token-2") // Should renew
    } finally {
      provider.close()
    }
  }

  test("accessToken handles multiple sequential renewals") {
    val count = new AtomicInteger(0)
    requestHandler = exchange => {
      val num = count.incrementAndGet()
      sendJsonResponse(exchange, HttpStatus.SC_OK, successResponse(s"token-$num", expiresIn = 20))
    }

    val provider = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    try {
      assert(provider.accessToken() === "token-1")
      assert(provider.accessToken() === "token-2")
      assert(provider.accessToken() === "token-3")
      assert(count.get() === 3)
    } finally {
      provider.close()
    }
  }

  test("accessToken handles various error responses") {
    // HTTP error
    respondWith(HttpStatus.SC_UNAUTHORIZED, """{"error":"invalid_client"}""")
    val provider1 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    val ex1 = intercept[RuntimeException] {
      provider1.accessToken()
    }
    assert(ex1.getMessage.contains("Failed to obtain OAuth token"))
    assert(ex1.getCause.getMessage.contains("status code: 401"))
    provider1.close()

    // Missing access_token
    respondWith(HttpStatus.SC_OK, """{"token_type":"Bearer","expires_in":"3600"}""")
    val provider2 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    val ex2 = intercept[RuntimeException] {
      provider2.accessToken()
    }
    assert(ex2.getCause.getMessage.contains("missing 'access_token' field"))
    provider2.close()

    // Empty access_token
    respondWith(HttpStatus.SC_OK, """{"access_token":"","expires_in":"3600"}""")
    val provider3 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    val ex3 = intercept[RuntimeException] {
      provider3.accessToken()
    }
    assert(ex3.getCause.getMessage.contains("missing 'access_token' field"))
    provider3.close()

    // Missing expires_in
    respondWith(HttpStatus.SC_OK, """{"access_token":"token"}""")
    val provider4 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    val ex4 = intercept[RuntimeException] {
      provider4.accessToken()
    }
    assert(ex4.getCause.getMessage.contains("missing 'expires_in' field"))
    provider4.close()

    // Invalid expires_in
    respondWith(HttpStatus.SC_OK, """{"access_token":"token","expires_in":"not-a-number"}""")
    val provider5 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    val ex5 = intercept[RuntimeException] {
      provider5.accessToken()
    }
    assert(ex5.getCause.getMessage.contains("Invalid 'expires_in' value"))
    provider5.close()

    // Malformed JSON
    respondWith(HttpStatus.SC_OK, "not valid json")
    val provider6 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    intercept[RuntimeException] {
      provider6.accessToken()
    }
    provider6.close()
  }

  test("accessToken handles edge case expiration values") {
    // Zero expiration
    respondWith(HttpStatus.SC_OK, successResponse(expiresIn = 0))
    val provider1 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    assert(provider1.accessTempToken().isReadyToRenew)
    provider1.close()

    // Negative expiration
    respondWith(HttpStatus.SC_OK, successResponse(expiresIn = -1))
    val provider2 = new OAuthUCTokenProvider(serverUri, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    assert(provider2.accessTempToken().isReadyToRenew)
    provider2.close()
  }
}
