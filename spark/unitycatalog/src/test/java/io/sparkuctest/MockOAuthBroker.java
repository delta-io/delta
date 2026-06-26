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

package io.sparkuctest;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.TokenType;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;

/**
 * Test OAuth broker that adapts the connector's client-credentials grant to UC's token-exchange
 * grant, so server-validating OAuth tests work without modifying the connector. It fuses three
 * roles that would be separate parties in a real deployment:
 *
 * <ul>
 *   <li><b>Client-credentials token endpoint</b> (faces the connector): {@code POST /token} accepts
 *       the connector's plain client-credentials grant.
 *   <li><b>OIDC identity provider</b> (faces UC): {@code GET /.well-known/openid-configuration} and
 *       {@code GET /jwks} let UC discover + trust this issuer (put it in {@code
 *       server.allowed-issuers}), and {@code /token} mints an {@code admin} id_token signed by this
 *       server's key.
 *   <li><b>Token-exchange client</b> (faces UC's STS): {@code /token} exchanges that id_token at
 *       UC's token-exchange endpoint for a UC-internal token, which it returns as {@code
 *       access_token}.
 * </ul>
 *
 * Net: the connector does a plain client-credentials call and receives a UC-acceptable token, while
 * UC actually validates it (issuer, signature, audience, enabled principal).
 */
public class MockOAuthBroker {

  /** Audience minted into the id_token; must match UC's {@code server.audiences}. */
  public static final String AUDIENCE = "uc-oauth-test-audience";

  /** Subject of the id_token. {@code admin} is auto-created + enabled when UC auth is on. */
  private static final String SUBJECT = "admin";

  /** Validity of the minted id_token (5 minutes); generous so the exchange completes within it. */
  private static final long ID_TOKEN_LIFETIME_MILLIS = 300_000L;

  private final String expectedClientId;
  private final String expectedClientSecret;
  // Maximum lifetime advertised in the /token response: any expires_in UC returns is capped at this
  // value. The connector renews ~30s before expiry, so a value at or below that lead time makes it
  // re-run the grant (and thus re-exchange) on each call.
  private final long maxLifetimeSeconds;
  private final AtomicInteger issuedTokenCount = new AtomicInteger();
  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpClient httpClient = HttpClient.newHttpClient();

  private HttpServer server;
  private ExecutorService executor;
  private RSAPublicKey publicKey;
  private Algorithm algorithm;
  private String keyId;

  // UC base URI (ends with '/'); Must be set after the UC server starts so /token can call the
  // exchange.
  @Setter private volatile String ucBaseUri;

  public MockOAuthBroker(
      String expectedClientId, String expectedClientSecret, long maxLifetimeSeconds) {
    this.expectedClientId = expectedClientId;
    this.expectedClientSecret = expectedClientSecret;
    this.maxLifetimeSeconds = maxLifetimeSeconds;
  }

  public void start() throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    var keyPair = gen.generateKeyPair();
    this.publicKey = (RSAPublicKey) keyPair.getPublic();
    this.algorithm = Algorithm.RSA512(publicKey, (RSAPrivateKey) keyPair.getPrivate());
    this.keyId = UUID.randomUUID().toString();

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/.well-known/openid-configuration", this::handleDiscovery);
    server.createContext("/jwks", this::handleJwks);
    server.createContext("/token", this::handleToken);
    // Multithreaded (daemon) executor is required: the /token handler blocks on UC's exchange
    // call, and UC calls back into this same server for discovery + JWKS. A single-threaded
    // executor would self-deadlock. Daemon threads let the forked test JVM exit cleanly.
    executor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(executor);
    server.start();
  }

  /** Issuer URL; register this exactly in UC's {@code server.allowed-issuers}. */
  public String issuer() {
    return "http://localhost:" + server.getAddress().getPort();
  }

  public String tokenEndpoint() {
    return issuer() + "/token";
  }

  public int issuedTokenCount() {
    return issuedTokenCount.get();
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
    // HttpServer.stop() does not shut down a custom Executor, so do it here; otherwise the cached
    // pool's (daemon) threads linger ~60s after each test class and accumulate across a run.
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }

  /**
   * OIDC discovery document ({@code GET /.well-known/openid-configuration}): advertises this
   * server's {@code issuer} and {@code jwks_uri} (OpenID Connect Discovery 1.0).
   */
  private void handleDiscovery(HttpExchange exchange) throws IOException {
    String issuer = issuer();
    MockHttpSupport.respondJson(
        exchange, 200, Map.of("issuer", issuer, "jwks_uri", issuer + "/jwks"));
  }

  /**
   * Serves the JWK Set: a {@code keys} array containing this server's RSA public signing key.
   * Follows the standard JWKS shape (RFC 7517 section 5), with the RSA public-key parameters
   * kty/n/e per RFC 7518 section 6.3.1 (n and e are Base64urlUInt-encoded; see toUnsignedBytes).
   */
  private void handleJwks(HttpExchange exchange) throws IOException {
    Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
    String n = enc.encodeToString(toUnsignedBytes(publicKey.getModulus()));
    String e = enc.encodeToString(toUnsignedBytes(publicKey.getPublicExponent()));
    MockHttpSupport.respondJson(
        exchange,
        200,
        Map.of(
            "keys",
            List.of(
                Map.of("kty", "RSA", "use", "sig", "alg", "RS512", "kid", keyId, "n", n, "e", e))));
  }

  /**
   * Token endpoint ({@code POST /token}): handles the client-credentials grant. On valid client
   * authentication it returns an OAuth 2.0 access-token response (RFC 6749 section 5.1) whose
   * {@code access_token} is a UC-internal token; on invalid client credentials it returns a 401
   * {@code invalid_client} error (RFC 6749 section 5.2).
   */
  private void handleToken(HttpExchange exchange) throws IOException {
    // OAuth client_secret_basic: the connector authenticates its client-credentials grant by
    // sending client id/secret as HTTP Basic, so we match them as the Basic user-id/password.
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())
        || !MockHttpSupport.basicAuthMatches(exchange, expectedClientId, expectedClientSecret)) {
      MockHttpSupport.respondJson(exchange, 401, Map.of("error", "invalid_client"));
      return;
    }
    // Drain the request body (the grant_type/scope form params) without parsing it: this mock
    // authenticates via the Basic header above and assumes the client-credentials grant, so the
    // body is unused. Consuming it leaves no unread request bytes on the connection when we
    // respond.
    exchange.getRequestBody().readAllBytes();
    try {
      String idToken = mintIdToken();
      ExchangedToken exchanged = exchangeForInternalToken(idToken);
      // Advertise the smaller of UC's expires_in (if any) and the broker's max lifetime. UC OSS
      // returns no expires_in today, so the max is used; if a future UC returns a longer expiry the
      // cap still applies, so a low test cap keeps forcing renewal. (The UC-internal token carries
      // no exp claim, so server-side expiry is not testable yet; see UCDeltaOAuthRefreshTest.)
      long expiresInSeconds =
          exchanged.expiresInSeconds != null
              ? Math.min(exchanged.expiresInSeconds, maxLifetimeSeconds)
              : maxLifetimeSeconds;
      MockHttpSupport.respondJson(
          exchange,
          200,
          Map.of(
              "access_token",
              exchanged.accessToken,
              "token_type",
              "Bearer",
              "expires_in",
              expiresInSeconds));
      issuedTokenCount.incrementAndGet();
    } catch (RuntimeException e) {
      // Jackson escapes the detail value, so a raw exception message is safe to embed.
      MockHttpSupport.respondJson(
          exchange,
          502,
          Map.of("error", "exchange_failed", "detail", String.valueOf(e.getMessage())));
    }
  }

  private String mintIdToken() {
    return JWT.create()
        .withSubject(SUBJECT)
        .withIssuer(issuer())
        .withAudience(AUDIENCE)
        .withIssuedAt(new Date())
        .withExpiresAt(new Date(System.currentTimeMillis() + ID_TOKEN_LIFETIME_MILLIS))
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .sign(algorithm);
  }

  /** A UC token-exchange result: the access token, plus its lifetime if UC advertised one. */
  private static final class ExchangedToken {
    final String accessToken;
    final Long expiresInSeconds; // null when UC did not advertise an expires_in

    ExchangedToken(String accessToken, Long expiresInSeconds) {
      this.accessToken = accessToken;
      this.expiresInSeconds = expiresInSeconds;
    }
  }

  /**
   * Exchanges the given id_token for a UC-internal access token at UC's token-exchange endpoint
   * (RFC 8693). Returns the access token and, if UC advertised an {@code expires_in}, its lifetime
   * (null otherwise). Throws if UC's base URI has not been set, or if the exchange request fails or
   * returns no token.
   */
  private ExchangedToken exchangeForInternalToken(String idToken) {
    if (ucBaseUri == null) {
      throw new IllegalStateException("UC base URI not set on MockOAuthBroker");
    }
    // RFC 8693 token-exchange form. The grant and token-type URNs come from UC's own enums rather
    // than hardcoded strings. (The model's toUrlQueryString() serializer that AuthCli uses is not
    // in the pinned UC version, so the form is assembled here.)
    String form =
        "grant_type="
            + GrantType.TOKEN_EXCHANGE.getValue()
            + "&requested_token_type="
            + TokenType.ACCESS_TOKEN.getValue()
            + "&subject_token_type="
            + TokenType.ID_TOKEN.getValue()
            + "&subject_token="
            + idToken;
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(ucBaseUri + "api/1.0/unity-control/auth/tokens"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.FORM_DATA.toString())
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IllegalStateException(
            "token-exchange failed: HTTP " + response.statusCode() + " " + response.body());
      }
      JsonNode body = mapper.readTree(response.body());
      JsonNode accessToken = body.get("access_token");
      if (accessToken == null || accessToken.asText().isEmpty()) {
        throw new IllegalStateException("token-exchange response missing access_token");
      }
      // UC OSS does not advertise token expiry today; take expires_in if a future version does,
      // otherwise leave it null so the caller falls back to its own lifetime.
      JsonNode expiresIn = body.get("expires_in");
      Long expiresInSeconds = expiresIn != null && expiresIn.isNumber() ? expiresIn.asLong() : null;
      return new ExchangedToken(accessToken.asText(), expiresInSeconds);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("token-exchange request failed", e);
    }
  }

  /**
   * RSA modulus/exponent as big-endian bytes without a leading sign byte. {@code
   * BigInteger.toByteArray()} prepends a 0x00 sign byte for positive values; the JWKS {@code
   * n}/{@code e} fields are Base64urlUInt-encoded (RFC 7518 section 2), which omits it.
   */
  private static byte[] toUnsignedBytes(BigInteger value) {
    byte[] bytes = value.toByteArray();
    if (bytes.length > 1 && bytes[0] == 0) {
      byte[] trimmed = new byte[bytes.length - 1];
      System.arraycopy(bytes, 1, trimmed, 0, trimmed.length);
      return trimmed;
    }
    return bytes;
  }
}
