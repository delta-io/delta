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

package io.sparkuctest.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/** Shared helpers for the JDK-{@code HttpServer}-based OAuth/OIDC test mocks. */
final class MockHttpSupport {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The "Basic" auth-scheme prefix of an Authorization header value (RFC 7617). */
  private static final String BASIC_AUTH_PREFIX = "Basic ";

  private MockHttpSupport() {}

  /**
   * Returns true when the request carries credentials for the "Basic" HTTP authentication scheme
   * (RFC 7617) matching the expected user-id and password. This is generic Basic authentication;
   * OAuth's {@code client_secret_basic} client authentication reuses it with user-id=client_id and
   * password=client_secret.
   */
  static boolean basicAuthMatches(
      HttpExchange exchange, String expectedUserId, String expectedPassword) {
    String header = exchange.getRequestHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (header == null || !header.startsWith(BASIC_AUTH_PREFIX)) {
      return false;
    }
    String decoded =
        new String(
            Base64.getDecoder().decode(header.substring(BASIC_AUTH_PREFIX.length())),
            StandardCharsets.UTF_8);
    int colon = decoded.indexOf(':');
    if (colon < 0) {
      return false;
    }
    return expectedUserId.equals(decoded.substring(0, colon))
        && expectedPassword.equals(decoded.substring(colon + 1));
  }

  /**
   * Serializes the given fields to a JSON object (Jackson handles all value escaping) and writes it
   * as the response body with the given status code.
   */
  static void respondJson(HttpExchange exchange, int status, Map<String, ?> fields)
      throws IOException {
    byte[] bytes;
    try {
      bytes = MAPPER.writeValueAsBytes(fields);
    } catch (JsonProcessingException e) {
      throw new IOException("Failed to serialize JSON response", e);
    }
    exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
