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

package io.delta.storage.commit.uccommitcoordinator;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * OAuth-based implementation of {@link UCTokenProvider} that manages Unity Catalog access tokens
 * using OAuth 2.0 client credentials flow.
 *
 * <p>This provider automatically handles token renewal when tokens are about to expire,
 * with thread-safe caching to minimize token requests.
 *
 * <p>Thread Safety: This class is thread-safe. Multiple threads can safely call
 * {@link #accessToken()} concurrently.
 */
public class OAuthUCTokenProvider implements UCTokenProvider {

  /**
   * Lead time before token expiration to trigger renewal (30 seconds)
   */
  public static final long RENEW_LEAD_TIME_MILLIS = 30_000L;

  private final String uri;
  private final String clientId;
  private final String clientSecret;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;

  private volatile TemporaryToken tempToken;

  /**
   * Creates a new OAuth token provider.
   *
   * @param uri          OAuth token endpoint URI
   * @param clientId     OAuth client ID
   * @param clientSecret OAuth client secret
   * @throws IllegalArgumentException if any parameter is null or empty
   */
  public OAuthUCTokenProvider(String uri, String clientId, String clientSecret) {
    checkArg(uri != null && !uri.trim().isEmpty(), "URI cannot be null or empty");
    checkArg(clientId != null && !clientId.trim().isEmpty(),
        "Client ID cannot be null or empty");
    checkArg(clientSecret != null && !clientSecret.trim().isEmpty(),
        "Client secret cannot be null or empty");

    this.uri = uri;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.httpClient = HttpClientBuilder.create().build();

    this.mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
  }


  @Override
  public String accessToken() {
    TemporaryToken token = accessTempToken();
    return token.token;
  }

  TemporaryToken accessTempToken() {
    if (tempToken == null || tempToken.isReadyToRenew()) {
      synchronized (this) {
        if (tempToken == null || tempToken.isReadyToRenew()) {
          try {
            tempToken = requestToken();
          } catch (IOException e) {
            throw new RuntimeException("Failed to obtain OAuth token", e);
          }
        }
      }
    }
    return tempToken;
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  /**
   * Requests a new token from the OAuth server.
   *
   * @return a new temporary token
   * @throws IOException if the request fails
   */
  private TemporaryToken requestToken() throws IOException {
    HttpPost request = new HttpPost(uri);
    request.setHeader(HttpHeaders.CONTENT_TYPE,
        ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
    request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodeCredentials());

    String requestBody = toUrlEncoded(
        Map.of("grant_type", "client_credentials", "scope", "all-apis"));
    request.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      String body = EntityUtils.toString(response.getEntity());
      if (statusLine.getStatusCode() == HttpStatus.SC_OK) {
        Map<String, String> result = mapper.readValue(body, new TypeReference<>() {
        });

        // Parse and validate the access token.
        String accessToken = result.get("access_token");
        checkIOArg(accessToken != null && !accessToken.isEmpty(),
            "Response missing 'access_token' field");

        // Parse and validate the expires duration.
        String expiresInStr = result.get("expires_in");
        checkIOArg(expiresInStr != null && !expiresInStr.isEmpty(),
            "Response missing 'expires_in' field");

        // Parse and validate the expires duration.
        long expiresInSeconds;
        try {
          expiresInSeconds = Long.parseLong(expiresInStr);
        } catch (NumberFormatException e) {
          throw new IOException("Invalid 'expires_in' value: " + expiresInStr, e);
        }

        long expirationTimeMillis = System.currentTimeMillis() + (expiresInSeconds * 1000L);
        return new TemporaryToken(accessToken, expirationTimeMillis);
      } else {
        throw new IOException(
            String.format(
                "Failed to obtain access token from OAuth endpoint %s, status code: %s, body: %s",
                uri, statusLine.getStatusCode(), body));
      }
    }
  }

  private static void checkArg(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  private static void checkIOArg(boolean expression, String message) throws IOException {
    if (!expression) {
      throw new IOException(message);
    }
  }

  /**
   * Encodes client credentials for Basic authentication.
   *
   * @return Base64-encoded credentials
   */
  private String encodeCredentials() {
    String credentials = String.format("%s:%s", clientId, clientSecret);
    return Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Converts a map of parameters to URL-encoded form data.
   *
   * @param params parameters to encode
   * @return URL-encoded string
   */
  private static String toUrlEncoded(Map<String, String> params) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (sb.length() > 0) {
        sb.append("&");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  public static class TemporaryToken {

    private final String token;
    private final long expirationTimeMillis;

    public TemporaryToken(String token, long expirationTimeMillis) {
      this.token = token;
      this.expirationTimeMillis = expirationTimeMillis;
    }

    /**
     * Checks if the token is ready to be renewed. Returns true if the current time plus the renewal
     * lead time is at or past the expiration.
     *
     * @return true if the token should be renewed
     */
    public boolean isReadyToRenew() {
      return System.currentTimeMillis() + RENEW_LEAD_TIME_MILLIS >= expirationTimeMillis;
    }
  }
}
