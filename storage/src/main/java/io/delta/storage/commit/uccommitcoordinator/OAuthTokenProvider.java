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

package io.delta.storage.commit.uccommitcoordinator;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
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

/**
 * Internal class - not intended for direct use.
 *
 * <p>OAuth-based token provider that fetches and automatically renews access tokens.
 *
 * <p>This provider uses the OAuth 2.0 client credentials flow to obtain access tokens. It
 * automatically renews tokens before they expire (default: 30 seconds before expiration). Token
 * requests are retried automatically for resilience against transient network failures.
 */
class OAuthTokenProvider implements TokenProvider {

  private static final long DEFAULT_LEAD_RENEWAL_TIME_SECONDS = 30L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;
  private long leadRenewalTimeSeconds;
  private CloseableHttpClient httpClient;
  private ObjectMapper mapper;

  private volatile TempToken tempToken;

  OAuthTokenProvider() {
  }

  @Override
  public void initialize(Map<String, String> configs) {
    // Parse and validate the OAuth URI.
    String oauthUri = configs.get(AuthConfigs.OAUTH_URI);
    Preconditions.checkArgument(
        oauthUri != null && !oauthUri.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_URI);
    this.oauthUri = oauthUri;

    // Parse and validate the OAuth Client ID.
    String oauthClientId = configs.get(AuthConfigs.OAUTH_CLIENT_ID);
    Preconditions.checkArgument(
        oauthClientId != null && !oauthClientId.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_CLIENT_ID);
    this.oauthClientId = oauthClientId;

    // Parse and validate the OAuth Client Secret.
    String oauthClientSecret = configs.get(AuthConfigs.OAUTH_CLIENT_SECRET);
    Preconditions.checkArgument(
        oauthClientSecret != null && !oauthClientSecret.isEmpty(),
        "Configuration key '%s' is missing or empty",
        AuthConfigs.OAUTH_CLIENT_SECRET);
    this.oauthClientSecret = oauthClientSecret;

    this.leadRenewalTimeSeconds = DEFAULT_LEAD_RENEWAL_TIME_SECONDS;
    this.httpClient = HttpClientBuilder.create().build();

    this.mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
  }

  @Override
  public String accessToken() {
    if (tempToken == null || tempToken.isReadyToRenew()) {
      synchronized (this) {
        if (tempToken == null || tempToken.isReadyToRenew()) {
          tempToken = renewToken();
        }
      }
    }
    return tempToken.token();
  }

  @Override
  public Map<String, String> configs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE);
    configs.put(AuthConfigs.OAUTH_URI, oauthUri);
    configs.put(AuthConfigs.OAUTH_CLIENT_ID, oauthClientId);
    configs.put(AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret);

    return configs;
  }

  private TempToken renewToken() {
    HttpPost request = new HttpPost(oauthUri);
    request.setHeader(HttpHeaders.CONTENT_TYPE,
        ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
    request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodeCredentials());

    String requestBody = toUrlEncoded(
        ImmutableMap.of("grant_type", "client_credentials", "scope", "all-apis"));
    request.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      String body = EntityUtils.toString(response.getEntity());
      if (statusLine.getStatusCode() == HttpStatus.SC_OK) {
        Map<String, String> result = mapper.readValue(body,
            new TypeReference<Map<String, String>>() {});

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
        return new TempToken(accessToken, Instant.ofEpochMilli(expirationTimeMillis));
      } else {
        throw new IOException(
            String.format(
                "Failed to obtain access token from OAuth endpoint %s, status code: %s, body: %s",
                oauthClientId, statusLine.getStatusCode(), body));
      }
    } catch (IOException e){
      throw new UncheckedIOException(e);
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
    String credentials = String.format("%s:%s", oauthClientId, oauthClientSecret);
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

  private class TempToken {

    private final String token;
    private final Instant expirationTime;

    TempToken(String token, Instant expirationTime) {
      this.token = token;
      this.expirationTime = expirationTime;
    }

    public String token() {
      return token;
    }

    public boolean isReadyToRenew() {
      Instant renewalTime = expirationTime.minusSeconds(leadRenewalTimeSeconds);
      return Instant.now().isAfter(renewalTime);
    }
  }
}
