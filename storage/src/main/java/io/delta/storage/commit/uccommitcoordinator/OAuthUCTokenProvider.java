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

public class OAuthUCTokenProvider implements UCTokenProvider {

  public static final long renewLeadTimeMillis = 30_000L;
  private final String uri;
  private final String clientId;
  private final String clientSecret;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;

  private volatile TemporaryToken tempToken;

  public OAuthUCTokenProvider(String uri, String clientId, String clientSecret) {
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
    if (tempToken == null || tempToken.isReadyToRenew()) {
      synchronized (this) {
        if (tempToken == null || tempToken.isReadyToRenew()) {
          try {
            tempToken = requestToken();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return tempToken.token;
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

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
        String accessToken = result.get("access_token");
        long expiresInSeconds = Long.parseLong(result.get("expires_in"));
        return new TemporaryToken(accessToken, expiresInSeconds * 1000L);
      } else {
        throw new IOException(
            String.format("Failed to obtain access token from %s, status code: %s, body: %s", uri,
                statusLine.getStatusCode(), body));
      }
    }
  }

  private String encodeCredentials() {
    String credentials = String.format("%s:%s", clientId, clientSecret);
    Base64.Encoder encoder = Base64.getEncoder();
    return encoder.encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  private static String toUrlEncoded(Map<String, String> params) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : params.entrySet()) {
      if (sb.length() > 0) {
        sb.append("&");
      }
      sb.append(e.getKey()).append('=').append(e.getValue());
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

    public boolean isReadyToRenew() {
      return System.currentTimeMillis() + renewLeadTimeMillis >= expirationTimeMillis;
    }
  }
}
