/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

/**
 * An {@link AccessTokenProvider} that returns a fixed OAuth2 access token from Hadoop
 * configuration. Used for GCS credential vending where the catalog service provides a short-lived
 * OAuth token.
 *
 * <p>Configuration keys:
 *
 * <ul>
 *   <li>{@code fs.gs.auth.access.token} — the OAuth2 access token (required)
 *   <li>{@code fs.gs.auth.access.token.expiration.ms} — expiration timestamp in epoch milliseconds
 *       (optional; defaults to 1 hour from current time)
 * </ul>
 */
public class FixedGcsAccessTokenProvider implements AccessTokenProvider {

  private static final long FALLBACK_EXPIRATION_MS = 3600_000L;

  private Configuration conf;

  @Override
  public AccessToken getAccessToken() {
    String token = conf.get(UnityCatalog.GCP_OAUTH_TOKEN_KEY);
    if (token == null || token.isEmpty()) {
      throw new RuntimeException(
          "Missing GCS access token in configuration: " + UnityCatalog.GCP_OAUTH_TOKEN_KEY);
    }

    long expirationMs;
    String expirationStr = conf.get("fs.gs.auth.access.token.expiration.ms");
    if (expirationStr != null && !expirationStr.isEmpty()) {
      try {
        expirationMs = Long.parseLong(expirationStr);
      } catch (NumberFormatException e) {
        expirationMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
      }
    } else {
      expirationMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
    }

    return new AccessToken(token, expirationMs);
  }

  @Override
  public void refresh() {}

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
