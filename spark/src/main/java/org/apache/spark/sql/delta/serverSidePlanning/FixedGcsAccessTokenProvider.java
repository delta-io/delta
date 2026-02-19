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

package org.apache.spark.sql.delta.serverSidePlanning;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import com.google.auth.oauth2.AccessToken;

/**
 * A custom AccessTokenProvider used for server-side planning with temporary GCS credentials from
 * credential vending services.
 *
 * Configuration keys:
 * - fs.gs.auth.access.token: The OAuth2 access token
 * - fs.gs.auth.access.token.expiration.ms: Optional expiration timestamp in epoch milliseconds
 *
 * If no expiration is provided, defaults to 1 hour from current time. This default does not
 * guarantee that the token will be valid for the entire duration of the query. If the actual token expires earlier, queries will fail.
 */
public class FixedGcsAccessTokenProvider implements AccessTokenProvider {

  private static final String CONFIG_TOKEN = "fs.gs.auth.access.token";
  private static final String CONFIG_EXPIRATION_MS = "fs.gs.auth.access.token.expiration.ms";
  private static final long FALLBACK_EXPIRATION_MS = 3600_000L; 

  private Configuration conf;

  @Override
  public AccessToken getAccessToken() {
    String token = conf.get(CONFIG_TOKEN);
    if (token == null || token.isEmpty()) {
      throw new RuntimeException("Missing GCS access token in configuration: " + CONFIG_TOKEN);
    }

    // Read expiration timestamp from config, or use fallback
    long expirationMs;
    String expirationStr = conf.get(CONFIG_EXPIRATION_MS);
    if (expirationStr != null && !expirationStr.isEmpty()) {
      try {
        expirationMs = Long.parseLong(expirationStr);
      } catch (NumberFormatException e) {
        // If parsing fails, use fallback
        expirationMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
      }
    } else {
      // No expiration provided, use fallback. 
      expirationMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
    }

    return new AccessToken(token, expirationMs);
  }

  @Override
  public void refresh() {
    // Refresh is not supported, token is static and expires.
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
