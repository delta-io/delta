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

package org.apache.spark.sql.delta.catalog.credentials.storage;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.client.model.GcpOauthToken;

import java.io.IOException;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf;
import org.sparkproject.guava.base.Preconditions;

public class GcsVendedTokenProvider extends GenericCredentialProvider
    implements AccessTokenProvider {

  public static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  public static final String ACCESS_TOKEN_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";
  private Configuration conf;

  public GcsVendedTokenProvider() {}

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN) != null) {
      String oauthToken = conf.get(UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN);
      Preconditions.checkNotNull(
          oauthToken,
          "GCS OAuth token not set, please check '%s' in hadoop configuration",
          UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN);

      long expiredTimeMillis =
          conf.getLong(UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
              Long.MAX_VALUE);
      Preconditions.checkState(
          expiredTimeMillis > 0,
          "Expired time %s must be greater than 0, please check configure key '%s'",
          expiredTimeMillis,
          UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME);

      return GenericCredential.forGcs(oauthToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public AccessToken getAccessToken() {
    GenericCredential generic = accessCredentials();

    GcpOauthToken gcpToken = generic.temporaryCredentials().getGcpOauthToken();
    Preconditions.checkNotNull(gcpToken, "GCS OAuth token of generic credential cannot be null");

    String tokenValue = gcpToken.getOauthToken();
    Preconditions.checkNotNull(tokenValue, "GCS OAuth token value cannot be null");

    Long expirationMillis = generic.temporaryCredentials().getExpirationTime();
    Instant expirationInstant =
        expirationMillis == null ? null : Instant.ofEpochMilli(expirationMillis);
    return new AccessToken(tokenValue, expirationInstant);
  }

  @Override
  public void refresh() throws IOException {
    // Renewal happens when getAccessToken() calls accessCredentials().
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    initialize(configuration);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
