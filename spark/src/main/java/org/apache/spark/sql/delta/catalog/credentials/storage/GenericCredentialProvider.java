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

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.retry.RetryPolicy;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.delta.catalog.credentials.ApiClientFactory;
import org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

public abstract class GenericCredentialProvider {
  static final Cache<String, GenericCredential> globalCache;
  private static final String UC_CREDENTIAL_CACHE_MAX_SIZE =
      "unitycatalog.credential.cache.maxSize";
  private static final long UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT = 1024;

  static {
    long maxSize = Long.getLong(UC_CREDENTIAL_CACHE_MAX_SIZE, UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT);
    globalCache = CacheBuilder.newBuilder().maximumSize(maxSize).build();
  }

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private URI ucUri;
  private TokenProvider tokenProvider;
  private String credUid;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    String clockName = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCDeltaRestCatalogApiCredentialConf.UC_RENEWAL_LEAD_TIME_KEY,
            UCDeltaRestCatalogApiCredentialConf.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    String ucUriStr = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY);
    Preconditions.checkNotNull(
        ucUriStr, "'%s' is not set in hadoop configuration",
        UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY);
    this.ucUri = URI.create(ucUriStr);

    this.tokenProvider =
        TokenProvider.create(conf.getPropsWithPrefix(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX));

    this.credUid = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY);
    Preconditions.checkState(
        credUid != null && !credUid.isEmpty(),
        "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
        UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY);

    this.credCacheEnabled =
        conf.getBoolean(
            UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

    this.credential = initGenericCredential(conf);
  }

  public abstract GenericCredential initGenericCredential(Configuration conf);

  public GenericCredential accessCredentials() {
    if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
          try {
            credential = renewCredential();
          } catch (ApiException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return credential;
  }

  protected TemporaryCredentialsApi temporaryCredentialsApi() {
    if (tempCredApi == null) {
      synchronized (this) {
        if (tempCredApi == null) {
          RetryPolicy retryPolicy = UCDeltaRestCatalogApiCredentialConf.createRequestRetryPolicy(conf);
          tempCredApi =
              new TemporaryCredentialsApi(
                  ApiClientFactory.createApiClient(retryPolicy, ucUri, tokenProvider));
        }
      }
    }

    return tempCredApi;
  }

  private GenericCredential renewCredential() throws ApiException {
    if (credCacheEnabled) {
      synchronized (globalCache) {
        GenericCredential cached = globalCache.getIfPresent(credUid);
        if (cached != null && !cached.readyToRenew(clock, renewalLeadTimeMillis)) {
          return cached;
        }
        GenericCredential renewed = createGenericCredentials();
        globalCache.put(credUid, renewed);
        return renewed;
      }
    } else {
      return createGenericCredentials();
    }
  }

  private GenericCredential createGenericCredentials() throws ApiException {
    TemporaryCredentials tempCred;
    String type = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY);

      tempCred =
          temporaryCredentialsApi()
              .generateTemporaryPathCredentials(
                  new GenerateTemporaryPathCredential()
                      .url(path)
                      .operation(PathOperation.fromValue(pathOperation)));
    } else if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY);

      tempCred =
          temporaryCredentialsApi()
              .generateTemporaryTableCredentials(
                  new GenerateTemporaryTableCredential()
                      .tableId(tableId)
                      .operation(TableOperation.fromValue(tableOperation)));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
              type, UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY));
    }

    return new GenericCredential(tempCred);
  }
}
