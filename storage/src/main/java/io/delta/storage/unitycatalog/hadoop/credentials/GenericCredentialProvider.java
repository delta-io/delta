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

package io.delta.storage.unitycatalog.hadoop.credentials;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CredentialOperation;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CredentialsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StorageCredential;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf;
import org.apache.hadoop.conf.Configuration;

public abstract class GenericCredentialProvider {
  static final Map<String, GenericCredential> globalCache;
  private static final String UC_CREDENTIAL_CACHE_MAX_SIZE =
      "unitycatalog.credential.cache.maxSize";
  private static final long UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT = 1024;

  static {
    final long maxSize =
        Long.getLong(UC_CREDENTIAL_CACHE_MAX_SIZE, UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT);
    globalCache =
        Collections.synchronizedMap(
            new LinkedHashMap<String, GenericCredential>(16, 0.75f, true) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<String, GenericCredential> eldest) {
                return size() > maxSize;
              }
            });
  }

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private String ucUri;
  private TokenProvider tokenProvider;
  private String credUid;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile UCClient ucClient;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    String clockName = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCDeltaRestCatalogApiCredentialConf.UC_RENEWAL_LEAD_TIME_KEY,
            UCDeltaRestCatalogApiCredentialConf.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    String ucUriStr = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY);
    Objects.requireNonNull(
        ucUriStr,
        String.format(
            "'%s' is not set in hadoop configuration",
            UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY));
    this.ucUri = ucUriStr;

    this.tokenProvider =
        TokenProvider.create(
            conf.getPropsWithPrefix(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX));

    this.credUid = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY);
    checkState(
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
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return credential;
  }

  protected UCClient ucClient() {
    if (ucClient == null) {
      synchronized (this) {
        if (ucClient == null) {
          ucClient = new UCTokenBasedRestClient(ucUri, tokenProvider, Collections.emptyMap());
        }
      }
    }

    return ucClient;
  }

  private GenericCredential renewCredential() throws IOException {
    if (credCacheEnabled) {
      synchronized (globalCache) {
        GenericCredential cached = globalCache.get(credUid);
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

  private GenericCredential createGenericCredentials() throws IOException {
    String type = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      throw new UnsupportedOperationException(
          "UC Delta Rest Catalog API path credential renewal is not supported by this Delta "
              + "version.");
    } else if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String catalog = requireConf(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_CATALOG_KEY);
      String schema = requireConf(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_SCHEMA_KEY);
      String table = requireConf(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_NAME_KEY);
      String location = requireConf(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_LOCATION_KEY);
      String tableOperation = requireConf(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY);

      CredentialsResponse response =
          ucClient()
              .getTableCredentials(
                  CredentialOperation.valueOf(tableOperation), catalog, schema, table);
      return GenericCredential.fromStorageCredential(
          selectStorageCredential(location, response.getStorageCredentials()));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
              type, UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY));
    }
  }

  private String requireConf(String key) {
    String value = conf.get(key);
    checkState(value != null && !value.isEmpty(), "'%s' is not set", key);
    return value;
  }

  private StorageCredential selectStorageCredential(
      String location, List<StorageCredential> storageCredentials) {
    StorageCredential bestMatch = null;
    for (StorageCredential credential : storageCredentials) {
      if (credential != null
          && credential.getPrefix() != null
          && matchesCredentialPrefix(location, credential.getPrefix())
          && (bestMatch == null || credential.getPrefix().length() > bestMatch.getPrefix().length())) {
        bestMatch = credential;
      }
    }
    checkState(
        bestMatch != null, "No UC Delta Rest Catalog API credential matched location '%s'.", location);
    return bestMatch;
  }

  private boolean matchesCredentialPrefix(String location, String prefix) {
    String normalizedLocation = stripTrailingSlash(location);
    String normalizedPrefix = stripTrailingSlash(prefix);
    return !normalizedPrefix.isEmpty()
        && (normalizedLocation.equals(normalizedPrefix)
            || (normalizedLocation.startsWith(normalizedPrefix)
                && normalizedLocation.charAt(normalizedPrefix.length()) == '/'));
  }

  private String stripTrailingSlash(String value) {
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  private static void checkState(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, args));
    }
  }
}
