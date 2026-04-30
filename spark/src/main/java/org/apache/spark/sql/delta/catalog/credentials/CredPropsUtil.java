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

package org.apache.spark.sql.delta.catalog.credentials;

import static org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.delta.catalog.credentials.fs.CredScopedFileSystem;
import org.apache.spark.sql.delta.catalog.credentials.fs.CredScopedFs;
import org.apache.spark.sql.delta.catalog.credentials.storage.AbfsVendedTokenProvider;
import org.apache.spark.sql.delta.catalog.credentials.storage.AwsVendedTokenProvider;
import org.apache.spark.sql.delta.catalog.credentials.storage.GcsVendedTokenProvider;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.collect.ImmutableMap;

public class CredPropsUtil {
  private CredPropsUtil() {}

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    public T set(String key, String value) {
      builder.put(key, value);
      return self();
    }

    public T uri(String uri) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY, uri);
      return self();
    }

    public T tokenProvider(TokenProvider tokenProvider) {
      tokenProvider
          .configs()
          .forEach(
              (key, value) ->
                  builder.put(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX + key, value));
      return self();
    }

    public T uid(String uid) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY, uid);
      return self();
    }

    public T credentialType(String credType) {
      Preconditions.checkArgument(
          UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(credType)
              || UCDeltaRestCatalogApiCredentialConf
                  .UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(credType),
          "Invalid credential type '%s', must be either 'path' or 'table'.",
          credType);
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY, credType);
      return self();
    }

    public T tableId(String tableId) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY, tableId);
      return self();
    }

    public T tableOperation(TableOperation tableOp) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY, tableOp.getValue());
      return self();
    }

    public T path(String path) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_PATH_KEY, path);
      return self();
    }

    public T pathOperation(PathOperation pathOp) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY, pathOp.getValue());
      return self();
    }

    public T saveAndOverride(
        Map<String, String> fsImplProps, String key, String defaultOriginal, String newValue) {
      builder.put(key + ".original", fsImplProps.getOrDefault(key, defaultOriginal));
      builder.put(key, newValue);
      return self();
    }

    protected abstract T self();

    public Map<String, String> build() {
      return builder.build();
    }
  }

  private static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {
    S3PropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      set("fs.s3a.path.style.access", "true");
      set("fs.s3.impl.disable.cache", "true");
      set("fs.s3a.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CredScopedFs.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected S3PropsBuilder self() {
      return this;
    }
  }

  private static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {
    GcsPropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      set("fs.gs.create.items.conflict.check.enable", "true");
      set("fs.gs.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  private static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {
    AbfsPropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      set("fs.abfs.impl.disable.cache", "true");
      set("fs.abfss.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.Abfs",
            CredScopedFs.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.Abfss",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }

  private static Map<String, String> s3FixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    return new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
        .set("fs.s3a.access.key", awsCred.getAccessKeyId())
        .set("fs.s3a.secret.key", awsCred.getSecretAccessKey())
        .set("fs.s3a.session.token", awsCred.getSessionToken())
        .build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder =
        new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER,
                AwsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId())
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY,
                awsCred.getSecretAccessKey())
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN,
                awsCred.getSessionToken());

    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> s3TableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> s3PathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> gsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    Long expirationTime =
        tempCreds.getExpirationTime() == null ? Long.MAX_VALUE : tempCreds.getExpirationTime();
    return new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken())
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    GcpOauthToken gcpToken = tempCreds.getGcpOauthToken();
    GcsPropsBuilder builder =
        new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(
                UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN,
                gcpToken.getOauthToken());

    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> gsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> gsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> abfsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    return new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, azureSas.getSasToken())
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN, azureSas.getSasToken());

    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> abfsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> abfsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String scheme,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
      case "s3a":
        if (renewCredEnabled) {
          return s3TableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsTableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }

  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String scheme,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
      case "s3a":
        if (renewCredEnabled) {
          return s3PathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }
}
