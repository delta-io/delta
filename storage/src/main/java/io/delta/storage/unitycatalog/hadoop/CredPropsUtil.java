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

package io.delta.storage.unitycatalog.hadoop;

import static io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CredentialOperation;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StorageCredential;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StorageCredentialConfig;
import io.unitycatalog.client.auth.TokenProvider;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class CredPropsUtil {
  // Keep these as strings so callers can build credential options without loading cloud SDKs.
  private static final String ABFS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.delta.storage.unitycatalog.hadoop.credentials.AbfsVendedTokenProvider";
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.delta.storage.unitycatalog.hadoop.credentials.AwsVendedTokenProvider";
  private static final String GCS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.delta.storage.unitycatalog.hadoop.credentials.GcsVendedTokenProvider";
  private static final String CRED_SCOPED_FILE_SYSTEM_CLASS =
      "io.delta.storage.unitycatalog.hadoop.fs.CredScopedFileSystem";
  private static final String CRED_SCOPED_FS_CLASS =
      "io.delta.storage.unitycatalog.hadoop.fs.CredScopedFs";
  private static final String AZURE_ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";
  private static final String GCS_ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  private static final String GCS_ACCESS_TOKEN_EXPIRATION_KEY =
      "fs.gs.auth.access.token.expiration";

  private CredPropsUtil() {}

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final Map<String, String> builder = new LinkedHashMap<>();

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
      if (!UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(credType)
          && !UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(credType)) {
        throw new IllegalArgumentException(
            String.format("Invalid credential type '%s', must be either 'path' or 'table'.", credType));
      }
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY, credType);
      return self();
    }

    public T tableId(String tableId) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY, tableId);
      return self();
    }

    public T table(String catalog, String schema, String table, String location) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_CATALOG_KEY, catalog);
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_SCHEMA_KEY, schema);
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_NAME_KEY, table);
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_LOCATION_KEY, location);
      return self();
    }

    public T tableOperation(CredentialOperation tableOp) {
      builder.put(
          UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY,
          toTableOperationValue(tableOp));
      return self();
    }

    public T path(String path) {
      builder.put(UCDeltaRestCatalogApiCredentialConf.UC_PATH_KEY, path);
      return self();
    }

    public T pathOperation(CredentialOperation pathOp) {
      builder.put(
          UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY,
          toPathOperationValue(pathOp));
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
      return Collections.unmodifiableMap(new LinkedHashMap<>(builder));
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
            CRED_SCOPED_FILE_SYSTEM_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CRED_SCOPED_FILE_SYSTEM_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CRED_SCOPED_FS_CLASS);
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
            CRED_SCOPED_FILE_SYSTEM_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            CRED_SCOPED_FS_CLASS);
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
            CRED_SCOPED_FILE_SYSTEM_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            CRED_SCOPED_FILE_SYSTEM_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.Abfs",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.Abfss",
            CRED_SCOPED_FS_CLASS);
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
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    return new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
        .set("fs.s3a.access.key", requireCredentialField(
            config.getS3AccessKeyId(), credential, "S3 access key"))
        .set("fs.s3a.secret.key", requireCredentialField(
            config.getS3SecretAccessKey(), credential, "S3 secret key"))
        .set("fs.s3a.session.token", requireCredentialField(
            config.getS3SessionToken(), credential, "S3 session token"))
        .build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    S3PropsBuilder builder =
        new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER,
                AWS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY,
                requireCredentialField(config.getS3AccessKeyId(), credential, "S3 access key"))
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY,
                requireCredentialField(config.getS3SecretAccessKey(), credential, "S3 secret key"))
            .set(
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN,
                requireCredentialField(config.getS3SessionToken(), credential, "S3 session token"));

    if (credential.getExpirationTimeMs() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(credential.getExpirationTimeMs()));
    }

    return builder;
  }

  private static Map<String, String> s3TableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      String catalog,
      String schema,
      String table,
      String location,
      StorageCredential credential) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .table(catalog, schema, table, location)
        .tableOperation(requireOperation(credential))
        .build();
  }

  private static Map<String, String> s3PathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      StorageCredential credential) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(requireOperation(credential))
        .build();
  }

  private static Map<String, String> gsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    Long expirationTime =
        credential.getExpirationTimeMs() == null ? Long.MAX_VALUE : credential.getExpirationTimeMs();
    return new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(GCS_ACCESS_TOKEN_KEY, requireCredentialField(
            config.getGcsOauthToken(), credential, "GCS OAuth token"))
        .set(GCS_ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    GcsPropsBuilder builder =
        new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GCS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(
                UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN,
                requireCredentialField(config.getGcsOauthToken(), credential, "GCS OAuth token"));

    if (credential.getExpirationTimeMs() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
          String.valueOf(credential.getExpirationTimeMs()));
    }

    return builder;
  }

  private static Map<String, String> gsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      String catalog,
      String schema,
      String table,
      String location,
      StorageCredential credential) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .table(catalog, schema, table, location)
        .tableOperation(requireOperation(credential))
        .build();
  }

  private static Map<String, String> gsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      StorageCredential credential) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(requireOperation(credential))
        .build();
  }

  private static Map<String, String> abfsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    return new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(AZURE_ACCESS_TOKEN_KEY, requireCredentialField(
            config.getAzureSasToken(), credential, "Azure SAS token"))
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      StorageCredential credential) {
    StorageCredentialConfig config = requireSingleCloudConfig(credential);
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, ABFS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(
                UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN,
                requireCredentialField(config.getAzureSasToken(), credential, "Azure SAS token"));

    if (credential.getExpirationTimeMs() != null) {
      builder.set(
          UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
          String.valueOf(credential.getExpirationTimeMs()));
    }

    return builder;
  }

  private static Map<String, String> abfsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      String catalog,
      String schema,
      String table,
      String location,
      StorageCredential credential) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .table(catalog, schema, table, location)
        .tableOperation(requireOperation(credential))
        .build();
  }

  private static Map<String, String> abfsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      StorageCredential credential) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, credential)
        .credentialType(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(requireOperation(credential))
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
      String catalog,
      String schema,
      String table,
      String location,
      StorageCredential credential) {
    switch (scheme) {
      case "s3":
      case "s3a":
        if (renewCredEnabled) {
          return s3TableTempCredProps(
              credScopedFsEnabled,
              fsImplProps,
              uri,
              tokenProvider,
              tableId,
              catalog,
              schema,
              table,
              location,
              credential);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsTableTempCredProps(
              credScopedFsEnabled,
              fsImplProps,
              uri,
              tokenProvider,
              tableId,
              catalog,
              schema,
              table,
              location,
              credential);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTableTempCredProps(
              credScopedFsEnabled,
              fsImplProps,
              uri,
              tokenProvider,
              tableId,
              catalog,
              schema,
              table,
              location,
              credential);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      default:
        return Collections.emptyMap();
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
      StorageCredential credential) {
    switch (scheme) {
      case "s3":
      case "s3a":
        if (renewCredEnabled) {
          return s3PathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, credential);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, credential);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, credential);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, credential);
        }
      default:
        return Collections.emptyMap();
    }
  }

  private static StorageCredentialConfig requireSingleCloudConfig(StorageCredential credential) {
    StorageCredentialConfig config = requireConfig(credential);
    boolean hasS3 =
        config.getS3AccessKeyId() != null
            || config.getS3SecretAccessKey() != null
            || config.getS3SessionToken() != null;
    boolean hasAzure = config.getAzureSasToken() != null;
    boolean hasGcs = config.getGcsOauthToken() != null;
    int configuredClouds = (hasS3 ? 1 : 0) + (hasAzure ? 1 : 0) + (hasGcs ? 1 : 0);
    if (configuredClouds != 1) {
      throw new IllegalArgumentException(
          String.format(
              "UC Delta Rest Catalog API storage credential for prefix %s must contain exactly "
                  + "one cloud credential config.",
              credential.getPrefix()));
    }
    return config;
  }

  private static StorageCredentialConfig requireConfig(StorageCredential credential) {
    Objects.requireNonNull(credential, "credential cannot be null");
    StorageCredentialConfig config = credential.getConfig();
    if (config == null) {
      throw new IllegalArgumentException(
          String.format(
              "UC Delta Rest Catalog API storage credential for prefix %s is missing config.",
              credential.getPrefix()));
    }
    return config;
  }

  private static CredentialOperation requireOperation(StorageCredential credential) {
    Objects.requireNonNull(credential, "credential cannot be null");
    CredentialOperation operation = credential.getOperation();
    if (operation == null) {
      throw new IllegalArgumentException(
          String.format(
              "UC Delta Rest Catalog API storage credential for prefix %s is missing operation.",
              credential.getPrefix()));
    }
    return operation;
  }

  private static String requireCredentialField(
      String value, StorageCredential credential, String field) {
    if (value == null) {
      throw new IllegalArgumentException(
          String.format(
              "UC Delta Rest Catalog API storage credential for prefix %s is missing %s.",
              credential.getPrefix(), field));
    }
    return value;
  }

  private static String toTableOperationValue(CredentialOperation operation) {
    switch (operation) {
      case READ:
      case READ_WRITE:
        return operation.name();
      default:
        throw new IllegalArgumentException(
            "Unsupported UC Delta Rest Catalog API credential operation: " + operation);
    }
  }

  private static String toPathOperationValue(CredentialOperation operation) {
    switch (operation) {
      case READ:
        return "PATH_READ";
      case READ_WRITE:
        return "PATH_READ_WRITE";
      default:
        throw new IllegalArgumentException(
            "Unsupported UC Delta Rest Catalog API credential operation: " + operation);
    }
  }
}
