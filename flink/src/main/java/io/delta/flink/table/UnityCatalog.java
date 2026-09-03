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

package io.delta.flink.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.function.CheckedSupplier;
import io.delta.kernel.types.*;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.TableIdentifier;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCConfigUtils;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import io.delta.storage.commit.uccommitcoordinator.exceptions.NoSuchTableException;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.*;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code UnityCatalog} is a {@link DeltaCatalog} implementation backed by Unity Catalog.
 *
 * <p>Table resolution, credential vending, and creation go through the UC Delta-Tables API (via
 * {@link UCDeltaTokenBasedRestClient#loadTable} / {@code createStagingTable}). The credentials it
 * vends register a Unity Catalog credential provider that refreshes short-lived credentials
 * automatically. Catalog browsing (schema/table listing) uses the UC OpenAPI client, which has no
 * Delta-Tables equivalent.
 *
 * <p>Authentication is performed using a bearer token (or OAuth client credentials) supplied at
 * construction time.
 */
public class UnityCatalog implements DeltaCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(UnityCatalog.class);

  enum AuthMode {
    token,
    oauth;
  }

  static Map<String, String> buildTokenProviderConf(
      AuthMode authMode,
      String token,
      URI oauthUri,
      String oauthClientId,
      String oauthClientSecret) {
    switch (authMode) {
      case oauth:
        return Map.of(
            "type",
            "oauth",
            "oauth.uri",
            oauthUri.toString(),
            "oauth.clientId",
            oauthClientId,
            "oauth.clientSecret",
            oauthClientSecret);
      default:
        return Map.of("type", "static", "token", token);
    }
  }

  /**
   * Builds the {@code ucConfig} map consumed by {@link UCDeltaTokenBasedRestClient}: the endpoint
   * under {@code uri}, the auth settings under the {@code auth.} prefix, and the app versions under
   * the {@code appVersions.} prefix.
   */
  static Map<String, String> buildUcConfig(
      URI endpoint,
      AuthMode authMode,
      String token,
      URI oauthUri,
      String oauthClientId,
      String oauthClientSecret) {
    Map<String, String> ucConfig = new HashMap<>();
    ucConfig.put(UCConfigUtils.URI_KEY, endpoint.toString());
    buildTokenProviderConf(authMode, token, oauthUri, oauthClientId, oauthClientSecret)
        .forEach((key, value) -> ucConfig.put(UCConfigUtils.AUTH_PREFIX + key, value));
    VersionHelper.appVersions()
        .forEach(
            (name, version) -> ucConfig.put(UCConfigUtils.APP_VERSIONS_PREFIX + name, version));
    return ucConfig;
  }

  private static Map<String, String> requiredCreateProperties(
      String tableId,
      Map<String, String> requestedProperties,
      UCDeltaModels.StagingTableInfo stagingTableInfo) {
    Map<String, String> requiredProperties = new HashMap<>();
    AbstractProtocol requiredProtocol = stagingTableInfo.getRequiredProtocol();
    if (requiredProtocol != null) {
      Set<String> requiredFeatures = new HashSet<>();
      if (requiredProtocol.getReaderFeatures() != null) {
        requiredFeatures.addAll(requiredProtocol.getReaderFeatures());
      }
      if (requiredProtocol.getWriterFeatures() != null) {
        requiredFeatures.addAll(requiredProtocol.getWriterFeatures());
      }
      requiredFeatures.forEach(
          feature ->
              putRequiredProperty(
                  tableId,
                  requestedProperties,
                  requiredProperties,
                  "delta.feature." + feature,
                  "supported"));
    }
    stagingTableInfo
        .getRequiredProperties()
        .forEach(
            (key, value) -> {
              if (value != null) {
                putRequiredProperty(tableId, requestedProperties, requiredProperties, key, value);
              }
            });
    return requiredProperties;
  }

  private static void putRequiredProperty(
      String tableId,
      Map<String, String> requestedProperties,
      Map<String, String> requiredProperties,
      String key,
      String requiredValue) {
    String requestedValue = requestedProperties.get(key);
    if (requestedValue != null && !requestedValue.equals(requiredValue)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create table %s: catalog requires %s=%s, but the requested value is %s",
              tableId, key, requiredValue, requestedValue));
    }
    requiredProperties.put(key, requiredValue);
  }

  private final String name;
  private final URI endpoint;
  private final AuthMode authMode;
  private String token;
  private URI oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;
  private final boolean credentialVendingEnabled;

  /**
   * Lazily initialized API client used to communicate with the catalog service. This field is
   * marked transient to avoid serialization issues in distributed environments.
   */
  protected transient ApiClient apiClient;

  /** Client for table operations backed by the UC Delta-Tables API. */
  protected transient UCDeltaClient deltaClient;

  /**
   * Creates a {@code RESTCatalog} with the given endpoint and authentication token.
   *
   * @param name catalog name
   * @param endpoint the catalog REST endpoint URI as a string
   * @param token a bearer token used to authenticate REST requests
   */
  public UnityCatalog(String name, URI endpoint, String token) {
    this(name, endpoint, token, true);
  }

  public UnityCatalog(String name, URI endpoint, String token, boolean credentialVendingEnabled) {
    this.name = name;
    this.endpoint = endpoint;
    this.token = token;
    this.authMode = AuthMode.token;
    this.credentialVendingEnabled = credentialVendingEnabled;
  }

  /**
   * Creates a {@code RESTCatalog} with the given endpoint and OAuth authentication.
   *
   * @param name catalog name
   * @param endpoint the catalog REST endpoint URI as a string
   * @param oauthUri oauth uri
   * @param clientId oauth clientId
   * @param clientSecret oauth client secret
   */
  public UnityCatalog(
      String name, URI endpoint, URI oauthUri, String clientId, String clientSecret) {
    this(name, endpoint, oauthUri, clientId, clientSecret, true);
  }

  public UnityCatalog(
      String name,
      URI endpoint,
      URI oauthUri,
      String clientId,
      String clientSecret,
      boolean credentialVendingEnabled) {
    this.name = name;
    this.endpoint = endpoint;
    this.authMode = AuthMode.oauth;
    this.oauthUri = oauthUri;
    this.oauthClientId = clientId;
    this.oauthClientSecret = clientSecret;
    this.credentialVendingEnabled = credentialVendingEnabled;
  }

  /**
   * Returns the catalog name.
   *
   * @return the catalog name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the catalog REST endpoint.
   *
   * @return the catalog endpoint URI
   */
  public URI getEndpoint() {
    return endpoint;
  }

  /**
   * Returns the authentication token used for catalog access.
   *
   * @return the catalog bearer token
   */
  public String getToken() {
    return token;
  }

  /** @return the current ApiClient */
  public ApiClient getApiClient() {
    return apiClient;
  }

  /**
   * Parses {@code schema.table} or {@code catalog.schema.table} into a {@link UCTableIdentifier}.
   * In the 2-part form the catalog defaults to this catalog's name; in the 3-part form the leading
   * segment must equal this catalog's name.
   */
  UCTableIdentifier toUcTableIdentifier(String qualifiedTableName) {
    String[] namespaces = qualifiedTableName.split("\\.");
    Preconditions.checkArgument(namespaces.length == 2 || namespaces.length == 3);
    String catalogName;
    String schemaName;
    String tableName;
    if (namespaces.length == 3) {
      Preconditions.checkArgument(
          namespaces[0].equals(getName()),
          String.format(
              "table's catalog name %s must match catalog's name %s", namespaces[0], getName()));
      catalogName = namespaces[0];
      schemaName = namespaces[1];
      tableName = namespaces[2];
    } else {
      catalogName = getName();
      schemaName = namespaces[0];
      tableName = namespaces[1];
    }
    return new UCTableIdentifier(catalogName, schemaName, tableName);
  }

  @Override
  public void open() {
    if (apiClient == null) {
      Map<String, String> tokenProviderConf =
          buildTokenProviderConf(authMode, token, oauthUri, oauthClientId, oauthClientSecret);
      ApiClientBuilder builder =
          ApiClientBuilder.create()
              .uri(endpoint)
              .tokenProvider(TokenProvider.create(tokenProviderConf))
              .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(5).build());
      Map<String, String> appVersions = VersionHelper.appVersions();
      appVersions.forEach(builder::addAppVersion);
      apiClient = builder.build();
    }
    if (deltaClient == null) {
      Map<String, String> ucConfig =
          buildUcConfig(endpoint, authMode, token, oauthUri, oauthClientId, oauthClientSecret);
      ucConfig.put(
          UCConfigUtils.CREDENTIAL_VENDING_ENABLED_KEY, Boolean.toString(credentialVendingEnabled));
      deltaClient =
          new UCDeltaTokenBasedRestClient(ucConfig, /* hadoopConfSupplier = */ Configuration::new);
    }
  }

  protected <RET> RET withErrorHandling(CheckedSupplier<RET> body) {
    // TODO implement retry
    try {
      return body.get();
    } catch (Throwable e) {
      if (e instanceof ApiException) {
        ApiException apiException = (ApiException) e;
        try {
          JsonNode node = new ObjectMapper().readTree(apiException.getResponseBody());
          switch (node.get("error_code").asText()) {
            case "TABLE_ALREADY_EXISTS":
              throw new ExceptionUtils.ResourceAlreadyExistException(node.get("message").asText());
            case "TABLE_DOES_NOT_EXIST":
              throw new ExceptionUtils.ResourceNotFoundException(node.get("message").asText());
            default:
              throw new RuntimeException(apiException);
          }
        } catch (Exception ex) {
          throw ExceptionUtils.wrap(ex);
        }
      }
      throw ExceptionUtils.wrap(e);
    }
  }

  /** Converts a {@code schema.table} or {@code catalog.schema.table} name to the storage id. */
  private TableIdentifier toStorageTableIdentifier(String qualifiedTableName) {
    UCTableIdentifier id = toUcTableIdentifier(qualifiedTableName);
    return new TableIdentifier(
        new String[] {id.getCatalogName(), id.getSchemaName()}, id.getTableName());
  }

  /**
   * Loads table metadata from the remote catalog.
   *
   * <p>This method retrieves table information via the catalog REST API and resolves the table
   * storage location into a normalized {@link URI}. The returned {@link TableDescriptor} contains
   * the table UUID and resolved storage path.
   *
   * @param tableName the logical identifier of the table
   * @return a {@link TableDescriptor} describing the resolved table
   */
  @Override
  public TableDescriptor getTable(String tableName) {
    Objects.requireNonNull(tableName);
    return withErrorHandling(
        () -> {
          UCDeltaModels.TableInfo tableInfo;
          try {
            tableInfo = deltaClient.loadTable(toStorageTableIdentifier(tableName));
          } catch (NoSuchTableException e) {
            throw new ExceptionUtils.ResourceNotFoundException(e.getMessage());
          }
          TableDescriptor brief =
              new TableDescriptor(
                  tableName,
                  tableInfo.getTableId().toString(),
                  AbstractKernelTable.normalize(
                      URI.create(Objects.requireNonNull(tableInfo.getLocation()))),
                  tableInfo.getStorageProperties());
          LOG.debug("Loaded table with UUID {} at {}", brief.uuid, brief.tablePath);
          return brief;
        });
  }

  /**
   * Reserves a staging table in Unity Catalog and hands its allocated storage location to {@code
   * callback} so the caller can initialize the Delta table there.
   *
   * <p>The staging table is promoted to a real managed table by the Kernel UC committer when the
   * initial (version 0) commit is written, so this method does not register the table itself.
   *
   * @param tableId the {@code catalog.schema.table} identifier of the table to create
   * @param callback invoked with the reserved table id and storage location
   */
  @Override
  public void createTable(
      String tableId,
      StructType schema,
      List<String> partitions,
      Map<String, String> properties,
      Consumer<TableDescriptor> callback) {
    Objects.requireNonNull(tableId);
    Objects.requireNonNull(schema);
    Objects.requireNonNull(partitions);
    Objects.requireNonNull(properties);
    withErrorHandling(
        () -> {
          UCDeltaModels.StagingTableInfo stagingTableInfo =
              deltaClient.createStagingTable(toStorageTableIdentifier(tableId));
          String storageLocation = Objects.requireNonNull(stagingTableInfo.getLocation());
          Map<String, String> requiredProperties =
              requiredCreateProperties(tableId, properties, stagingTableInfo);
          callback.accept(
              new TableDescriptor(
                  tableId,
                  stagingTableInfo.getTableId().toString(),
                  URI.create(storageLocation),
                  stagingTableInfo.getStorageProperties(),
                  requiredProperties));
          return null;
        });
  }

  /**
   * Retrieves temporary credentials required to access the underlying storage for the given table.
   *
   * <p>This implementation requests short-lived credentials from the catalog service that are
   * scoped to the specified table and operation type (read/write).
   *
   * <p>The returned Hadoop configuration uses the renewable credential providers supplied by {@code
   * unitycatalog-hadoop}.
   *
   * <p>These providers use filesystem-specific expiration settings, so {@link CredentialManager}
   * does not schedule another refresh for UC credentials. Credential renewal is handled by {@code
   * unitycatalog-hadoop}; {@code renewCredential.enabled} defaults to {@code true}.
   *
   * @param tableName the three-part table name for which credentials are requested
   * @return a map of filesystem configuration properties containing temporary credentials
   * @throws RuntimeException if credential generation fails due to API or network errors
   */
  @Override
  public Map<String, String> getCredentials(String tableName) {
    Objects.requireNonNull(tableName);
    return withErrorHandling(
        () -> deltaClient.loadTable(toStorageTableIdentifier(tableName)).getStorageProperties());
  }

  /**
   * Reports post-commit metrics for the given table to Unity Catalog via the Delta-Tables API.
   *
   * @param tableName the {@code catalog.schema.table} identifier
   * @param tableId the table's UC UUID
   * @param report the per-commit metrics payload
   */
  public void reportMetrics(String tableName, String tableId, UCDeltaModels.CommitReport report) {
    withErrorHandling(
        () -> {
          deltaClient.reportMetrics(tableId, toStorageTableIdentifier(tableName), report);
          return null;
        });
  }

  /* ==============================================================================
   *  Catalog browsing (SHOW DATABASES / SHOW TABLES) for the Flink SQL catalog.
   *
   *  These stay on the UC OpenAPI client (SchemasApi / TablesApi): the UC
   *  Delta-Tables client used everywhere else only serves per-table data-plane
   *  operations (load, stage, create, commit, metrics) and exposes no
   *  schema/table listing. Migrate these once that listing is available.
   * ============================================================================== */

  public List<String> listSchemas() {
    return withErrorHandling(
        () -> {
          SchemasApi schemasApi = new SchemasApi(apiClient);
          return schemasApi.listSchemas(this.name, Integer.MAX_VALUE, null).getSchemas().stream()
              .map(SchemaInfo::getName)
              .collect(Collectors.toList());
        });
  }

  public SchemaInfo getSchema(String name) {
    return withErrorHandling(
        () -> {
          SchemasApi schemasApi = new SchemasApi(apiClient);
          return schemasApi.getSchema(String.format("%s.%s", getName(), name));
        });
  }

  public List<String> listTables(String schema) {
    return withErrorHandling(
        () -> {
          TablesApi tablesApi = new TablesApi(apiClient);
          return tablesApi.listTables(getName(), schema, Integer.MAX_VALUE, "").getTables().stream()
              .map(TableInfo::getName)
              .collect(Collectors.toList());
        });
  }

  public TableInfo getTableDetail(String tableId) {
    return withErrorHandling(
        () -> {
          TablesApi tablesApi = new TablesApi(apiClient);
          return tablesApi.getTable(tableId, false, false);
        });
  }
}
