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

package io.delta.flink.sink;

import io.delta.flink.table.*;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

/**
 * A builder for constructing {@link DeltaTable} instances with various backends.
 *
 * <p>The {@code TableBuilder} provides a fluent API for creating Delta tables with support for
 * multiple table types:
 *
 * <ul>
 *   <li><b>Hadoop</b>: File-based tables using Hadoop filesystem APIs
 *   <li><b>Unity Catalog</b>: Catalog-managed tables with Unity Catalog integration
 *   <li><b>UC Path</b>: Path-based access to Unity Catalog tables with credential management
 * </ul>
 *
 * <p><b>Usage Examples:</b>
 *
 * <pre>{@code
 * // Hadoop file-based table
 * DeltaTable hadoopTable = new TableBuilder()
 *     .withTablePath("/path/to/delta/table")
 *     .withSchema(schema)
 *     .withPartitionColNames(Arrays.asList("year", "month"))
 *     .build();
 *
 * // Unity Catalog managed table with token authentication
 * DeltaTable ucTable = new TableBuilder()
 *     .withTableName("catalog.schema.table")
 *     .withEndpoint("https://unity-catalog.example.com")
 *     .withToken("dapi123...")
 *     .withSchema(schema)
 *     .build();
 *
 * // Unity Catalog with OAuth authentication
 * DeltaTable ucOAuthTable = new TableBuilder()
 *     .withTableName("catalog.schema.table")
 *     .withEndpoint("https://unity-catalog.example.com")
 *     .withOauthUri("https://oauth.example.com/token")
 *     .withOauthClientId("client-id")
 *     .withOauthClientSecret("client-secret")
 *     .build();
 *
 * // Configuration-based setup
 * Map<String, String> config = Map.of(
 *     "type", "unitycatalog",
 *     "unitycatalog.table_name", "catalog.schema.table",
 *     "unitycatalog.endpoint", "https://unity-catalog.example.com",
 *     "unitycatalog.token", "dapi123..."
 * );
 * DeltaTable configTable = new TableBuilder()
 *     .withConfigurations(config)
 *     .withSchema(schema)
 *     .build();
 * }</pre>
 *
 * <p>The builder supports both programmatic configuration via fluent methods and bulk configuration
 * via a configuration map. When using {@link #withConfigurations(Map)}, the map can contain any of
 * the supported configuration options defined as constants in this class.
 */
public class TableBuilder {

  private enum TableType {
    /** File-based table using Hadoop filesystem APIs. */
    hadoop,
    /** Catalog-managed table with Unity Catalog. */
    unitycatalog,
    /** Path-based table access with Unity Catalog credentials. */
    ucpath
  }
  // ----------------------------------------------------------------------
  // Configuration Options
  // ----------------------------------------------------------------------

  private static final ConfigOption<TableType> TYPE =
      ConfigOptions.key("type").enumType(TableType.class).defaultValue(TableType.hadoop);

  private static final ConfigOption<String> HADOOP_TABLE_PATH =
      ConfigOptions.key("hadoop.table_path").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_NAME =
      ConfigOptions.key("unitycatalog.name").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_TABLE_NAME =
      ConfigOptions.key("unitycatalog.table_name").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_ENDPOINT =
      ConfigOptions.key("unitycatalog.endpoint").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_TOKEN =
      ConfigOptions.key("unitycatalog.token").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_OAUTH_URI =
      ConfigOptions.key("unitycatalog.oauth.uri").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_OAUTH_CLIENT_ID =
      ConfigOptions.key("unitycatalog.oauth.client_id").stringType().noDefaultValue();

  private static final ConfigOption<String> UNITYCATALOG_OAUTH_CLIENT_SECRET =
      ConfigOptions.key("unitycatalog.oauth.client_secret").stringType().noDefaultValue();

  private TableType tableType = TableType.hadoop;
  private StructType schema;
  private List<String> partitionColNames;
  private Map<String, String> configurations;
  // For file-based tables
  private String tablePath;
  // For catalog-based tables
  private String catalogName = "main";
  private String tableName;
  // UC configuration
  private URI endpoint;
  private String token;
  private URI oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;

  public TableBuilder() {
    this.configurations = new HashMap<>();
  }

  public TableBuilder withTablePath(String tablePath) {
    this.tablePath = tablePath;
    this.tableType = TableType.hadoop;
    return this;
  }

  public TableBuilder withSchema(RowType flinkSchema) {
    this.schema = Conversions.FlinkToDelta.schema(flinkSchema);
    return this;
  }

  public TableBuilder withSchema(StructType schema) {
    this.schema = schema;
    return this;
  }

  public TableBuilder withPartitionColNames(List<String> partitionColNames) {
    this.partitionColNames = partitionColNames;
    return this;
  }

  // For catalog-based tables
  public TableBuilder withTableName(String tableName) {
    this.tableName = tableName;
    this.tableType = TableType.unitycatalog;
    return this;
  }

  public TableBuilder withCatalogName(String catalogName) {
    this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    return this;
  }

  public TableBuilder withEndpoint(String catalogEndpoint) {
    this.endpoint = URI.create(catalogEndpoint);
    return this;
  }

  public TableBuilder withToken(String catalogToken) {
    this.token = catalogToken;
    return this;
  }

  public TableBuilder withOauthUri(String oauthUri) {
    this.oauthUri = URI.create(oauthUri);
    return this;
  }

  public TableBuilder withOauthClientId(String oauthClientId) {
    this.oauthClientId = oauthClientId;
    return this;
  }

  public TableBuilder withOauthClientSecret(String oauthClientSecret) {
    this.oauthClientSecret = oauthClientSecret;
    return this;
  }

  public TableBuilder withConfigurations(Map<String, String> configurations) {
    this.configurations.clear();
    this.configurations.putAll(configurations);
    Configuration extract = Configuration.fromMap(configurations);

    // Extract everything from configurations
    tableType = extract.get(TYPE, TableType.hadoop);
    tablePath = extract.get(HADOOP_TABLE_PATH, tablePath);
    catalogName = extract.get(UNITYCATALOG_NAME, "main");
    tableName = extract.get(UNITYCATALOG_TABLE_NAME, tableName);

    String endpoint = extract.get(UNITYCATALOG_ENDPOINT, null);
    if (Objects.nonNull(endpoint)) {
      this.endpoint = URI.create(endpoint);
    }
    token = extract.get(UNITYCATALOG_TOKEN, token);
    String oauthUriStr = extract.get(UNITYCATALOG_OAUTH_URI, null);
    if (Objects.nonNull(oauthUriStr)) {
      oauthUri = URI.create(oauthUriStr);
    }
    this.oauthClientId = extract.get(UNITYCATALOG_OAUTH_CLIENT_ID, null);
    this.oauthClientSecret = extract.get(UNITYCATALOG_OAUTH_CLIENT_SECRET, null);

    return this;
  }

  private DeltaCatalog createUnityCatalog() {
    Preconditions.checkArgument(endpoint != null);
    Preconditions.checkArgument(!(token == null && oauthUri == null));
    if (token != null) {
      return new UnityCatalog(catalogName, endpoint, token);
    } else {
      return new UnityCatalog(catalogName, endpoint, oauthUri, oauthClientId, oauthClientSecret);
    }
  }

  private CatalogManagedTable createCatalogManagedTable(DeltaCatalog catalog) {
    if (oauthUri != null) {
      return new CatalogManagedTable(
          catalog,
          tableName,
          configurations,
          schema,
          partitionColNames,
          endpoint,
          oauthUri,
          oauthClientId,
          oauthClientSecret);
    } else {
      return new CatalogManagedTable(
          catalog, tableName, configurations, schema, partitionColNames, endpoint, token);
    }
  }

  public DeltaTable build() {
    switch (tableType) {
      case hadoop:
        {
          Objects.requireNonNull(tablePath);
          return new HadoopTable(URI.create(tablePath), configurations, schema, partitionColNames);
        }
      case unitycatalog:
        {
          Objects.requireNonNull(endpoint);
          Preconditions.checkArgument(
              !(token == null && oauthUri == null),
              "Either token or OAuth config must be provided");
          DeltaCatalog restCatalog = createUnityCatalog();
          return createCatalogManagedTable(restCatalog);
        }
      case ucpath:
        {
          Objects.requireNonNull(endpoint);
          Preconditions.checkArgument(
              !(token == null && oauthUri == null),
              "Either token or OAuth config must be provided");
          DeltaCatalog restCatalog = createUnityCatalog();
          return new HadoopTable(restCatalog, tableName, configurations, schema, partitionColNames);
        }
      default:
        throw new IllegalArgumentException("unreachable");
    }
  }
}
