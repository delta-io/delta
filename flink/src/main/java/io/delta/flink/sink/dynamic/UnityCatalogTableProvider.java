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

package io.delta.flink.sink.dynamic;

import io.delta.flink.sink.TableBuilder;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link DeltaTableProvider} backed by {@link io.delta.flink.table.UnityCatalog} and {@link
 * io.delta.flink.table.CatalogManagedTable}, built via {@link TableBuilder}.
 *
 * <p>Each {@code tableName} from the stream must be a Unity Catalog table identifier (typically
 * {@code catalog.schema.table}), matching {@link TableBuilder#withTableName(String)}.
 */
public final class UnityCatalogTableProvider implements DeltaTableProvider {

  private final String unityCatalogName;
  private final URI endpoint;
  private final Map<String, String> tableConfigurations;
  private final String token;
  private final URI oauthUri;
  private final String oauthClientId;
  private final String oauthClientSecret;

  private UnityCatalogTableProvider(
      String unityCatalogName,
      URI endpoint,
      Map<String, String> tableConfigurations,
      String token,
      URI oauthUri,
      String oauthClientId,
      String oauthClientSecret) {
    this.unityCatalogName = Objects.requireNonNull(unityCatalogName, "unityCatalogName");
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.tableConfigurations =
        new HashMap<>(Objects.requireNonNull(tableConfigurations, "tableConfigurations"));
    this.token = token;
    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
  }

  /**
   * Provider using static bearer token authentication to the Unity Catalog endpoint.
   *
   * @param unityCatalogName catalog name (e.g. {@code main})
   * @param endpoint REST base URI for Unity Catalog
   * @param token bearer token
   * @param tableConfigurations extra options (same keys as {@link
   *     TableBuilder#withConfigurations(Map)}); must not set {@code unitycatalog.table_name}, which
   *     comes from each {@link #getOrCreate} call.
   */
  public static UnityCatalogTableProvider forToken(
      String unityCatalogName,
      URI endpoint,
      String token,
      Map<String, String> tableConfigurations) {
    Objects.requireNonNull(token, "token");
    return new UnityCatalogTableProvider(
        unityCatalogName, endpoint, tableConfigurations, token, null, null, null);
  }

  /**
   * Provider using OAuth client credentials against the Unity Catalog endpoint.
   *
   * @param unityCatalogName catalog name (e.g. {@code main})
   * @param endpoint REST base URI for Unity Catalog
   * @param oauthUri OAuth token endpoint
   * @param oauthClientId client id
   * @param oauthClientSecret client secret
   * @param tableConfigurations extra options (same keys as {@link
   *     TableBuilder#withConfigurations(Map)})
   */
  public static UnityCatalogTableProvider forOAuth(
      String unityCatalogName,
      URI endpoint,
      URI oauthUri,
      String oauthClientId,
      String oauthClientSecret,
      Map<String, String> tableConfigurations) {
    Objects.requireNonNull(oauthUri, "oauthUri");
    Objects.requireNonNull(oauthClientId, "oauthClientId");
    Objects.requireNonNull(oauthClientSecret, "oauthClientSecret");
    return new UnityCatalogTableProvider(
        unityCatalogName,
        endpoint,
        tableConfigurations,
        null,
        oauthUri,
        oauthClientId,
        oauthClientSecret);
  }

  @Override
  public DeltaTable getOrCreate(
      String tableName, StructType schema, List<String> partitionColumns) {
    Objects.requireNonNull(tableName, "tableName");
    List<String> parts = partitionColumns == null ? List.of() : partitionColumns;
    TableBuilder builder =
        new TableBuilder()
            .withConfigurations(new HashMap<>(tableConfigurations))
            .withCatalogName(unityCatalogName)
            .withTableName(tableName)
            .withEndpoint(endpoint.toString())
            .withSchema(schema)
            .withPartitionColNames(parts);
    if (token != null) {
      builder.withToken(token);
    } else {
      builder
          .withOauthUri(oauthUri.toString())
          .withOauthClientId(oauthClientId)
          .withOauthClientSecret(oauthClientSecret);
    }
    DeltaTable table = builder.build();
    table.open();
    return table;
  }
}
