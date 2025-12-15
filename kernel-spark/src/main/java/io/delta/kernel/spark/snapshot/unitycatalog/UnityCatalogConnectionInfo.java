/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;

/**
 * Connection information for Unity Catalog managed tables.
 *
 * <p>This POJO encapsulates all the information needed to connect to a Unity Catalog table without
 * requiring Spark dependencies.
 */
public final class UnityCatalogConnectionInfo {
  private final String tableId;
  private final String tablePath;
  private final String endpoint;
  private final String token;

  public UnityCatalogConnectionInfo(
      String tableId, String tablePath, String endpoint, String token) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.endpoint = requireNonNull(endpoint, "endpoint is null");
    this.token = requireNonNull(token, "token is null");
  }

  public String getTableId() {
    return tableId;
  }

  public String getTablePath() {
    return tablePath;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getToken() {
    return token;
  }
}
