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
package io.delta.kernel.spark.utils;

import static java.util.Objects.requireNonNull;

/**
 * Immutable data class holding Unity Catalog configuration extracted from Spark catalog settings.
 *
 * <p>Contains the catalog name, URI, and authentication token needed to communicate with a Unity
 * Catalog instance.
 */
public final class UCCatalogConfig {
  private final String catalogName;
  private final String uri;
  private final String token;

  /**
   * Creates a new Unity Catalog configuration.
   *
   * @param catalogName the catalog name (e.g., "unity")
   * @param uri the Unity Catalog server URI (e.g., "https://uc-server:8080")
   * @param token the authentication token
   * @throws NullPointerException if any parameter is null
   */
  public UCCatalogConfig(String catalogName, String uri, String token) {
    this.catalogName = requireNonNull(catalogName, "catalogName is null");
    this.uri = requireNonNull(uri, "uri is null");
    this.token = requireNonNull(token, "token is null");
  }

  /** @return the catalog name */
  public String getCatalogName() {
    return catalogName;
  }

  /** @return the Unity Catalog server URI */
  public String getUri() {
    return uri;
  }

  /** @return the authentication token */
  public String getToken() {
    return token;
  }

  @Override
  public String toString() {
    return "UCCatalogConfig{"
        + "catalogName='"
        + catalogName
        + '\''
        + ", uri='"
        + uri
        + '\''
        + ", token='***'"
        + '}';
  }
}
