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
package io.delta.spark.internal.v2.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.uccommitcoordinator.UCConfigUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Table information for Unity Catalog managed tables.
 *
 * <p>This POJO encapsulates all the information needed to interact with a Unity Catalog table
 * without requiring Spark dependencies.
 */
public final class UCTableInfo {

  private final String tableId;
  private final String tablePath;
  private final UCTableIdentifier tableIdentifier;
  private final String ucUri;
  private final Map<String, String> authConfig;
  private final Map<String, String> optInFlags;

  public UCTableInfo(
      String tableId,
      String tablePath,
      UCTableIdentifier tableIdentifier,
      String ucUri,
      Map<String, String> authConfig) {
    this(tableId, tablePath, tableIdentifier, ucUri, authConfig, Collections.emptyMap());
  }

  public UCTableInfo(
      String tableId,
      String tablePath,
      UCTableIdentifier tableIdentifier,
      String ucUri,
      Map<String, String> authConfig,
      Map<String, String> optInFlags) {
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.tableIdentifier = requireNonNull(tableIdentifier, "tableIdentifier is null");
    this.ucUri = requireNonNull(ucUri, "ucUri is null");
    this.authConfig = Collections.unmodifiableMap(requireNonNull(authConfig, "authConfig is null"));
    this.optInFlags = Collections.unmodifiableMap(requireNonNull(optInFlags, "optInFlags is null"));
  }

  public String getTableId() {
    return tableId;
  }

  public String getTablePath() {
    return tablePath;
  }

  public UCTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getUcUri() {
    return ucUri;
  }

  public Map<String, String> getAuthConfig() {
    return authConfig;
  }

  public Map<String, String> getOptInFlags() {
    return optInFlags;
  }

  /**
   * Builds a flat config map suitable for {@code UCTokenBasedRestClientFactory.createUCClient}.
   * Re-adds the {@code auth.} prefix to auth config keys, includes {@code uri}, and forwards any
   * per-catalog opt-in flags ({@link UCConfigUtils#DELTA_REST_API_ENABLED_KEY} etc.) so the factory
   * picks the correct client implementation.
   */
  public Map<String, String> toUcConfig() {
    Map<String, String> ucConfig = new HashMap<>();
    ucConfig.put(UCConfigUtils.URI_KEY, ucUri);
    authConfig.forEach((k, v) -> ucConfig.put(UCConfigUtils.AUTH_PREFIX + k, v));
    ucConfig.putAll(optInFlags);
    return ucConfig;
  }
}
