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

package io.delta.storage.commit.uccommitcoordinator;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared utility methods for parsing flat UC configuration maps (e.g. auth, appVersions).
 * Used by both the Scala factory ({@code UCTokenBasedRestClientFactory}) and the Java
 * client ({@link UCDeltaTokenBasedRestClient}).
 *
 * <p>Config key names are the single source of truth and must stay aligned with OSS Unity Catalog.
 */
public final class UCConfigUtils {

  public static final String URI_KEY = "uri";
  public static final String AUTH_PREFIX = "auth.";
  public static final String APP_VERSIONS_PREFIX = "appVersions.";
  public static final String DELTA_REST_API_ENABLED_KEY = "deltaRestApi.enabled";
  public static final String RENEW_CREDENTIAL_ENABLED_KEY = "renewCredential.enabled";
  public static final String CRED_SCOPED_FS_ENABLED_KEY = "credScopedFs.enabled";

  /** Legacy top-level token key (without {@code auth.} prefix). */
  public static final String LEGACY_TOKEN_KEY = "token";
  /** Auth config key for the token provider type. */
  public static final String AUTH_TYPE_KEY = "type";
  /** Static token provider type value for legacy {@code token} configs. */
  public static final String STATIC_AUTH_TYPE = "static";

  private UCConfigUtils() {}

  /**
   * Extracts the required {@code uri} value from a flat ucConfig map.
   *
   * @throws IllegalArgumentException if {@code uri} is not present.
   */
  public static String extractUri(Map<String, String> ucConfig) {
    String uri = ucConfig.get(URI_KEY);
    if (uri == null) {
      throw new IllegalArgumentException("UC config must contain '" + URI_KEY + "'");
    }
    return uri;
  }

  /**
   * Extracts authentication configuration from a flat ucConfig map, preserving the original key
   * casing (e.g. {@code oauth.clientId}). Prefers {@code auth.*} keys; falls back to the legacy
   * {@code token} key.
   *
   * <p>Keys are matched case-sensitively against the {@code auth.} prefix, consistent with the rest
   * of the UC Delta catalog client path. The {@code auth.} prefix is stripped while the suffix
   * casing is preserved so downstream lookups (e.g. {@code oauth.clientId}) match exactly.
   *
   * @param ucConfig the flat configuration map.
   * @return a map suitable for {@code TokenProvider.create}, with the {@code auth.} prefix stripped.
   */
  public static Map<String, String> extractCaseSensitiveAuthConfig(Map<String, String> ucConfig) {
    Map<String, String> authConfig = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : ucConfig.entrySet()) {
      if (entry.getKey().startsWith(AUTH_PREFIX)) {
        authConfig.put(entry.getKey().substring(AUTH_PREFIX.length()), entry.getValue());
      }
    }
    if (!authConfig.isEmpty()) {
      return authConfig;
    }
    String token = ucConfig.get(LEGACY_TOKEN_KEY);
    if (token != null) {
      return Map.of(AUTH_TYPE_KEY, STATIC_AUTH_TYPE, LEGACY_TOKEN_KEY, token);
    }
    return Map.of();
  }

  /**
   * Returns {@code true} if the ucConfig contains any authentication configuration
   * ({@code auth.*} keys or legacy {@code token} key). Keys are matched case-sensitively.
   */
  public static boolean hasAuthConfig(Map<String, String> ucConfig) {
    for (String key : ucConfig.keySet()) {
      if (key.startsWith(AUTH_PREFIX)) {
        return true;
      }
    }
    return ucConfig.containsKey(LEGACY_TOKEN_KEY);
  }

  /**
   * Extracts {@code appVersions.*} entries from a flat ucConfig map, stripping the prefix. Keys are
   * matched case-sensitively.
   *
   * @param ucConfig the flat configuration map.
   * @return a map of application name to version string.
   */
  public static Map<String, String> extractAppVersions(Map<String, String> ucConfig) {
    Map<String, String> versions = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : ucConfig.entrySet()) {
      if (entry.getKey().startsWith(APP_VERSIONS_PREFIX)) {
        versions.put(entry.getKey().substring(APP_VERSIONS_PREFIX.length()), entry.getValue());
      }
    }
    return versions;
  }

  /**
   * Returns whether the Delta REST API client should be used. Defaults to {@code true} when the
   * key is absent.
   */
  public static boolean isDeltaRestApiEnabled(Map<String, String> ucConfig) {
    return parseBoolean(ucConfig, DELTA_REST_API_ENABLED_KEY, true);
  }

  /**
   * Returns whether credential renewal is enabled. Defaults to {@code true} when the key is absent.
   */
  public static boolean isCredentialRenewalEnabled(Map<String, String> ucConfig) {
    return parseBoolean(ucConfig, RENEW_CREDENTIAL_ENABLED_KEY, true);
  }

  /**
   * Returns whether credential-scoped filesystem access is enabled. Defaults to {@code true} when
   * the key is absent.
   */
  public static boolean isCredentialScopedFsEnabled(Map<String, String> ucConfig) {
    return parseBoolean(ucConfig, CRED_SCOPED_FS_ENABLED_KEY, true);
  }

  /**
   * Parses a boolean config value with a default fallback. Only {@code "true"} / {@code "false"}
   * (case-insensitive) are accepted; any other value throws so misconfigured keys surface loudly
   * rather than silently falling back.
   *
   * @throws IllegalArgumentException if the value is present but not a valid boolean.
   */
  public static boolean parseBoolean(
      Map<String, String> ucConfig, String key, boolean defaultValue) {
    String value = ucConfig.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value.equalsIgnoreCase("true")) {
      return true;
    }
    if (value.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid boolean value for config '" + key + "': '" + value + "'");
  }
}
