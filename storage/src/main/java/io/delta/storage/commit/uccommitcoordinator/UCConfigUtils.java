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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Shared utility methods for parsing flat UC configuration maps (e.g. auth, appVersions).
 * Used by both the Scala factory ({@code UCTokenBasedRestClientFactory}) and the Java
 * clients ({@link UCDeltaTokenBasedRestClient}, {@link UCTokenBasedRestClient}).
 */
public final class UCConfigUtils {

  public static final String URI_KEY = "uri";
  public static final String AUTH_PREFIX = "auth.";
  public static final String APP_VERSIONS_PREFIX = "appVersions.";

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
   * Extracts authentication configuration from a flat ucConfig map.
   * Prefers {@code auth.*} keys; falls back to legacy {@code token} key.
   *
   * @param ucConfig the flat configuration map.
   * @return a map suitable for {@code TokenProvider.create}, with the {@code auth.} prefix stripped.
   */
  public static Map<String, String> extractAuthConfig(Map<String, String> ucConfig) {
    String authPrefixLower = AUTH_PREFIX.toLowerCase(Locale.ROOT);
    Map<String, String> authConfig = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : ucConfig.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).startsWith(authPrefixLower)) {
        String suffix = entry.getKey().substring(authPrefixLower.length());
        authConfig.put(suffix, entry.getValue());
      }
    }
    if (!authConfig.isEmpty()) {
      return authConfig;
    }
    String token = ucConfig.get("token");
    if (token != null) {
      Map<String, String> legacy = new LinkedHashMap<>();
      legacy.put("type", "static");
      legacy.put("token", token);
      return legacy;
    }
    return Collections.emptyMap();
  }

  /**
   * Returns {@code true} if the ucConfig contains any authentication configuration
   * ({@code auth.*} keys or legacy {@code token} key).
   */
  public static boolean hasAuthConfig(Map<String, String> ucConfig) {
    String authPrefixLower = AUTH_PREFIX.toLowerCase(Locale.ROOT);
    for (String key : ucConfig.keySet()) {
      if (key.toLowerCase(Locale.ROOT).startsWith(authPrefixLower)) {
        return true;
      }
    }
    return ucConfig.containsKey("token");
  }

  /**
   * Extracts {@code appVersions.*} entries from a flat ucConfig map, stripping the prefix.
   *
   * @param ucConfig the flat configuration map.
   * @return a map of application name to version string.
   */
  public static Map<String, String> extractAppVersions(Map<String, String> ucConfig) {
    String appPrefixLower = APP_VERSIONS_PREFIX.toLowerCase(Locale.ROOT);
    Map<String, String> versions = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : ucConfig.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).startsWith(appPrefixLower)) {
        versions.put(entry.getKey().substring(appPrefixLower.length()), entry.getValue());
      }
    }
    return versions;
  }

  /**
   * Parses a boolean config value with a default fallback.
   */
  public static boolean parseBoolean(
      Map<String, String> ucConfig, String key, boolean defaultValue) {
    String value = ucConfig.get(key);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }
}
