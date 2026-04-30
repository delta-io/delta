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

import io.unitycatalog.client.internal.Preconditions;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class AuthConfigUtils {
  private static final String AUTH_PREFIX = "auth.";
  private static final String TYPE = "type";
  private static final String STATIC_TYPE = "static";
  private static final String STATIC_TOKEN = "token";

  private AuthConfigUtils() {}

  public static Map<String, String> buildAuthConfigs(Map<String, String> configs) {
    Map<String, String> newConfigs = new HashMap<>();

    for (Map.Entry<String, String> e : configs.entrySet()) {
      if (e.getKey().startsWith(AuthConfigUtils.AUTH_PREFIX)) {
        String newKey = e.getKey().substring(AuthConfigUtils.AUTH_PREFIX.length()).trim();
        if (!newKey.isEmpty()) {
          newConfigs.put(newKey, e.getValue());
        }
      }
    }

    String token = configs.get(AuthConfigUtils.STATIC_TOKEN);
    if (token != null) {
      Preconditions.checkArgument(
          !newConfigs.containsKey(AuthConfigUtils.STATIC_TOKEN),
          "Static token was configured twice, choose only one: 'token' (legacy) or "
              + "'auth.token' (new-style).");

      newConfigs.put(TYPE, STATIC_TYPE);
      newConfigs.put(STATIC_TOKEN, token);
    }

    return new CaseInsensitiveStringMap(newConfigs);
  }
}
