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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/** Internal class - not intended for direct use. */
class StaticTokenProvider implements TokenProvider {
  private String token;

  @Override
  public void initialize(Map<String, String> configs) {
    String token = configs.get(AuthConfigs.STATIC_TOKEN);
    Preconditions.checkArgument(
        token != null, "Configuration key '%s' is missing or empty", AuthConfigs.STATIC_TOKEN);
    this.token = token;
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> configs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE_VALUE);
    configs.put(AuthConfigs.STATIC_TOKEN, token);
    return configs;
  }
}
