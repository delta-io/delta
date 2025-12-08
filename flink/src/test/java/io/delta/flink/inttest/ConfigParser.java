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

package io.delta.flink.inttest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ConfigParser {

  public static class TestConfig {
    public URI workspace;
    public String token;
    public String clusterId;
  }

  public static TestConfig fromArgs(String[] args) {
    Map<String, String> parsed = new HashMap<>();

    for (String arg : args) {
      if (!arg.startsWith("--") || !arg.contains("=")) {
        throw new IllegalArgumentException("Invalid argument format: " + arg);
      }

      String[] parts = arg.substring(2).split("=", 2);
      parsed.put(parts[0], parts[1]);
    }

    TestConfig config = new TestConfig();

    try {
      if (parsed.containsKey("workspace")) {
        config.workspace = new URI(parsed.get("workspace"));
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid workspace URI", e);
    }

    config.token = parsed.get("token");
    config.clusterId = parsed.get("clusterId");

    // Optional: validate required fields
    if (config.workspace == null || config.token == null || config.clusterId == null) {
      throw new IllegalArgumentException(
          "Missing required arguments: --workspace, --token, --clusterId");
    }

    return config;
  }
}
