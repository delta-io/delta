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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;

/** Serializable snapshot of Hadoop configuration for executor-side reconstruction. */
public class SerializableHadoopConf implements Serializable {
  private final Map<String, String> entries;

  public SerializableHadoopConf(Configuration configuration) {
    requireNonNull(configuration, "configuration is null");
    Map<String, String> snapshot = new HashMap<>();
    for (Entry<String, String> entry : configuration) {
      String key = requireNonNull(entry.getKey(), "configuration key is null");
      String value =
          requireNonNull(entry.getValue(), "configuration value is null for key: " + key);
      snapshot.put(key, value);
    }
    this.entries = Collections.unmodifiableMap(snapshot);
  }

  public Configuration toConfiguration() {
    Configuration configuration = new Configuration(false);
    entries.forEach(configuration::set);
    return configuration;
  }

  Map<String, String> getEntries() {
    return entries;
  }
}
