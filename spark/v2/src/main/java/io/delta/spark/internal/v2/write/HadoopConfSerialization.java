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
package io.delta.spark.internal.v2.write;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * POC helper: serialize Hadoop {@link Configuration} as {@link Map}{@code <String,String>} so it
 * can be sent to executors. {@link Configuration} is not {@link java.io.Serializable}, but
 * DataWriterFactory is serialized when Spark sends it to workers.
 *
 * <p><b>Choice (documented):</b> We copy all config entries into a Map and reconstruct on the
 * executor. Alternatives: (1) Obtain Hadoop config from Spark task context on the executor if Spark
 * exposes it — would avoid copying but depends on Spark internals; (2) use Kryo/special
 * serialization for Configuration — adds dependency and complexity. For the prototype we use
 * Map-based copy; sufficient for e2e and local/standalone tests.
 */
public final class HadoopConfSerialization {

  private HadoopConfSerialization() {}

  /**
   * Copy all key-value pairs from the configuration into a new map. The returned map is
   * serializable and can be sent to executors.
   */
  public static Map<String, String> toMap(Configuration conf) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  /**
   * Build a new Configuration from a map. Used on the executor to recreate config from the
   * serialized map.
   */
  public static Configuration fromMap(Map<String, String> map) {
    Configuration conf = new Configuration(false);
    if (map != null) {
      for (Map.Entry<String, String> e : map.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
    }
    return conf;
  }
}
