/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Singleton configuration loader for delta-flink.properties. */
public final class Conf {

  public static String SINK_RETRY_MAX_ATTEMPT = "sink.retry.max-attempt";
  public static String SINK_RETRY_DELAY_MS = "sink.retry.delay-ms";
  public static String SINK_RETRY_MAX_DELAY_MS = "sink.retry.max-delay-ms";

  public static String TABLE_HADOOP_CACHE_ENABLE = "table.hadoop.cache.enable";
  public static String TABLE_HADOOP_CACHE_SIZE = "table.hadoop.cache.size";
  public static String TABLE_HADOOP_CACHE_EXPIRE_MS = "table.hadoop.cache.expire-ms";

  public static String CREDENTIALS_REFRESH_THREAD_POOL_SIZE =
      "credentials.refresh.thread-pool-size";
  public static String CREDENTIALS_REFRESH_AHEAD_MS = "credentials.refresh.ahead-ms";

  private static final String CONFIG_FILE = "delta-flink.properties";

  private final Map<String, String> props;

  private Conf() {
    this.props = load();
  }

  /*================
   * Confs
   *================*/

  public int getSinkRetryMaxAttempt() {
    return Integer.parseInt(getOrDefault(SINK_RETRY_MAX_ATTEMPT, "4"));
  }

  public long getSinkRetryDelayMs() {
    return Long.parseLong(getOrDefault(SINK_RETRY_DELAY_MS, "200"));
  }

  public long getSinkRetryMaxDelayMs() {
    return Long.parseLong(getOrDefault(SINK_RETRY_MAX_DELAY_MS, "20000"));
  }

  public boolean getHadoopTableCacheEnable() {
    return Boolean.parseBoolean(getOrDefault(TABLE_HADOOP_CACHE_ENABLE, "true"));
  }

  public int getHadoopTableCacheSize() {
    return Integer.parseInt(getOrDefault(TABLE_HADOOP_CACHE_SIZE, "100"));
  }

  public long getHadoopTableCacheExpireInMs() {
    return Long.parseLong(getOrDefault(TABLE_HADOOP_CACHE_EXPIRE_MS, "300000"));
  }

  public int getCredentialsRefreshThreadPoolSize() {
    return Integer.parseInt(getOrDefault(CREDENTIALS_REFRESH_THREAD_POOL_SIZE, "10"));
  }

  public long getCredentialsRefreshAheadInMs() {
    return Long.parseLong(getOrDefault(CREDENTIALS_REFRESH_AHEAD_MS, "60000"));
  }

  private static final Conf INSTANCE = new Conf();

  public static Conf getInstance() {
    return INSTANCE;
  }

  /** Returns an immutable view of all configuration entries. */
  public Map<String, String> asMap() {
    return props;
  }

  /** Returns a configuration value or null if missing. */
  public String get(String key) {
    return props.get(key);
  }

  /** Returns a configuration value or default if missing. */
  public String getOrDefault(String key, String defaultValue) {
    return props.getOrDefault(key, defaultValue);
  }

  // ----------------- internals -----------------

  // For debug purpose
  private URL sourcePath;

  private Map<String, String> load() {
    Properties p = new Properties();
    try (InputStream in = Conf.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
      if (in == null) {
        return Map.of();
      }
      sourcePath = Conf.class.getClassLoader().getResource(CONFIG_FILE);
      p.load(in);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load " + CONFIG_FILE, e);
    }
    Map<String, String> map = new HashMap<>();
    for (String name : p.stringPropertyNames()) {
      map.put(name, p.getProperty(name));
    }
    return Collections.unmodifiableMap(map);
  }
}
