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

package io.delta.flink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global configuration for Delta Flink sinks.
 *
 * <p>This class loads process-wide configuration from the {@code delta-flink.properties} file and
 * exposes shared settings that apply to <em>all</em> {@code DeltaSink} instances within the JVM.
 * These configurations are intended for operational tuning and common defaults, such as retry
 * behavior, thread pool sizing, caching, and credential refresh.
 *
 * <p>The configuration is loaded once and managed as a singleton. All sinks created in the same
 * process observe the same global configuration values.
 *
 * <p>Per-sink or per-table behavior should be configured explicitly when constructing the {@code
 * DeltaSink} instance. Sink-level configuration takes precedence over global defaults defined here.
 *
 * <p>This class is not intended to be instantiated or mutated directly by users.
 */
public final class Conf {

  private static final Logger LOG = LoggerFactory.getLogger(Conf.class);

  public static final String SINK_RETRY_MAX_ATTEMPT = "sink.retry.max-attempt";
  // The i-th retry will have a delay of `delay-ms * (2 ^ i)`
  public static final String SINK_RETRY_DELAY_MS = "sink.retry.delay-ms";
  // Retry will stop if the delay exceeds max-delay
  public static final String SINK_RETRY_MAX_DELAY_MS = "sink.retry.max-delay-ms";

  public static final String SINK_WRITER_NUM_CONCURRENT_FILE = "sink.writer.num-concurrent-file";

  public static final String TABLE_THREAD_POOL_SIZE = "table.thread-pool-size";

  public static final String TABLE_CACHE_ENABLE = "table.cache.enable";
  public static final String TABLE_CACHE_SIZE = "table.cache.size";
  public static final String TABLE_CACHE_EXPIRE_MS = "table.cache.expire-ms";

  public static final String DELTA_CHECKPOINT_FREQUENCY = "delta.checkpoint.frequency";

  public static final String CREDENTIALS_REFRESH_THREAD_POOL_SIZE =
      "credentials.refresh.thread-pool-size";
  public static final String CREDENTIALS_REFRESH_AHEAD_MS = "credentials.refresh.ahead-ms";

  private static final String CONFIG_FILE = "delta-flink.properties";
  private static final Conf INSTANCE = new Conf();

  private final Map<String, String> props;
  // For debug purpose
  private URL sourcePath;

  private Conf() {
    this.props = load();
  }

  public static Conf getInstance() {
    return INSTANCE;
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

  public int getSinkWriterNumConcurrentFiles() {
    return Integer.parseInt(getOrDefault(SINK_WRITER_NUM_CONCURRENT_FILE, "1000"));
  }

  public int getTableThreadPoolSize() {
    return Integer.parseInt(getOrDefault(TABLE_THREAD_POOL_SIZE, "5"));
  }

  public boolean getTableCacheEnable() {
    return Boolean.parseBoolean(getOrDefault(TABLE_CACHE_ENABLE, "true"));
  }

  public int getTableCacheSize() {
    return Integer.parseInt(getOrDefault(TABLE_CACHE_SIZE, "100"));
  }

  public long getTableCacheExpireInMs() {
    return Long.parseLong(getOrDefault(TABLE_CACHE_EXPIRE_MS, "300000"));
  }

  public double getDeltaCheckpointFrequency() {
    return Double.parseDouble(getOrDefault(DELTA_CHECKPOINT_FREQUENCY, "0"));
  }

  public int getCredentialsRefreshThreadPoolSize() {
    return Integer.parseInt(getOrDefault(CREDENTIALS_REFRESH_THREAD_POOL_SIZE, "10"));
  }

  public long getCredentialsRefreshAheadInMs() {
    return Long.parseLong(getOrDefault(CREDENTIALS_REFRESH_AHEAD_MS, "60000"));
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
  private Map<String, String> load() {
    Properties p = new Properties();
    try (InputStream in = Conf.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
      if (in == null) {
        return Map.of();
      }
      sourcePath = Conf.class.getClassLoader().getResource(CONFIG_FILE);
      LOG.info("Loaded configuration from {}", sourcePath);
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
