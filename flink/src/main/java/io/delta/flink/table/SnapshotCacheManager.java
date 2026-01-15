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

package io.delta.flink.table;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.delta.flink.Conf;
import io.delta.kernel.Snapshot;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@code SnapshotCacheManager} provides a pluggable abstraction for caching {@link Snapshot}
 * instances keyed by a string identifier (for example, a table path).
 *
 * <p>The cache is primarily used to avoid repeatedly loading and reconstructing Delta table
 * snapshots from remote storage, which can be expensive for tables with long histories or high
 * access frequency.
 *
 * <p>Implementations may choose different caching strategies, including no-op caching or in-memory
 * eviction-based caching.
 *
 * <p>This interface is {@link Serializable} so that it can be safely used in distributed Flink jobs
 * and reconstructed across task restarts.
 */
public interface SnapshotCacheManager extends Serializable {

  /**
   * Inserts or updates a cached snapshot for the given key.
   *
   * @param key the cache key (for example, a table path)
   * @param snapshot the snapshot to cache
   */
  default void put(String key, Snapshot snapshot) {}

  /**
   * Invalidates the cached snapshot associated with the given key.
   *
   * @param key the cache key to invalidate
   */
  default void invalidate(String key) {}

  /**
   * Retrieves a cached snapshot for the given key, loading it on demand if it is not already
   * present in the cache.
   *
   * <p>If the snapshot is not cached, the provided {@code body} is invoked to compute the value,
   * which may then be stored in the cache depending on the implementation.
   *
   * <p>If a version exists, versionProbe is used to quickly verify if the version is up-to-date.
   *
   * @param key the cache key
   * @param versionProbe checks if a version exists. Expected to be faster than body. Used to
   *     quickly verify if the cached snapshot is updated. It takes a version number and return true
   *     if the version already exists.
   * @param body a callable used to compute the snapshot on cache miss
   * @return the cached or newly loaded snapshot, empty means no snapshot exists ( empty table )
   */
  default Optional<Snapshot> get(
      String key, Predicate<Long> versionProbe, Function<String, Optional<Snapshot>> body) {
    try {
      return body.apply(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static SnapshotCacheManager getInstance() {
    return Conf.getInstance().getTableCacheEnable()
        ? new LocalCacheManager()
        : new NoCacheManager();
  }

  /**
   * A no-op {@link SnapshotCacheManager} implementation that performs no caching.
   *
   * <p>This implementation always bypasses caching logic and is useful in scenarios where caching
   * is undesirable or needs to be disabled explicitly.
   */
  class NoCacheManager implements SnapshotCacheManager {
    // Intentionally empty
  }

  /**
   * The default {@link SnapshotCacheManager} implementation backed by an in-memory, path-based
   * cache.
   *
   * <p>This implementation uses a bounded cache with time-based eviction to store {@link Snapshot}
   * instances, providing faster snapshot access while preventing unbounded memory growth.
   *
   * <p>Cache size and expiration are controlled via {@link Conf}.
   */
  class LocalCacheManager implements SnapshotCacheManager {

    /**
     * A path-based snapshot cache used to speed up snapshot loading.
     *
     * <p>The cache is bounded in size and evicts entries based on access time to balance
     * performance and memory usage.
     */
    static final Cache<String, Optional<Snapshot>> SNAPSHOT_CACHE =
        Caffeine.newBuilder()
            .maximumSize(Conf.getInstance().getTableCacheSize())
            .expireAfterAccess(Conf.getInstance().getTableCacheExpireInMs(), TimeUnit.MILLISECONDS)
            .build();

    @Override
    public void put(String key, Snapshot snapshot) {
      SNAPSHOT_CACHE.put(key, Optional.ofNullable(snapshot));
    }

    @Override
    public void invalidate(String key) {
      SNAPSHOT_CACHE.invalidate(key);
    }

    @Override
    public Optional<Snapshot> get(
        String key, Predicate<Long> versionProbe, Function<String, Optional<Snapshot>> body) {
      Optional<Snapshot> cached = SNAPSHOT_CACHE.get(key, body);
      // Probe if `version + 1` already exists.
      long versionToProbe = cached.map(Snapshot::getVersion).orElse(-1L) + 1;
      // `version + 1` exists. It means current cache at `version` is outdated.
      if (versionProbe.test(versionToProbe)) {
        // Cache is outdated and needs reload
        SNAPSHOT_CACHE.invalidate(key);
        return SNAPSHOT_CACHE.get(key, body);
      }
      return cached;
    }
  }
}
