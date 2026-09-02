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
package org.apache.spark.sql.delta.v2.tablemanager

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.google.common.base.Ticker
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf

/**
 * Process-local cache of [[DeltaV2TableManager]] composites, mirroring DeltaLog's cache pattern.
 *
 * Enabled when `delta.log.cacheSize > 0`. Size and TTL are read from [[SQLConf]] at first access,
 * reusing the V1 DeltaLog cache configs.
 */
object DeltaV2TableManagerCache extends DeltaLogging {

  @volatile private var cache: Option[Cache[DeltaV2CacheKey, DeltaV2TableManager]] = None
  private var tickerForTesting: Option[Ticker] = None

  def isEnabled(sqlConf: SQLConf): Boolean =
    sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE) > 0

  def getOrCreate(
      sqlConf: SQLConf,
      key: DeltaV2CacheKey,
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    recordFrameProfile(
        "Delta", "DeltaV2.tableManagerCache.getOrCreate") {
      if (!isEnabled(sqlConf)) {
        return createManager(key, initialCatalogTableOpt)
      }
      val managerCache = getOrCreateCache(sqlConf)
      managerCache.get(key, () => {
        recordFrameProfile(
            "Delta", "DeltaV2.cache.createManager") {
          createManager(key, initialCatalogTableOpt)
        }
      })
    }
  }

  /**
   * Inserts a pre-built manager into the cache for testing. Ensures
   * the cache is initialized using the given [[SQLConf]], then puts
   * the entry directly. Used by lifecycle tests (retire, capacity,
   * TTL) that need custom/stub managers without routing through
   * the production loader.
   */
  private[tablemanager] def putForTesting(
      sqlConf: SQLConf,
      key: DeltaV2CacheKey,
      manager: DeltaV2TableManager): Unit = {
    val managerCache = getOrCreateCache(sqlConf)
    managerCache.put(key, manager)
  }

  def invalidate(key: DeltaV2CacheKey): Unit = {
    cache.foreach(_.invalidate(key))
  }

  def clearCache(): Unit = {
    cache.foreach(_.invalidateAll())
  }

  /**
   * Invalidates all cache entries whose log path matches the given path.
   *
   * @param logPath the `_delta_log` directory path to match against cached keys. Must be in the
   *   same form as [[DeltaV2CacheKey.path]] -- i.e., the fully-qualified data path with
   *   `_delta_log` appended. Plain equality is used; no normalization or scheme resolution is
   *   applied.
   */
  def invalidateByLogPath(logPath: Path): Unit = {
    cache.foreach(_.asMap().keySet().removeIf(_.path == logPath))
  }

  private[tablemanager]
  def cacheSizeForTesting(): Long = cache.map(_.size()).getOrElse(0L)

  private[tablemanager]
  def containsKeyForTesting(key: DeltaV2CacheKey): Boolean = {
    cache.exists(_.getIfPresent(key) != null)
  }
  private[tablemanager] def resetCacheForTesting(): Unit = synchronized {
    cache.foreach(_.invalidateAll())
    cache = None
    tickerForTesting = None
  }

  /**
   * Injects a custom [[Ticker]] for deterministic TTL testing. Clears
   * the existing cache so the next access rebuilds with the ticker.
   */
  private[tablemanager] def setTickerForTesting(
      ticker: Ticker): Unit = synchronized {
    cache.foreach(_.invalidateAll())
    cache = None
    tickerForTesting = Some(ticker)
  }

  /**
   * Triggers pending eviction maintenance. Guava defers cleanup until
   * the next cache access; call this after advancing a test ticker to
   * force the eviction and [[RemovalListener]] callback.
   */
  private[tablemanager] def cleanUpForTesting(): Unit = {
    cache.foreach(_.cleanUp())
  }

  private def createManager(
      key: DeltaV2CacheKey,
      initialCatalogTableOpt: Option[CatalogTable]
  ): DeltaV2TableManagerImpl = {
    new DeltaV2TableManagerImpl(key, initialCatalogTableOpt)
  }

  /**
   * Returns the process-global cache, initializing it on first access. Uses the same
   * `synchronized` + `Option` pattern as [[DeltaLog.getOrCreateCache]] -- the first caller's
   * SQLConf determines size and TTL for all subsequent callers.
   */
  private def getOrCreateCache(
      sqlConf: SQLConf): Cache[DeltaV2CacheKey, DeltaV2TableManager] = synchronized {
    cache.getOrElse {
      val maxSize = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
      val ttlMinutes = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES)
      val listener: RemovalListener[DeltaV2CacheKey, DeltaV2TableManager] = notification => {
        val composite = notification.getValue
        if (composite != null) {
          composite.retire()
        }
      }
      val builder = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterAccess(ttlMinutes, TimeUnit.MINUTES)
        .removalListener(listener)
      tickerForTesting.foreach(builder.ticker)
      val newCache =
        builder.build[DeltaV2CacheKey, DeltaV2TableManager]()
      cache = Some(newCache)
      newCache
    }
  }

}
