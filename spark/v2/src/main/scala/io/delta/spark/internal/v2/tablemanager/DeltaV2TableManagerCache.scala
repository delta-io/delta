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
package io.delta.spark.internal.v2.tablemanager

import java.util.concurrent.{ExecutionException, TimeUnit}

import io.delta.spark.internal.v2.DeltaV2Logging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.google.common.base.Ticker
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf

/**
 * Per-instance [[DeltaV2TableManager]] cache configured through constructor parameters.
 * The companion object owns the process-wide singleton; tests construct independent
 * instances with custom settings and injected factories.
 */
private[tablemanager] class DeltaV2TableManagerCache(
    maxSize: Long,
    ttlMinutes: Long,
    ticker: Ticker = Ticker.systemTicker(),
    managerFactory: (DeltaV2CacheKey, Option[CatalogTable]) => DeltaV2TableManager =
      (key, catalog) => new DeltaV2TableManagerImpl(key, catalog)
) extends DeltaV2Logging {

  private val cache: Cache[DeltaV2CacheKey, DeltaV2TableManager] = {
    val listener: RemovalListener[DeltaV2CacheKey, DeltaV2TableManager] =
      notification => {
        val composite = notification.getValue
        if (composite != null) composite.retire()
      }
    CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .expireAfterAccess(ttlMinutes, TimeUnit.MINUTES)
      .removalListener(listener)
      .ticker(ticker)
      .build[DeltaV2CacheKey, DeltaV2TableManager]()
  }

  // Guava's Cache.get wraps loader failures: checked exceptions in ExecutionException,
  // runtime exceptions in UncheckedExecutionException, and Errors in ExecutionError.
  // Unwrapping preserves the original Delta error class, SQLSTATE, and cause chain
  // rather than exposing Guava cache wrappers to callers.
  def getOrCreate(
      key: DeltaV2CacheKey,
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    try {
      cache.get(key, () => {
        recordFrameProfile("tableManagerCache.createManager") {
          managerFactory(key, initialCatalogTableOpt)
        }
      })
    } catch {
      case e @ (_: UncheckedExecutionException | _: ExecutionError | _: ExecutionException) =>
        logWarning(log"Cache loader failed; rethrowing original cause", e.getCause)
        throw e.getCause
    }
  }

  def invalidate(key: DeltaV2CacheKey): Unit = cache.invalidate(key)

  /**
   * Invalidates all entries whose log path matches the given path. Plain equality is
   * used; no normalization or scheme resolution is applied.
   */
  def invalidateByLogPath(logPath: Path): Unit = {
    cache.asMap().keySet().removeIf(_.path == logPath)
  }

  def invalidateAll(): Unit = cache.invalidateAll()

  def size(): Long = cache.size()

  def getIfPresent(key: DeltaV2CacheKey): Option[DeltaV2TableManager] =
    Option(cache.getIfPresent(key))

  def contains(key: DeltaV2CacheKey): Boolean =
    cache.getIfPresent(key) != null

  /** Triggers pending eviction maintenance (Guava defers cleanup). */
  def cleanUp(): Unit = cache.cleanUp()
}

/**
 * Process-local cache of [[DeltaV2TableManager]] composites, mirroring DeltaLog's cache
 * pattern. Enabled when `delta.log.cacheSize > 0`. Size and TTL are read from [[SQLConf]]
 * at first access, reusing the V1 DeltaLog cache configs.
 */
private[v2] object DeltaV2TableManagerCache extends DeltaV2Logging {

  @volatile private var instance: Option[DeltaV2TableManagerCache] = None

  def isEnabled(sqlConf: SQLConf): Boolean =
    sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE) > 0

  def getOrCreate(
      sqlConf: SQLConf,
      key: DeltaV2CacheKey,
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    recordFrameProfile("tableManagerCache.getOrCreate") {
      if (!isEnabled(sqlConf)) {
        return createManager(key, initialCatalogTableOpt)
      }
      getOrCreateInstance(sqlConf).getOrCreate(key, initialCatalogTableOpt)
    }
  }

  def invalidate(key: DeltaV2CacheKey): Unit = {
    instance.foreach(_.invalidate(key))
  }

  def clearCache(): Unit = {
    instance.foreach(_.invalidateAll())
  }

  /**
   * Invalidates all cache entries whose key path equals `logPath`. Matching uses
   * exact [[Path]] equality; no normalization, qualification, or scheme resolution
   * is applied by this method -- the caller must supply an already-qualified path
   * consistent with the key's construction in [[DeltaV2CacheKey.from]].
   */
  def invalidateByLogPath(logPath: Path): Unit = {
    instance.foreach(_.invalidateByLogPath(logPath))
  }

  /**
   * Retires and clears all cached managers, then discards the singleton. Matches
   * the DeltaLog.unsetCache precedent for process-global test isolation.
   */
  private[tablemanager] def unsetCache(): Unit = synchronized {
    instance.foreach(_.invalidateAll())
    instance = None
  }

  private def createManager(
      key: DeltaV2CacheKey,
      initialCatalogTableOpt: Option[CatalogTable]
  ): DeltaV2TableManagerImpl = {
    new DeltaV2TableManagerImpl(key, initialCatalogTableOpt)
  }

  /**
   * Returns the process-global cache instance, initializing on first access. First
   * caller's SQLConf determines size and TTL for all subsequent callers.
   */
  private def getOrCreateInstance(sqlConf: SQLConf): DeltaV2TableManagerCache = synchronized {
    instance.getOrElse {
      val maxSize = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
      val ttlMinutes =
        sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES)
      val newInstance = new DeltaV2TableManagerCache(
        maxSize = maxSize,
        ttlMinutes = ttlMinutes)
      instance = Some(newInstance)
      newInstance
    }
  }
}
