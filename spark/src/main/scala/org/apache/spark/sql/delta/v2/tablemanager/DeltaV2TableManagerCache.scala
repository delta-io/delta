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

package org.apache.spark.sql.delta.v2.tablemanager

import java.util.concurrent.{ExecutionException, TimeUnit}

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener}
import com.google.common.util.concurrent.UncheckedExecutionException
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

  @volatile private var cache:
      Cache[DeltaV2CacheKey, DeltaV2TableManager] = _

  def isEnabled(sqlConf: SQLConf): Boolean =
    sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE) > 0

  def getOrCreate(
      sqlConf: SQLConf,
      key: DeltaV2CacheKey,
      catalogTableOpt: Option[CatalogTable] = None): DeltaV2TableManager = {
    if (!isEnabled(sqlConf)) {
      return createManager(key, catalogTableOpt)
    }
    val managerCache = getOrCreateCache(sqlConf)
    try {
      managerCache.get(key, () => createManager(key, catalogTableOpt))
    } catch {
      // Guava Cache.get wraps loader exceptions; unwrap to re-throw the original cause
      // (same pattern as DeltaLog.apply's cache lookup).
      case e: ExecutionException => throw e.getCause
      case e: UncheckedExecutionException => throw e.getCause
    }
  }

  def invalidate(key: DeltaV2CacheKey): Unit = {
    val managerCache = cache
    if (managerCache != null) {
      managerCache.invalidate(key)
    }
  }

  def clearCache(): Unit = {
    val managerCache = cache
    if (managerCache != null) {
      managerCache.invalidateAll()
    }
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
    val managerCache = cache
    if (managerCache != null) {
      managerCache.asMap().keySet().removeIf(_.path == logPath)
    }
  }

  def cacheSizeForTesting(): Long = {
    val managerCache = cache
    if (managerCache == null) 0L else managerCache.size()
  }

  def putForTesting(
      sqlConf: SQLConf,
      key: DeltaV2CacheKey,
      manager: DeltaV2TableManager): Unit = {
    getOrCreateCache(sqlConf).put(key, manager)
  }

  def containsKeyForTesting(key: DeltaV2CacheKey): Boolean = {
    val managerCache = cache
    managerCache != null && managerCache.getIfPresent(key) != null
  }

  private def createManager(
      key: DeltaV2CacheKey,
      catalogTableOpt: Option[CatalogTable]): DeltaV2TableManagerImpl = {
    new DeltaV2TableManagerImpl(key, catalogTableOpt)
  }

  private def getOrCreateCache(
      sqlConf: SQLConf): Cache[DeltaV2CacheKey, DeltaV2TableManager] = {
    var managerCache = cache
    if (managerCache == null) {
      synchronized {
        managerCache = cache
        if (managerCache == null) {
          val maxSize =
            sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
          val ttlMinutes =
            sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES)
          val listener: RemovalListener[
              DeltaV2CacheKey, DeltaV2TableManager] = notification => {
            val composite = notification.getValue
            if (composite != null) {
              composite.retire()
            }
          }
          managerCache = CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(ttlMinutes, TimeUnit.MINUTES)
            .removalListener(listener)
            .build[DeltaV2CacheKey, DeltaV2TableManager]()
          cache = managerCache
        }
      }
    }
    managerCache
  }

}
