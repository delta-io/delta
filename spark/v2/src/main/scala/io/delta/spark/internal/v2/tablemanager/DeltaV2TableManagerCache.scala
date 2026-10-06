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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaFileSystemOptions, PathWithFileSystem}
import io.delta.spark.internal.v2.DeltaV2Logging
import com.google.common.base.Ticker
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf

/**
 * Per-instance [[DeltaV2TableManager]] cache configured through constructor parameters. The
 * companion object owns the process-wide singleton; tests construct independent instances with
 * custom settings and injected factories.
 */
private[tablemanager] class DeltaV2TableManagerCache(
    maxSize: Long,
    ttlMinutes: Long,
    ticker: Ticker = Ticker.systemTicker(),
    managerFactory: (
        DeltaV2TableManagerCache.CacheKey,
        Option[CatalogTable]) => DeltaV2TableManager =
      (key, catalog) => new DeltaV2TableManagerImpl(
        key.path.getParent,
        key.sessionInvariantFsOptions,
        catalog)
) extends DeltaV2Logging {
  import DeltaV2TableManagerCache.CacheKey

  private val cache: Cache[CacheKey, DeltaV2TableManager] = {
    val listener: RemovalListener[CacheKey, DeltaV2TableManager] = notification => {
        val manager = notification.getValue
        if (manager != null) manager.retire()
      }
    CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .expireAfterAccess(ttlMinutes, TimeUnit.MINUTES)
      .removalListener(listener)
      .ticker(ticker)
      .build[CacheKey, DeltaV2TableManager]()
  }

  // Guava's Cache.get wraps loader failures: checked exceptions in ExecutionException, runtime
  // exceptions in UncheckedExecutionException, and Errors in ExecutionError. Unwrapping preserves
  // the original Delta error class, SQLSTATE, and cause chain rather than exposing Guava wrappers.
  def getOrCreate(
      key: CacheKey,
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    try {
      cache.get(key, () => {
        recordFrameProfile("tableManagerCache.createManager") {
          managerFactory(key, initialCatalogTableOpt)
        }
      })
    } catch {
      case e @ (_: UncheckedExecutionException |
          _: ExecutionError | _: ExecutionException) =>
        val cause = Option(e.getCause).getOrElse(e)
        logWarning(log"Cache loader failed; rethrowing original cause", cause)
        throw cause
    }
  }

  def invalidate(key: CacheKey): Unit = cache.invalidate(key)

  /**
   * Invalidates all entries whose log path matches the given path. Plain equality is used; no
   * normalization or scheme resolution is applied.
   */
  def invalidateByLogPath(logPath: Path): Unit = {
    cache.asMap().keySet().removeIf(_.path == logPath)
  }

  def invalidateAll(): Unit = cache.invalidateAll()

  def size(): Long = cache.size()

  def getIfPresent(key: CacheKey): Option[DeltaV2TableManager] = Option(cache.getIfPresent(key))

  def contains(key: CacheKey): Boolean = cache.getIfPresent(key) != null

  /** Triggers pending eviction maintenance (Guava defers cleanup). */
  def cleanUp(): Unit = cache.cleanUp()
}

/**
 * Process-local cache of [[DeltaV2TableManager]] composites, mirroring DeltaLog's cache pattern.
 * Enabled when `delta.log.cacheSize > 0`. Size and TTL are read from [[SQLConf]] at first access,
 * reusing the V1 DeltaLog cache configs.
 *
 * Wider callers use the [[forTable]] facade, which constructs the internal cache key, resolves the
 * session configuration, and delegates to the per-instance cache. The cache key type is not
 * exposed outside the `tablemanager` package.
 */
private[v2] object DeltaV2TableManagerCache extends DeltaV2Logging {

  // === Cache key ==================================================

  /**
   * Cache identity for the Delta V2 table-manager cache, aligned with the V1 DeltaLogCacheKey.
   *
   * @param path the Delta log directory path (`_delta_log`), derived from a fully-qualified data
   *   path. This is NOT the table data path -- it is `dataPath/_delta_log`. Must be absolute;
   *   scheme presence depends on the caller's qualification (catalog-resolved paths carry a
   *   scheme, local test paths may not).
   * @param sessionInvariantFsOptions filesystem-prefixed options (`fs.*`, `dfs.*`) extracted from
   *   reader/writer options and catalog storage properties. These are the credential-bearing
   *   options that distinguish cache entries for the same path accessed with different
   *   credentials. Called "session-invariant" because they are fixed at table-resolution time and
   *   do not change across requests to the same cached composite. Values are redacted in
   *   [[toString]] to prevent credential leakage in logs.
   */
  private[tablemanager] case class CacheKey(
      path: Path,
      sessionInvariantFsOptions: Map[String, String]) {

    override def toString: String =
      s"CacheKey(path=$path,fsOptions=<redacted>)"
  }

  private[tablemanager] object CacheKey {

    /**
     * Constructs a cache key from caller-supplied table coordinates.
     *
     * Cache identity must follow DeltaLog's resolved filesystem identity rather than lexical path
     * spelling: two paths that resolve to the same filesystem location must share a cache entry,
     * and paths that differ by authority or mount point must remain distinct.
     *
     * To achieve this, `newHadoopConfWithOptions` snapshots the current session's Hadoop
     * configuration and overlays the derived session-invariant filesystem options. This
     * Configuration is used transiently to choose the filesystem, default authority, and working
     * directory, then call `FileSystem.makeQualified`. Neither the SparkSession nor the
     * Configuration is retained in the cache key or manager.
     *
     * Unqualified paths can therefore resolve differently under different session default
     * filesystems, while already-qualified absolute paths (e.g. `s3://bucket/path`) are generally
     * stable. Different authorities and mount points remain distinct; the code intentionally does
     * not lowercase or hand-normalize bucket or authority names.
     *
     * @param spark the active SparkSession, used to resolve filesystem options via
     *   [[DeltaFileSystemOptions.buildFsOptions]] and to qualify the cache key path through the
     *   filesystem.
     * @param dataPath the table's data directory path as a string (e.g.
     *   `s3://bucket/warehouse/db/table` or `/tmp/local-table`). Must NOT include the `_delta_log`
     *   suffix -- this method appends it.
     * @param options reader/writer options (Java map). Only `fs.*` and `dfs.*` prefixed entries are
     *   retained; others are filtered out.
     * @param catalogTableOpt optional catalog table whose storage properties contribute
     *   additional filesystem options.
     */
    def from(
        spark: SparkSession,
        dataPath: String,
        options: java.util.Map[String, String],
        catalogTableOpt: Option[CatalogTable] = None
    ): CacheKey = {
      val sessionInvariantFsOptions = DeltaFileSystemOptions.buildFsOptions(
        spark, options.asScala.toMap, catalogTableOpt)
      val rawLogPath = DeltaTableUtils.safeConcatPaths(new Path(dataPath), "_delta_log")
      // Snapshot the session's Hadoop config with fs options overlaid.
      // Used only for qualification; not retained in the key.
      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)
      // scalastyle:on deltahadoopconfiguration
      val qualifiedLogPath = PathWithFileSystem
        .withConf(rawLogPath, hadoopConf).fs.makeQualified(rawLogPath)
      CacheKey(qualifiedLogPath, sessionInvariantFsOptions)
    }
  }

  // === Process-global singleton ===================================

  @volatile private var instance: Option[DeltaV2TableManagerCache] = None

  def isEnabled(sqlConf: SQLConf): Boolean = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE) > 0

  /**
   * Returns a cached or freshly-created [[DeltaV2TableManager]] for the given table coordinates.
   *
   * This is the sole public entry point for wider callers. It constructs the internal [[CacheKey]],
   * derives SQLConf from the [[SparkSession]], and routes through the process-global singleton
   * cache (or bypasses it when caching is disabled).
   *
   * @param spark the active SparkSession, used for filesystem resolution and configuration.
   * @param dataPath the table's data directory path as a string.
   * @param options reader/writer options (Java map).
   * @param initialCatalogTableOpt optional catalog table whose storage properties contribute
   *   additional filesystem options.
   */
  def forTable(
      spark: SparkSession,
      dataPath: String,
      options: java.util.Map[String, String],
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    recordFrameProfile("tableManagerCache.forTable") {
      val key = CacheKey.from(spark, dataPath, options, initialCatalogTableOpt)
      val sqlConf = spark.sessionState.conf
      if (!isEnabled(sqlConf)) {
        return new DeltaV2TableManagerImpl(
          key.path.getParent,
          key.sessionInvariantFsOptions,
          initialCatalogTableOpt)
      }
      getOrCreateInstance(sqlConf).getOrCreate(key, initialCatalogTableOpt)
    }
  }

  /**
   * Package-private getOrCreate for per-instance tests that inject custom cache keys directly.
   */
  private[tablemanager] def getOrCreate(
      sqlConf: SQLConf,
      key: CacheKey,
      initialCatalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2TableManager = {
    recordFrameProfile("tableManagerCache.getOrCreate") {
      if (!isEnabled(sqlConf)) {
        return new DeltaV2TableManagerImpl(
          key.path.getParent,
          key.sessionInvariantFsOptions,
          initialCatalogTableOpt)
      }
      getOrCreateInstance(sqlConf).getOrCreate(key, initialCatalogTableOpt)
    }
  }

  private[tablemanager] def invalidate(key: CacheKey): Unit = instance.foreach(_.invalidate(key))

  def clearCache(): Unit = instance.foreach(_.invalidateAll())

  /**
   * Invalidates all cache entries whose key path equals `logPath`. Matching uses exact [[Path]]
   * equality; no normalization, qualification, or scheme resolution is applied by this method --
   * the caller must supply an already-qualified path consistent with the key's construction in
   * [[CacheKey.from]].
   */
  def invalidateByLogPath(logPath: Path): Unit = instance.foreach(_.invalidateByLogPath(logPath))

  /**
   * Retires and clears all cached managers, then discards the singleton. Matches the
   * DeltaLog.unsetCache precedent for process-global test isolation.
   */
  private[tablemanager] def unsetCache(): Unit = synchronized {
    instance.foreach(_.invalidateAll())
    instance = None
  }

  /**
   * Returns the process-global cache instance, initializing on first access. First caller's SQLConf
   * determines size and TTL for all subsequent callers.
   */
  private def getOrCreateInstance(sqlConf: SQLConf): DeltaV2TableManagerCache = synchronized {
    instance.getOrElse {
      val maxSize = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
      val ttlMinutes = sqlConf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES)
      val newInstance = new DeltaV2TableManagerCache(maxSize = maxSize, ttlMinutes = ttlMinutes)
      instance = Some(newInstance)
      newInstance
    }
  }
}
