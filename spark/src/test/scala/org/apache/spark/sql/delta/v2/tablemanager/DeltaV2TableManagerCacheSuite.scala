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

import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaV2TableManagerCacheSuite
    extends QueryTest
    with SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaV2TableManagerCache.resetCacheForTesting()
  }

  override def afterEach(): Unit = {
    DeltaV2TableManagerCache.resetCacheForTesting()
    super.afterEach()
  }

  test("enabled by default") {
    assert(DeltaV2TableManagerCache.isEnabled(spark.sessionState.conf))
  }

  test("default cache size is positive") {
    val size =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
    assert(size > 0, s"Expected positive cache size, got $size")
  }

  test("disabled when size=0") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "0") {
      assert(
        !DeltaV2TableManagerCache.isEnabled(spark.sessionState.conf))
    }
  }

  test("getOrCreate bypasses cache when size=0") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "0") {
      withTempDir { dir =>
        val key = DeltaV2CacheKey.from(
          spark, dir.getCanonicalPath, Collections.emptyMap())
        val first = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        val second = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        assert(first ne second)
      }
    }
  }

  test("getOrCreate caches when enabled") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = DeltaV2CacheKey.from(
          spark, dir.getCanonicalPath, Collections.emptyMap())
        val first = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        val second = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        assert(first eq second)
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 1)
      }
    }
  }

  test("different keys produce different entries") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dirA =>
        withTempDir { dirB =>
          val keyA = DeltaV2CacheKey.from(
            spark, dirA.getCanonicalPath, Collections.emptyMap())
          val keyB = DeltaV2CacheKey.from(
            spark, dirB.getCanonicalPath, Collections.emptyMap())
          DeltaV2TableManagerCache.getOrCreate(
            spark.sessionState.conf, keyA)
          DeltaV2TableManagerCache.getOrCreate(
            spark.sessionState.conf, keyB)
          assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 2)
        }
      }
    }
  }

  test("invalidate removes a single entry") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = DeltaV2CacheKey.from(
          spark, dir.getCanonicalPath, Collections.emptyMap())
        DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        assert(DeltaV2TableManagerCache.containsKeyForTesting(key))
        DeltaV2TableManagerCache.invalidate(key)
        assert(!DeltaV2TableManagerCache.containsKeyForTesting(key))
      }
    }
  }

  test("invalidateByLogPath removes matching entries") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dirA =>
        withTempDir { dirB =>
          val keyA = DeltaV2CacheKey.from(
            spark, dirA.getCanonicalPath, Collections.emptyMap())
          val keyB = DeltaV2CacheKey.from(
            spark, dirB.getCanonicalPath, Collections.emptyMap())
          DeltaV2TableManagerCache.getOrCreate(
            spark.sessionState.conf, keyA)
          DeltaV2TableManagerCache.getOrCreate(
            spark.sessionState.conf, keyB)
          assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 2)

          DeltaV2TableManagerCache.invalidateByLogPath(keyA.path)
          assert(
            !DeltaV2TableManagerCache.containsKeyForTesting(keyA))
          assert(
            DeltaV2TableManagerCache.containsKeyForTesting(keyB))
        }
      }
    }
  }

  test("clearCache empties the entire cache") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = DeltaV2CacheKey.from(
          spark, dir.getCanonicalPath, Collections.emptyMap())
        DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() > 0)
        DeltaV2TableManagerCache.clearCache()
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 0)
      }
    }
  }

  test("process-global: different sessions with same key return same instance") {
    // Verified precedent: DeltaLog.forTable(sessionA, path) eq DeltaLog.forTable(sessionB, path)
    // when both sessions resolve to the same DeltaLogCacheKey. The Guava cache is process-wide
    // and the first caller's conf initializes it. We assert the same for our V2 cache.
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val sessionA = spark.newSession()
        val sessionB = spark.newSession()
        // Give session B a different cache size to prove it doesn't create a separate cache
        sessionB.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "500")

        val keyA = DeltaV2CacheKey.from(
          sessionA, dir.getCanonicalPath, Collections.emptyMap())
        val keyB = DeltaV2CacheKey.from(
          sessionB, dir.getCanonicalPath, Collections.emptyMap())

        val fromA = DeltaV2TableManagerCache.getOrCreate(
          sessionA.sessionState.conf, keyA)
        val fromB = DeltaV2TableManagerCache.getOrCreate(
          sessionB.sessionState.conf, keyB)

        assert(keyA == keyB,
          "Different sessions must produce the same cache key for the same path")
        assert(fromA eq fromB,
          "Same key from different sessions must return the same cached instance " +
            "(process-global single-flight, matching DeltaLog.forTable semantics)")
      }
    }
  }

  test("process-global: cache is single-flight initialized regardless of calling session") {
    // If concurrent callers initialized separate cache instances, each would load its own manager.
    // Referential equality therefore proves both process-global cache initialization and Guava's
    // per-key loader single-flight behavior.
    withTempDir { dir =>
      val sessionA = spark.newSession()
      val sessionB = spark.newSession()
      sessionA.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "1000")
      sessionB.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "2000")
      val keyA = DeltaV2CacheKey.from(
        sessionA, dir.getCanonicalPath, Collections.emptyMap())
      val keyB = DeltaV2CacheKey.from(
        sessionB, dir.getCanonicalPath, Collections.emptyMap())
      assert(keyA == keyB)

      val start = new CountDownLatch(1)
      // scalastyle:off sparkThreadPools
      val executor = Executors.newFixedThreadPool(2)
      // scalastyle:on sparkThreadPools
      try {
        val futureA = executor.submit(() => {
          start.await()
          DeltaV2TableManagerCache.getOrCreate(sessionA.sessionState.conf, keyA)
        })
        val futureB = executor.submit(() => {
          start.await()
          DeltaV2TableManagerCache.getOrCreate(sessionB.sessionState.conf, keyB)
        })
        start.countDown()

        assert(futureA.get() eq futureB.get(),
          "Concurrent sessions must share one cache and one manager load for the same key")
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 1)
      } finally {
        executor.shutdownNow()
      }
    }
  }
}
