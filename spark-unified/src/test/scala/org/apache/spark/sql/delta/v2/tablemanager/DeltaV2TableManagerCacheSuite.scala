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

import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.google.common.base.Ticker
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaV2TableManagerCacheSuite
    extends QueryTest
    with SharedSparkSession {

  /** Creates a cache key from a data directory using the public factory. */
  private def makeKey(dataPath: String): DeltaV2CacheKey =
    DeltaV2CacheKey.from(spark, dataPath, Collections.emptyMap())

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
        val key = makeKey(dir.getCanonicalPath)
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
        val key = makeKey(dir.getCanonicalPath)
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
          val keyA = makeKey(dirA.getCanonicalPath)
          val keyB = makeKey(dirB.getCanonicalPath)
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
        val key = makeKey(dir.getCanonicalPath)
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
          val keyA = makeKey(dirA.getCanonicalPath)
          val keyB = makeKey(dirB.getCanonicalPath)
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
        val key = makeKey(dir.getCanonicalPath)
        DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key)
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() > 0)
        DeltaV2TableManagerCache.clearCache()
        assert(DeltaV2TableManagerCache.cacheSizeForTesting() == 0)
      }
    }
  }

  test("process-global: different sessions share same instance") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val sessionA = spark.newSession()
        val sessionB = spark.newSession()
        sessionB.conf.set(
          DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "500")

        val keyA = DeltaV2CacheKey.from(
          sessionA, dir.getCanonicalPath, Collections.emptyMap())
        val keyB = DeltaV2CacheKey.from(
          sessionB, dir.getCanonicalPath, Collections.emptyMap())

        val fromA = DeltaV2TableManagerCache.getOrCreate(
          sessionA.sessionState.conf, keyA)
        val fromB = DeltaV2TableManagerCache.getOrCreate(
          sessionB.sessionState.conf, keyB)

        assert(keyA == keyB,
          "Same path must produce the same cache key across sessions")
        assert(fromA eq fromB,
          "Same key from different sessions must return the same " +
            "cached instance (process-global, DeltaLog semantics)")
      }
    }
  }

  test("process-global: single-flight initialization") {
    withTempDir { dir =>
      val sessionA = spark.newSession()
      val sessionB = spark.newSession()
      sessionA.conf.set(
        DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "1000")
      sessionB.conf.set(
        DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "2000")
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
          DeltaV2TableManagerCache.getOrCreate(
            sessionA.sessionState.conf, keyA)
        })
        val futureB = executor.submit(() => {
          start.await()
          DeltaV2TableManagerCache.getOrCreate(
            sessionB.sessionState.conf, keyB)
        })
        start.countDown()

        assert(futureA.get() eq futureB.get(),
          "Concurrent sessions must share one manager load")
        assert(
          DeltaV2TableManagerCache.cacheSizeForTesting() == 1)
      } finally {
        executor.shutdownNow()
      }
    }
  }

  // --- initialCatalogTableOpt preservation ------------------------

  test("cache hit preserves initialCatalogTableOpt from first load") {
    withSQLConf(
        DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = makeKey(dir.getCanonicalPath)
        val catalogA = CatalogTable(
          identifier = TableIdentifier("tableA"),
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty,
          schema = new StructType())
        val catalogB = CatalogTable(
          identifier = TableIdentifier("tableB"),
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty,
          schema = new StructType())

        val first = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key, Some(catalogA))
        val second = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf, key, Some(catalogB))

        assert(first eq second)
        val impl = first.asInstanceOf[DeltaV2TableManagerImpl]
        assert(impl.initialCatalogTableOpt === Some(catalogA),
          "initial catalog should be from first load")
      }
    }
  }

  // --- RemovalListener -> retire() --------------------------------

  test("removal listener retires manager on invalidation") {
    withSQLConf(
        DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = makeKey(dir.getCanonicalPath)
        val stub = new StubTableManager("a")
        DeltaV2TableManagerCache.putForTesting(
          spark.sessionState.conf, key, stub)
        assert(!stub.retired)
        DeltaV2TableManagerCache.invalidate(key)
        assert(stub.retired,
          "retire() should be called on eviction")
      }
    }
  }

  // --- Capacity / first-caller-wins -------------------------------

  test("first-caller config wins: maxSize=1 evicts LRU") {
    DeltaV2TableManagerCache.resetCacheForTesting()
    withSQLConf(
        DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1") {
      withTempDir { dirA =>
        withTempDir { dirB =>
          val keyA = makeKey(dirA.getCanonicalPath)
          val keyB = makeKey(dirB.getCanonicalPath)
          val stubA = new StubTableManager("a")
          val stubB = new StubTableManager("b")

          DeltaV2TableManagerCache.putForTesting(
            spark.sessionState.conf, keyA, stubA)

          val bigSession = spark.newSession()
          bigSession.conf.set(
            DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "1000")
          DeltaV2TableManagerCache.putForTesting(
            bigSession.sessionState.conf, keyB, stubB)

          DeltaV2TableManagerCache.cleanUpForTesting()
          assert(
            DeltaV2TableManagerCache.cacheSizeForTesting() == 1,
            "effective maxSize should remain 1")
          assert(stubA.retired,
            "LRU entry should be retired on capacity eviction")
          assert(!stubB.retired,
            "newest entry should remain cached")
          assert(
            DeltaV2TableManagerCache.containsKeyForTesting(keyB),
            "newest entry should still be present")
        }
      }
    }
  }

  // --- Deterministic TTL with Ticker ------------------------------

  test("deterministic TTL eviction retires expired entry") {
    val ticker = new TestTicker()
    DeltaV2TableManagerCache.setTickerForTesting(ticker)
    try {
      val ttlMinutes = 10
      withSQLConf(
          DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000",
          DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES.key
            -> ttlMinutes.toString) {
        withTempDir { dir =>
          val key = makeKey(dir.getCanonicalPath)
          val stub = new StubTableManager("ttl")
          DeltaV2TableManagerCache.putForTesting(
            spark.sessionState.conf, key, stub)
          assert(
            DeltaV2TableManagerCache.containsKeyForTesting(key))

          ticker.advance((ttlMinutes + 1) * 60L)
          DeltaV2TableManagerCache.cleanUpForTesting()

          assert(stub.retired, "expired entry should be retired")
          assert(
            !DeltaV2TableManagerCache.containsKeyForTesting(key))
        }
      }
    } finally {
      DeltaV2TableManagerCache.resetCacheForTesting()
    }
  }

}

// === Test helpers (package-private, same-file) ====================

private[tablemanager] class TestTicker extends Ticker {
  private var now: Long = System.nanoTime()
  override def read(): Long = now
  def advance(seconds: Long): Long = {
    now += TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS)
    now
  }
}

private[tablemanager] class StubTableManager(val id: String)
    extends DeltaV2TableManager {
  @volatile var retired: Boolean = false
  override def retire(): Unit = { retired = true }
}
