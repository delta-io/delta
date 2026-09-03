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

import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.delta.spark.internal.v2.tablemanager.DeltaV2TableManagerCache.CacheKey

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager

import com.google.common.base.Ticker
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaV2TableManagerCacheSuite
    extends QueryTest
    with SharedSparkSession {

  /** Creates a cache key from a data directory using the public factory. */
  private def makeKey(dataPath: String): CacheKey =
    CacheKey.from(spark, dataPath, Collections.emptyMap())

  // Process-global companion tests use unsetCache for isolation.
  override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaV2TableManagerCache.unsetCache()
  }

  override def afterEach(): Unit = {
    DeltaV2TableManagerCache.unsetCache()
    super.afterEach()
  }

  // === Per-instance tests (class methods, custom factories) =======

  test("per-instance: cache hit returns same instance") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      val first = cache.getOrCreate(key)
      val second = cache.getOrCreate(key)
      assert(first eq second)
      assert(cache.size() == 1)
    }
  }

  test("per-instance: different keys produce different entries") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
    withTempDir { dirA =>
      withTempDir { dirB =>
        val keyA = makeKey(dirA.getCanonicalPath)
        val keyB = makeKey(dirB.getCanonicalPath)
        cache.getOrCreate(keyA)
        cache.getOrCreate(keyB)
        assert(cache.size() == 2)
      }
    }
  }

  test("per-instance: cache hit preserves initialCatalogTableOpt from first load") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
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

      val first = cache.getOrCreate(key, Some(catalogA))
      val second = cache.getOrCreate(key, Some(catalogB))
      assert(first eq second)
      val impl = first.asInstanceOf[DeltaV2TableManagerImpl]
      assert(
        impl.initialCatalogTableOpt === Some(catalogA),
        "initial catalog should be from first load")
    }
  }

  test("per-instance: removal listener retires manager on invalidation") {
    val stub = new StubTableManager("a")
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = 60,
      managerFactory = (_, _) => stub)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      cache.getOrCreate(key)
      assert(!stub.retired)
      cache.invalidate(key)
      assert(stub.retired, "retire() should be called on eviction")
    }
  }

  test("per-instance: maxSize=1 LRU eviction retires old manager") {
    val stubA = new StubTableManager("a")
    val stubB = new StubTableManager("b")
    val stubs = Iterator(stubA, stubB)
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1,
      ttlMinutes = 60,
      managerFactory = (_, _) => stubs.next())
    withTempDir { dirA =>
      withTempDir { dirB =>
        val keyA = makeKey(dirA.getCanonicalPath)
        val keyB = makeKey(dirB.getCanonicalPath)
        cache.getOrCreate(keyA)
        cache.getOrCreate(keyB)
        cache.cleanUp()
        assert(cache.size() == 1, "effective maxSize should remain 1")
        assert(stubA.retired, "LRU entry should be retired on capacity eviction")
        assert(!stubB.retired, "newest entry should remain cached")
        assert(cache.contains(keyB), "newest entry should still be present")
      }
    }
  }

  test("per-instance: deterministic TTL eviction retires expired entry") {
    val ticker = new TestTicker()
    val ttlMinutes = 10
    val stub = new StubTableManager("ttl")
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = ttlMinutes,
      ticker = ticker,
      managerFactory = (_, _) => stub)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      cache.getOrCreate(key)
      assert(cache.contains(key))

      ticker.advance((ttlMinutes + 1) * 60L)
      cache.cleanUp()

      assert(stub.retired, "expired entry should be retired")
      assert(!cache.contains(key))
    }
  }

  // --- Guava exception unwrapping via injected factory -------------

  test("per-instance: unwraps checked exception from ExecutionException") {
    val cause = new java.io.IOException("checked-cause")
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = 60,
      managerFactory = (_, _) => throw cause)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      val caught = intercept[java.io.IOException] {
        cache.getOrCreate(key)
      }
      assert(caught eq cause, "original cause identity must be preserved")
    }
  }

  test("per-instance: unwraps runtime exception from UncheckedExecutionException") {
    val cause = new IllegalStateException("runtime-cause")
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = 60,
      managerFactory = (_, _) => throw cause)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      val caught = intercept[IllegalStateException] {
        cache.getOrCreate(key)
      }
      assert(caught eq cause, "original cause identity must be preserved")
    }
  }

  test("per-instance: unwraps Error from ExecutionError") {
    val cause = new StackOverflowError("error-cause")
    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = 60,
      managerFactory = (_, _) => throw cause)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      val caught = intercept[StackOverflowError] {
        cache.getOrCreate(key)
      }
      assert(caught eq cause, "original cause identity must be preserved")
    }
  }

  // --- Per-instance size / getIfPresent / invalidation -------------

  test("per-instance: size and getIfPresent reflect cache state") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      assert(cache.size() == 0)
      assert(cache.getIfPresent(key).isEmpty)
      cache.getOrCreate(key)
      assert(cache.size() == 1)
      assert(cache.getIfPresent(key).isDefined)
    }
  }

  test("per-instance: invalidate removes a single entry") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      cache.getOrCreate(key)
      assert(cache.contains(key))
      cache.invalidate(key)
      assert(!cache.contains(key))
    }
  }

  test("per-instance: invalidateByLogPath removes all entries with matching path") {
    // Two distinct keys share the same qualified log path but differ in sessionInvariantFsOptions,
    // proving removeIf matches every entry whose path equals the target -- not just one key.
    val stubA = new StubTableManager("pathA")
    val stubB = new StubTableManager("pathB")
    val stubs = Iterator(stubA, stubB)
    withTempDir { dir =>
      val sharedLogPath = makeKey(dir.getCanonicalPath).path
      val keyA = CacheKey(
        sharedLogPath,
        Map("fs.s3a.access.key" -> "AAA"))
      val keyB = CacheKey(
        sharedLogPath,
        Map("fs.s3a.access.key" -> "BBB"))
      val cache = new DeltaV2TableManagerCache(
        maxSize = 1000,
        ttlMinutes = 60,
        managerFactory = (_, _) => stubs.next())
      cache.getOrCreate(keyA)
      cache.getOrCreate(keyB)
      assert(cache.size() == 2)

      cache.invalidateByLogPath(sharedLogPath)
      assert(cache.size() == 0, "both entries with the same path must be removed")
      assert(stubA.retired, "first manager must be retired")
      assert(stubB.retired, "second manager must be retired")
    }
  }

  test("per-instance: invalidateAll empties the entire cache") {
    val cache = new DeltaV2TableManagerCache(maxSize = 1000, ttlMinutes = 60)
    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      cache.getOrCreate(key)
      assert(cache.size() > 0)
      cache.invalidateAll()
      assert(cache.size() == 0)
    }
  }

  // === Process-global companion object tests ======================

  test("enabled by default") {
    assert(DeltaV2TableManagerCache.isEnabled(spark.sessionState.conf))
  }

  test("default cache size is positive") {
    val size = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
    assert(size > 0, s"Expected positive cache size, got $size")
  }

  test("disabled when size=0") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "0") {
      assert(!DeltaV2TableManagerCache.isEnabled(spark.sessionState.conf))
    }
  }

  test("getOrCreate bypasses cache when size=0") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "0") {
      withTempDir { dir =>
        val key = makeKey(dir.getCanonicalPath)
        val first = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf,
          key)
        val second = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf,
          key)
        assert(first ne second)
      }
    }
  }

  test("getOrCreate caches when enabled") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val key = makeKey(dir.getCanonicalPath)
        val first = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf,
          key)
        val second = DeltaV2TableManagerCache.getOrCreate(
          spark.sessionState.conf,
          key)
        assert(first eq second)
      }
    }
  }

  test("process-global: different sessions share same instance") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val sessionA = spark.newSession()
        val sessionB = spark.newSession()
        sessionB.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "500")

        val keyA = CacheKey.from(
          sessionA,
          dir.getCanonicalPath,
          Collections.emptyMap())
        val keyB = CacheKey.from(
          sessionB,
          dir.getCanonicalPath,
          Collections.emptyMap())

        val fromA = DeltaV2TableManagerCache.getOrCreate(
          sessionA.sessionState.conf,
          keyA)
        val fromB = DeltaV2TableManagerCache.getOrCreate(
          sessionB.sessionState.conf,
          keyB)

        assert(keyA == keyB, "Same path must produce the same cache key across sessions")
        assert(fromA eq fromB, "Same key from different sessions must share one cached instance")
      }
    }
  }

  test("per-instance: Guava per-key single-flight loader invokes factory once") {
    // Holds the first loader mid-flight while a second caller requests the same key, proving
    // Guava's per-key single-flight guarantees exactly one factory invocation and identical result
    // for both callers.
    val invocations = new AtomicInteger(0)
    val loaderEntered = new CountDownLatch(1)
    val loaderRelease = new CountDownLatch(1)
    val stub = new StubTableManager("single-flight")

    val cache = new DeltaV2TableManagerCache(
      maxSize = 1000,
      ttlMinutes = 60,
      managerFactory = (_, _) => {
        invocations.incrementAndGet()
        loaderEntered.countDown()
        loaderRelease.await()
        stub
      })

    withTempDir { dir =>
      val key = makeKey(dir.getCanonicalPath)
      // scalastyle:off sparkThreadPools
      val executor = Executors.newFixedThreadPool(2)
      // scalastyle:on sparkThreadPools
      try {
        val futureA = executor.submit(() => cache.getOrCreate(key))
        assert(loaderEntered.await(10, TimeUnit.SECONDS), "loader must be entered")
        val futureB = executor.submit(() => cache.getOrCreate(key))
        loaderRelease.countDown()

        val resultA = futureA.get(10, TimeUnit.SECONDS)
        val resultB = futureB.get(10, TimeUnit.SECONDS)
        assert(invocations.get() == 1, "factory must be invoked exactly once")
        assert(resultA eq resultB, "both callers must receive the same instance")
      } finally {
        executor.shutdownNow()
      }
    }
  }

  test("process-global: first caller's size config remains effective") {
    // Session A initializes the singleton with maxSize=1.
    val sessionA = spark.newSession()
    sessionA.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "1")

    // Session B would prefer a larger cache, but the singleton is already built.
    val sessionB = spark.newSession()
    sessionB.conf.set(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key, "1000")

    withTempDir { dirA =>
      withTempDir { dirB =>
        val keyA = CacheKey.from(
          sessionA,
          dirA.getCanonicalPath,
          Collections.emptyMap())
        val keyB = CacheKey.from(
          sessionB,
          dirB.getCanonicalPath,
          Collections.emptyMap())

        // Session A loads key A -- initializes singleton (size 1).
        val originalA = DeltaV2TableManagerCache.getOrCreate(
          sessionA.sessionState.conf,
          keyA)

        // Session B loads key B -- evicts key A under size-1.
        DeltaV2TableManagerCache.getOrCreate(
          sessionB.sessionState.conf,
          keyB)

        // Re-lookup key A: must create a new instance because size-1 evicted the original, proving
        // first caller's config governs, not the second caller's (size 1000).
        val reloadedA = DeltaV2TableManagerCache.getOrCreate(
          sessionA.sessionState.conf,
          keyA)
        assert(
          reloadedA ne originalA,
          "Key A should have been evicted under the first caller's maxSize=1 and " +
            "reloaded as a new instance")
      }
    }
  }

  // === Facade and manager-field assertions ========================

  test("forTable produces a manager with expected qualified path and options") {
    withTempDir { dir =>
      val manager = DeltaV2TableManagerCache.forTable(
        spark,
        dir.getCanonicalPath,
        Collections.emptyMap())
      val impl = manager.asInstanceOf[DeltaV2TableManagerImpl]
      assert(impl.qualifiedTableDataPath.isAbsolute)
      assert(impl.qualifiedTableDataPath.toUri.getPath.contains(dir.getName))
      assert(impl.sessionInvariantFsOptions.isEmpty)
      assert(impl.initialCatalogTableOpt.isEmpty)
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

private[tablemanager] class StubTableManager(val id: String) extends DeltaV2TableManager {
  @volatile var retired: Boolean = false
  override def snapshotManager(): DeltaV2SnapshotManager =
    throw new UnsupportedOperationException("stub")
  override def retire(): Unit = { retired = true }
}
